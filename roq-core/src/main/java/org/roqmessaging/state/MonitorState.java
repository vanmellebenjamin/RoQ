package org.roqmessaging.state;

import java.util.ArrayList;
import java.util.List;

import org.apache.log4j.Logger;
import org.roqmessaging.core.Monitor;
import org.roqmessaging.zookeeper.CoreMetadata;
import org.roqmessaging.zookeeper.RoQZooKeeper;


public class MonitorState {
	// logger
	private Logger logger = Logger.getLogger(MonitorState.class);
	//List of exchanges and their state
	private ArrayList<ExchangeState> knownHosts;
	private long maxThroughput;
	// monitor id
	private int monitorID;
	// Zookeeper client
	private RoQZooKeeper zkClient;
	
	public MonitorState(int basePort, long maxThroughput, String zkAddresses) {
		zkClient = new RoQZooKeeper(zkAddresses);
		zkClient.start();
		this.maxThroughput = maxThroughput;
		monitorID = basePort;
		
		if(zkClient.addMonitorIfNotExists(monitorID)) {
			recoverMonitorState(zkClient.getExchangesList(monitorID));
		}
	}
	
	private void recoverMonitorState(List<CoreMetadata.Exchange> exchanges) {
		for (CoreMetadata.Exchange exchange : exchanges) {
			knownHosts.add(new ExchangeState(zkClient.getExchangeState(exchange)));
		}
	}
	
	public ArrayList<ExchangeState> getAllHosts() {
		return knownHosts;
	}
	
	public ExchangeState getHost(int index) {
		return knownHosts.get(index);
	}
	
	public void removeHost(int hostAddress) {
		knownHosts.remove(hostAddress);
	}
	
	/**
	 * @param address the address:front port:backport
	 * @return the index of the state in the knowhost or -1;
	 */
	public int hostLookup(String address) {
		String[] addressInfo = address.split(":");
		int index = 0;
		if(addressInfo.length==3){
			for (ExchangeState state_i : knownHosts) {
				if (state_i.match(addressInfo[0], addressInfo[1], addressInfo[2])) {
					return index;
				}else{
					index++;
				}
			}
		}
		return -1;
	}
	
	public void addHost(String exchangeID, ExchangeState state) {
		//Save the new state in ZK
		zkClient.setExchangeState(monitorID, exchangeID, exchange);
		knownHosts.add(state);
	}
	
	public void clear() {
		zkClient.clearMonitor(monitorID);
	}
	
	/**
	 * @return a free host address + the front port. That must be only used for publisher
	 */
	public String getFreeHostForPublisher() {
		String freeHostAddress = "";
		if (!knownHosts.isEmpty()) {
			long tempt = Long.MAX_VALUE;
			int tempP =  Integer.MAX_VALUE;;
			int indexLoad = 0;
			int indexPub = 0;
			//Less than 10% of the max throughput
			boolean coldStart = true;
			for (int i = 0; i < knownHosts.size(); i++) {
				if (knownHosts.get(i).getThroughput()>(this.maxThroughput/10)) coldStart =false;
				//Check the less overloaded
				if (knownHosts.get(i).getThroughput() < tempt) {
					logger.debug("Host "+i+ " has a throughput of "+ knownHosts.get(i).getThroughput() + " byte/min");
					tempt = knownHosts.get(i).getThroughput();
					indexLoad = i;
				}
				//Check the less assigned
				if(knownHosts.get(i).getNbProd()<tempP){
					tempP=knownHosts.get(i).getNbProd();
					indexPub = i;
				}
			}
			if(coldStart){
				//We chose the less assigned exchange
				freeHostAddress = knownHosts.get(indexPub).getAddress()+ ":"+ knownHosts.get(indexPub).getFrontPort();
				logger.info("Assigned Producer to exchange " +indexPub  + " (cold start) @ "+ freeHostAddress);
				//Then we need tp update the cache, since the number of prod will arrive with stat later
				tempP++;
				knownHosts.get(indexPub).setNbProd(tempP);
			}else{
				//we choose the less overloaded exchange
				freeHostAddress = knownHosts.get(indexLoad).getAddress()+ ":"+ knownHosts.get(indexLoad).getFrontPort();
				logger.info("Assigned Producer to exchange " +indexLoad+ " "+ freeHostAddress );
			}
			
			
		}
		return freeHostAddress;
	}
	
	/**
	 * Register the host address of the exchange
	 * @param address the host address
	 * @param frontPort the front end port for publisher
	 * @param backPort the back end port for subscriber
	 * @return 1 if the host has been added otherwise it is hearbeat
	 */
	public int logHost(String address, String frontPort, String backPort) {
		if(!Monitor.shuttingDown){
			logger.trace("Log host procedure for "+ address +": "+ frontPort +"->"+ backPort);
			if (!knownHosts.isEmpty()) {
				for (ExchangeState exchange_i : knownHosts) {
					if(exchange_i.match(address, frontPort, backPort)){
						exchange_i.setAlive(true);
						 logger.trace("Host "+address+": "+ frontPort+"->"+ backPort+"  reported alive");
						return 0;
					}
				}
			}
			logger.info("Added new host: " + address+": "+ frontPort+"->"+ backPort);
			addHost(new ExchangeState(address, frontPort, backPort));
			return 1;
		}else{
			logger.warn("We are not going to log the host "+ address + " the monitor is shutting down");
		}
		return 0;
	}
	
	/**
	 * Evaluates the load on the exchange and select the less overloaded exchange if required.
	 * The relocation method does:
	 * 1. Get the host index in the exchange host list<br>
	 * 2. Check the max throughput value<br>
	 * 3. Check the limit case (1 publisher)<br>
	 * 4. Get a candidate: exchange that is still under the max throughput limit and that has the lower throughput<br>
	 * 5. Update the config state of the exchange candidate <br>
	 * 6. Send the relocate message to the publisher<br>
	 * @param exchg_addr the address of the current exchange
	 * @param bytesSent the bytesent per cycle sent through the exchange
	 * @return an empty string is there is nothing to do or the address of the new exchange if we need a re-location
	 */
	public String relocateProd(String exchg_addr, String bytesSent) {
		logger.debug("Relocate exchange procedure");
		int exch_index = hostLookup(exchg_addr);
		if (knownHosts.size() > 0 && exch_index != -1 && !Monitor.shuttingDown) {
			logger.debug("Do we need to relocate ?");
			//TODO externalize 1.0, 0.90, and 0.20 tolerance values
			if (knownHosts.get(exch_index).getThroughput() > (java.lang.Math.round(maxThroughput * 1.10))) { 
				logger.debug("Is "+knownHosts.get(exch_index).getThroughput() + " > "+  (java.lang.Math.round(maxThroughput * 1.10)));
				//Limit case: exchange saturated with only one producer TODO raised alarm
				if (knownHosts.get(exch_index).getNbProd() == 1) { 
					logger.error("Limit Case we cannot relocate  the unique producer !");
					return "";
				} else {
					int candidate_index = getFreeHost_index(exch_index);
					if (candidate_index != -1) {
						String candidate = knownHosts.get(candidate_index).getAddress();
						if (knownHosts.get(candidate_index).getThroughput() + Long.parseLong(bytesSent) < (java.lang.Math
								.round(maxThroughput * 0.90))) {
							
							knownHosts.get(candidate_index).addThroughput(Long.parseLong(bytesSent));
							knownHosts.get(candidate_index).addNbProd();
							knownHosts.get(exch_index).lessNbProd();
							knownHosts.get(exch_index).lessThroughput(Long.parseLong(bytesSent));
							logger.info("Relocate a publisher on "+ candidate+":"+knownHosts.get(candidate_index).getFrontPort());
							return candidate+":"+knownHosts.get(candidate_index).getFrontPort();
						}else if (knownHosts.get(candidate_index).getThroughput() + Long.parseLong(bytesSent) < knownHosts
								.get(hostLookup(exchg_addr)).getThroughput()
								&& 
								(knownHosts.get(hostLookup(exchg_addr)).getThroughput() - knownHosts.get(candidate_index).getThroughput())
								> (java.lang.Math.round(knownHosts.get(	hostLookup(exchg_addr)).getThroughput() * 0.20))) {
							
							knownHosts.get(candidate_index).addThroughput(Long.parseLong(bytesSent));
							knownHosts.get(candidate_index).addNbProd();
							knownHosts.get(exch_index).lessNbProd();
							knownHosts.get(exch_index).lessThroughput(Long.parseLong(bytesSent));
							logger.info("Relocating for load optimization on "+ candidate+":"+knownHosts.get(candidate_index).getFrontPort());
							return candidate+":"+knownHosts.get(candidate_index).getFrontPort();
						}
					}
				}
			}
		}
		return "";
	}
	
	/**
	 * @param address the exchange host address under the format IP:front port: back port
	 * @param throughput the current throughput
	 * @param nbprod the number of producers connected to the exchange
	 */
	public void updateExchgMetadata(String address, String throughput, String nbprod) {
		logger.debug("update Exchg Metadata");
		String[] addressInfo = address.split(":");
		if(addressInfo.length!=3){
			logger.error(new IllegalStateException("The message EVENT_MOST_PRODUCTIVE is nof formated correctly, the address is not as IP:front port:back port"));
			return;
		}
		if(!Monitor.shuttingDown){
			for (ExchangeState state_i : knownHosts) {
				if(state_i.match(addressInfo[0], addressInfo[1], addressInfo[2])){
					state_i.setThroughput(Long.parseLong(throughput));
					state_i.setNbProd(Integer.parseInt(nbprod));
					logger.info("Host " + state_i.getAddress() + " : " + state_i.getFrontPort()+ "->" + state_i.getBackPort()+" :" + state_i.getThroughput()
							+ " bytes/min, " + state_i.getNbProd() + " users");
				}
			}
		}else{
			logger.warn("We are not going to log the host "+ address + " the monitor is shutting down");
		}
	}

	private int getFreeHost_index(int cur_index) {
		int index = -1;
		if (!knownHosts.isEmpty()) {
			long tempt = Long.MAX_VALUE;
			for (int i = 0; i < knownHosts.size(); i++) {
				if (i == cur_index)
					continue;

				if (knownHosts.get(i).getThroughput() < tempt) {
					tempt = knownHosts.get(i).getThroughput();
					index = i;
				}
			}
		}
		return index;
	}

	/**
	 * @return the concatenate list of exchanged registered at the monitor
	 */
	public String bcastExchg() {
		String exchList = "";
		if (!knownHosts.isEmpty()) {
			for (int i = 0; i < knownHosts.size(); i++) {
				exchList += knownHosts.get(i).getAddress()+":"+knownHosts.get(i).getBackPort();
				if (i != knownHosts.size() - 1) {
					exchList += ",";
				}
			}
		}
		return exchList;
	}
	
	/**
	 * @return list of registered exchanges and their current states.
	 */
	public ArrayList<ExchangeState> getExhcangeMetaData(){
		return this.knownHosts;
	}
	
}
