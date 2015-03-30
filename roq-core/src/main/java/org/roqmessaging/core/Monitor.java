/**
 * Copyright 2012 EURANOVA
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at

   http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 * 
 */
package org.roqmessaging.core;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import org.apache.log4j.Logger;
import org.roqmessaging.core.interfaces.IStoppable;
import org.roqmessaging.core.stat.StatisticMonitor;
import org.roqmessaging.core.timer.MonitorStatTimer;
import org.roqmessaging.state.ExchangeState;
import org.roqmessaging.state.MonitorState;
import org.roqmessaging.utils.LocalState;
import org.roqmessaging.utils.Time;
import org.zeromq.ZMQ;

/**
 * Class Monitor
 * <p> Description: 
 * <br>1. Maintain the state of the exchanges in place and their states. 
 * <br>2. manage the auto scaling
 * <br>3. the publisher re-allocation
 * 
 *  @author Nam-Luc Tran, Sabri Skhiri
 */
public class Monitor implements Runnable, IStoppable {
	// State of the Monitor
	private MonitorState monitorState;
	private Logger logger = Logger.getLogger(Monitor.class);
	
	//define whether the thread is active
	private volatile boolean active = true;
	//define whether the thread is shutting down
	public static volatile boolean shuttingDown = false;
	private ZMQ.Context context;
	private ZMQ.Socket producersPub, brokerSub, initRep, listenersPub;
	//Monitor heart beat socket, client can check that monitor is alive
	private ZMQ.Socket heartBeat = null;
	private int basePort = 0, statPort =0;
	//The handle to the statistic monitor thread
	private StatisticMonitor statMonitor = null;
	//Shut down monitor
	private ShutDownMonitor shutDownMonitor;
	//The monitor statistic.
	private MonitorStatTimer monitorStat;
	private int period =60000;
	//The queue name
	private String qName = "name";
	
	//Local State for heartbeats
	private LocalState localState;
	// Minimum time between two heartbeats (in millis)
	private long hbPeriod;
	
	private Lock lock = new ReentrantLock();

	
	/**
	 * @param basePort default value must be 5571
	 * @param statPort default port for stat socket 5800
	 * @param qname the logical queue from which the monitor belongs to
	 * @param period the stat period for publication
	 */
	public Monitor(int basePort, int statPort, String qname, String period, String localStatePath, long hbPeriod, String zkAddress) {
		try {
			this.basePort = basePort;
			this.statPort = statPort;
			this.qName = qname;
			this.period = Integer.parseInt(period);
			monitorState = new MonitorState(basePort, 75000000L, zkAddress);

			localState = new LocalState(localStatePath + "/" + basePort);
			this.hbPeriod = hbPeriod;
			context = ZMQ.context(1);

			producersPub = context.socket(ZMQ.PUB);
			producersPub.bind("tcp://*:" + (basePort + 2));
			logger.debug("Binding procuder to " + "tcp://*:" + (basePort + 2));

			brokerSub = context.socket(ZMQ.SUB);
			brokerSub.bind("tcp://*:" + (basePort));
			logger.debug("Binding broker Sub  to " + "tcp://*:" + (basePort));
			brokerSub.subscribe("".getBytes());

			initRep = context.socket(ZMQ.REP);
			initRep.bind("tcp://*:" + (basePort + 1));
			logger.debug("Init request socket to " + "tcp://*:" + (basePort + 1));

			listenersPub = context.socket(ZMQ.PUB);
			listenersPub.bind("tcp://*:" + (basePort + 3));

			heartBeat = context.socket(ZMQ.REP);
			heartBeat.bind("tcp://*:" + (basePort + 4));
			logger.debug("Heart beat request socket to " + "tcp://*:" + (basePort + 4));

		} catch (Exception e) {
			logger.error("Error while creating Monitor, ABORDED", e);
			return;
		}

		// Stat monitor init thread
		this.statMonitor = new StatisticMonitor(statPort, qname);
		new Thread(this.statMonitor).start();

		// shutodown monitor
		// initiatlisation of the shutdown thread
		this.shutDownMonitor = new ShutDownMonitor(basePort + 5, this);
		new Thread(shutDownMonitor).start();
		logger.debug("Started shutdown monitor on " + (basePort + 5));

	}

	public void run() {
		int infoCode =0; 
		
		//1. Start the report timer & the queue stat timer
		Timer reportTimer = new Timer();
		reportTimer.schedule(new ReportExchanges(), 0, period+10);
		this.monitorStat = new MonitorStatTimer(this);
		reportTimer.schedule(this.monitorStat, 0, period);

		ZMQ.Poller items = new ZMQ.Poller(3);
		items.register(brokerSub);//0
		items.register(initRep);//1

		logger.info("Monitor started");
		long lastPublish = System.currentTimeMillis();
		long lastHb = Time.currentTimeMillis() - hbPeriod;
		long current;

		//2. Start the main run of the monitor
		while (this.active) {
			//not really clean, workaround to the fact thats sockets cannot be shared between threads
			if (System.currentTimeMillis() - lastPublish > 10000) { 
				listenersPub.send(("2," + monitorState.bcastExchg()).getBytes(), 0);
				lastPublish = System.currentTimeMillis();
				logger.debug("Alive hosts: " + monitorState.bcastExchg() );
			}
//			while (!hostsToRemove.isEmpty() && !this.shuttingDown) {
//				try {
//					producersPub.send((new Integer(RoQConstant.EXCHANGE_LOST).toString()+"," + monitorState.getHost(hostsToRemove.get(0)).getAddress()).getBytes(), 0);
//					knownHosts.remove((int) hostsToRemove.getHost(0));
//					hostsToRemove.remove(0);
//					logger.warn("Panic procedure initiated");
//				}finally {
//				}
//			}
			// Write Heartbeat
			if ((Time.currentTimeMillis() - lastHb) >= hbPeriod) {
				try {
					current = Time.currentTimeSecs();
					logger.info("Monitor Writing hb " + basePort + " " + current);
					localState.put("HB", current);
					lastHb = Time.currentTimeMillis();
				} catch (IOException e) {
					logger.info("Failed to write in local db: " + e);
				}
			}
			
			//3. According to the channel bit used, we can define what kind of info is sent
			items.poll(100);
			if (items.pollin(0)) { // Info from Exchange
								String info[] = new String(brokerSub.recv(0)).split(",");
					// Check if exchanges are present: this happens when the
					// queue is shutting down a client is asking for a
					// connection
					if (info.length > 1) {
						infoCode = Integer.parseInt(info[0]);
						switch (infoCode) {
						case RoQConstant.DEBUG:
							// Broker debug code
							logger.info(info[1]);
							break;
						case RoQConstant.EVENT_MOST_PRODUCTIVE:
							// Broker most productive producer code
							monitorState.updateExchgMetadata(info[1], info[4], info[6]);
							if (!info[1].equals("x")) {
								String relocation = monitorState.relocateProd(info[1], info[3]); // ip,bytessent
								if (!relocation.equals("")) {
									logger.debug("relocating " + info[2] + " on " + relocation);
									producersPub.send((new Integer(RoQConstant.REQUEST_RELOCATION).toString() + ","
											+ info[2] + "," + relocation).getBytes(), 0);
								}
							}
							break;
						case RoQConstant.EVENT_HEART_BEAT:
							// Broker heartbeat code Registration
							logger.trace("Getting Heart Beat information");
							if (info.length == 4) {
								if (monitorState.logHost(info[1], info[2], info[3]) == 1) {
									listenersPub.send((new Integer(RoQConstant.REQUEST_UPDATE_EXCHANGE_LIST).toString()
											+ "," + info[1]+":"+ info[2]).getBytes(), 0);
								}
							} else
								logger.error("The message recieved from the exchange heart beat"
										+ " does not contains 4 parts");
							break;

						case RoQConstant.EVENT_EXCHANGE_SHUT_DONW:
							// Broker shutdown notification
							logger.info("Broker " + info[1] + " has left the building");
							int hostIndex = monitorState.hostLookup(info[1]);
							if (hostIndex != -1 && !Monitor.shuttingDown) {
								monitorState.removeHost(hostIndex);							
							}

							producersPub.send(
									(new Integer(RoQConstant.EXCHANGE_LOST).toString() + "," + info[1]).getBytes(), 0);
							break;
						}
					} else {
						logger.error(" Error when recieving information from Exchange there is no  info code !",
								new IllegalStateException("The exchange sent a request" + " without info code !"));
					}
			
			}

			if (items.pollin(1)) { // Init socket
				logger.debug("Received init request from either producer or listner");
				String info[] = new String(initRep.recv(0)).split(",");
				infoCode = Integer.parseInt(info[0]);
				if (!Monitor.shuttingDown) {
					switch (infoCode) {
					case RoQConstant.CHANNEL_INIT_SUBSCRIBER:
						logger.debug("Received init request from listener");
						initRep.send(monitorState.bcastExchg().getBytes(), 0);
						break;
					case RoQConstant.CHANNEL_INIT_PRODUCER:
						String freeHost = monitorState.getFreeHostForPublisher();
						logger.debug("Received init request from producer. Assigned on " +freeHost);
						initRep.send(freeHost.getBytes(), 0);
						break;

					case 3:
						logger.debug("Received panic init from producer");
						initRep.send(monitorState.getFreeHostForPublisher().getBytes(), 0);
						// TODO: round
						// robin return
						// knownHosts.
						// should be
						// aware of the
						// profile of
						// the producer
						break;
					}
				}else{
					logger.info("Monitor is shuting down, no Exchange can be allocated");
					initRep.send("".getBytes(), 0);
				}
				
			}
		}
		// Exit running
		this.monitorStat.shutTdown();
		monitorState.getAllHosts().clear();
		reportTimer.cancel();
		closeSocket();
		logger.info("Monitor  "+ this.basePort+" Stopped");
	}

	/**
	 * Closes all open sockets.
	 */
	private void closeSocket() {
		logger.info("Closing all sockets from " + getName());
		producersPub.close();
		brokerSub.close();
		initRep.close();
		listenersPub.close();
		heartBeat.close();
	}


	/**
	 * Class ReportExchanges
	 * <p> Description: if a host do not send an heartbeat 
	 * it is considered as dead.
	 * 
	 * @author sskhiri
	 */
	class ReportExchanges extends TimerTask {
		public void run() {
			try {
				lock.lock();
				if (!monitorState.getAllHosts().isEmpty() && !shuttingDown) {
					for (int i = 0; i < monitorState.getAllHosts().size(); i++) {
						if (!monitorState.getHost(i).isAlive()) {
							monitorState.getHost(i).addLost();
							if (monitorState.getHost(i).getLost() > 0) {
								logger.info(monitorState.getHost(i).getAddress() + " is lost");
								hostsToRemove.add(i);
							}
						} else {
							monitorState.getHost(i).setAlive(false);
						}
					}
				}
			} finally {
				lock.unlock();
			}
		}
	}


	/**
	 * @see org.roqmessaging.core.interfaces.IStoppable#shutDown()
	 */
	public void shutDown() {
		Monitor.shuttingDown = true;
		try {
			logger.info("Starting the shutdown procedure...");
			this.lock.lock();
			// Stopping all exchange
			logger.info("Stopping all exchanges...(" + monitorState.getAllHosts().size() + ")");
			for (ExchangeState exchangeState_i : monitorState.getAllHosts()) {
				String address = exchangeState_i.getAddress();
				int backport = exchangeState_i.getBackPort();
				logger.info("Stopping exchange on " + address + ":" + (backport + 1));
				ZMQ.Socket shutDownExChange = ZMQ.context(1).socket(ZMQ.REQ);
				shutDownExChange.setSendTimeOut(-1);
				shutDownExChange.connect("tcp://" + address + ":" + (backport + 1));
				if (!shutDownExChange.send(Integer.toString(RoQConstant.SHUTDOWN_REQUEST).getBytes(), 0)) {
					logger.error("Error while sending shutdown request to exchange", new IllegalStateException(
							"The message has not been sent"));
				} else {
					logger.info("Sent success fully Stopping exchange on " + address + ":" + (backport + 1));
				}
				shutDownExChange.close();
			}
			monitorState.clear();
			this.statMonitor.shutDown();
			this.active = false;
			//When we shut down the exchanges, they send a exchange lost notification.
			//Some time the notification does not arrive because the monitor has already left
			//This could lead to not remove the exchange properly.
			Thread.sleep(2500);
		} catch (Exception  e) {
			logger.error("Error when running the monitor ", e);
		}finally{
			this.lock.unlock();
		}
	}

	/**
	 * @see org.roqmessaging.core.interfaces.IStoppable#getName()
	 */
	public String getName() {
		return this.qName +":"+ this.basePort;
	}
	

	/**
	 * @return the statPort
	 */
	public int getStatPort() {
		return statPort;
	}

	/**
	 * @param statPort the statPort to set
	 */
	public void setStatPort(int statPort) {
		this.statPort = statPort;
	}

}
