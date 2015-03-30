package org.roqmessaging.zookeeper;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import org.apache.curator.RetryPolicy;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.RetryOneTime;
import org.apache.log4j.Logger;
import org.roqmessaging.state.ExchangeState;
import org.roqmessaging.zookeeper.CoreMetadata.Exchange;

public class RoQZooKeeper {
	protected CuratorFramework client;
	protected RoQZKSimpleConfig cfg;
	private final Logger log = Logger.getLogger(getClass());
	
	public RoQZooKeeper(String zkAddresses) {
		cfg = new RoQZKSimpleConfig();
		cfg.servers = zkAddresses;
		
		// Start a Curator client, through which we can access ZooKeeper
		// Note: retry policy should be made configurable
		RetryPolicy retryPolicy = new RetryOneTime(1000);
				
		client = CuratorFrameworkFactory.builder()
				.connectString(zkAddresses)
				.namespace(cfg.namespace)
				.retryPolicy(retryPolicy)
				.build();
	}

	public RoQZooKeeper(RoQZKSimpleConfig config) {		
		// Start a Curator client, through which we can access ZooKeeper
		// Note: retry policy should be made configurable
		RetryPolicy retryPolicy = new RetryOneTime(1000);
		cfg = config;
		client = CuratorFrameworkFactory.builder()
					.connectString(config.servers)
					.retryPolicy(retryPolicy)
					.namespace(config.namespace)
					.build();	
	}
	
	// Start the client and leader election
	public void start() {
		log.info("");
		client.start();
	}
	
	public void close() {
		log.info("");
		client.close();
	}

	// Remove the znode if it exists.
	public String getGCMLeaderAddress() {
		log.info("");
		String path = RoQZKHelpers.makePath(cfg.znode_leaderAddress);
		String gcm = RoQZKHelpers.getDataString(client, path);
		return gcm;
	}

	/**
	 * Check if the monitor exists in ZK,
	 * The monitor is added in ZK if it not exists
	 * @param monitorID
	 * @return false if it exists, else true
	 */
	public boolean addMonitorIfNotExists(int monitorID) {
		log.info("");
		String path = RoQZKHelpers.makePath(cfg.znode_monitorState, 
				Integer.toString(monitorID), cfg.znode_monitor_exchangesState);
		if(RoQZKHelpers.zNodeExists(client, path)) {
			return true;
		} else {
			RoQZKHelpers.createZNodeAndParents(client, path);
			return false;
		}
		
	}
	
	/**
	 * Get the exchanges for this monitor
	 * The monitor is added in ZK if it not exists
	 * @param monitorID
	 * @return false if it exists, else true
	 */
	public List<CoreMetadata.Exchange> getExchangesList(int monitorID) {
		log.info("");
		
		List<CoreMetadata.Exchange> exchanges = new ArrayList<CoreMetadata.Exchange>();
		String path = RoQZKHelpers.makePath(cfg.znode_monitorState, Integer.toString(monitorID), cfg.znode_monitor_exchangesState);
		List<String> znodes = RoQZKHelpers.getChildren(client, path);
		
		// If something goes wrong, return an empty list.
		if (znodes == null) {
			log.debug("Helpers.getChildren() failed. Aborting getQueueList().");
			return exchanges;
		}
		
		for (String node : znodes) {
			exchanges.add(new CoreMetadata.Exchange(Integer.toString(monitorID), node));
		}
		return exchanges;
	}
	
	/**
	 * Get the exchanges for this monitor
	 * The monitor is added in ZK if it not exists
	 * @param monitorID
	 * @return false if it exists, else true
	 */
	public HashMap<String, String> getExchangeState(CoreMetadata.Exchange exchange) {
		log.info("");
		HashMap<String, String> exchangeState = new HashMap<String, String>();
		String basepath = getZKPath(exchange);
		exchangeState.put("address", getExchangeState(basepath, "address"));
		exchangeState.put("throughput", getExchangeState(basepath, "throughput"));
		exchangeState.put("nbProd", getExchangeState(basepath, "nbProd"));
		exchangeState.put("lost", getExchangeState(basepath, "lost"));
		exchangeState.put("frontPort", getExchangeState(basepath, "frontPort"));
		exchangeState.put("backPort", getExchangeState(basepath, "backPort"));
		return exchangeState;
	}
	
	/**
	 * Sete the exchange state for this monitor
	 * The monitor is added in ZK if it not exists
	 * @param monitorID
	 * @return false if it exists, else true
	 */
	public HashMap<String, String> setExchangeState(String monitorID, String exchangeID, ExchangeState exchange) {
		log.info("");
		HashMap<String, String> exchangeState = new HashMap<String, String>();
		// TODO: et data in ZK
		return exchangeState;
	}
	
	/**
	 * Remove the state of the monitor from ZK
	 */
	public void clearMonitor(int monitorID) {
		log.info("");
		String path = RoQZKHelpers.makePath(cfg.znode_monitorState, 
				Integer.toString(monitorID));
		RoQZKHelpers.deleteZNodeAndChildren(client, path);
	}
	
	private String getExchangeState(String exchangePath, String leafNode) {
		log.info("");
		String path = RoQZKHelpers.makePath(exchangePath, leafNode);
		return RoQZKHelpers.getDataString(client, path);
	}

	private String getZKPath(Exchange exchange) {
		String path =  RoQZKHelpers.makePath(cfg.znode_monitorState, exchange.monitorID, cfg.znode_monitor_exchangesState, exchange.exchangeID);
		return path;
	}
	
}
