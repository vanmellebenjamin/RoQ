package org.roqmessaging.management.zookeeper;

import java.io.EOFException;
import java.io.IOException;
import java.util.Collection;
import java.util.List;
import java.util.ArrayList;


import org.apache.curator.framework.recipes.leader.LeaderLatch;
import org.apache.curator.x.discovery.ServiceCache;
import org.apache.curator.x.discovery.ServiceDiscovery;
import org.apache.curator.x.discovery.ServiceDiscoveryBuilder;
import org.apache.curator.x.discovery.ServiceInstance;
import org.apache.curator.x.discovery.details.JsonInstanceSerializer;
import org.apache.log4j.Logger;
import org.roqmessaging.core.utils.RoQUtils;
import org.roqmessaging.zookeeper.RoQZKHelpers;
import org.roqmessaging.zookeeper.RoQZooKeeper;
import org.roqmessaging.management.GlobalConfigLeaderListener;
import org.roqmessaging.management.zookeeper.Metadata.HCM;
import org.roqmessaging.management.zookeeper.RoQZooKeeperConfig;

/**
 * TODO Split the class in two classes:
 * GCMClient
 * HCMClient
 */
public class RoQZooKeeperClient extends RoQZooKeeper {
	private final Logger log = Logger.getLogger(getClass());
	private LeaderLatch leaderLatch;
	private RoQZooKeeperConfig cfg;
	
	private ServiceDiscovery<HostDetails> serviceDiscovery = null;
	
	private ServiceInstance<HostDetails> instance;
	
	private ServiceCache<HostDetails> cache;
	
	public RoQZooKeeperClient(RoQZooKeeperConfig config) {
		super(config);
		log.info("");
		
		cfg = config;	
	}
	
	public void closeGCM() throws IOException {
		if (cache != null)
			cache.close();
		if (serviceDiscovery != null)
			serviceDiscovery.close();
		super.close();
	}
	
	/**
	 * This method is used to block the process
	 * until it becomes the leader.
	 * @throws InterruptedException 
	 * @throws EOFException 
	 */
	public void startLeaderElection() {
		leaderLatch = new LeaderLatch(client, cfg.znode_gcm);
		try {
			leaderLatch.start();
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
	
	public void startServiceDiscovery() throws Exception {
		String path = RoQZKHelpers.makePath(cfg.znode_hcm);
		JsonInstanceSerializer<HostDetails> serializer = new JsonInstanceSerializer<HostDetails>(HostDetails.class);
		serviceDiscovery = ServiceDiscoveryBuilder.builder(HostDetails.class).client(client).basePath(path).serializer(serializer).build();
		serviceDiscovery.start();
		cache = serviceDiscovery.serviceCacheBuilder().name("HCM").build();
		cache.addListener(new HcmListener());
		cache.start();
	}
	
	/**
	 * Create the zookeeper path tree for RoQ
	 */
	public void initZkClusterNodes() {
		String path = RoQZKHelpers.makePath(cfg.znode_queues);
		RoQZKHelpers.createZNodeAndParents(client, path);
		path = RoQZKHelpers.makePath(cfg.znode_queueTransactions);
		RoQZKHelpers.createZNodeAndParents(client, path);
		path = RoQZKHelpers.makePath(cfg.znode_exchangeTransactions);
		RoQZKHelpers.createZNodeAndParents(client, path);
	}
	
	/**
	 * This method is used to block the process
	 * until it becomes the leader.
	 * @throws InterruptedException 
	 * @throws EOFException 
	 */
	public void waitUntilLeader() throws EOFException, InterruptedException {
		leaderLatch.await();
		// This listener allow to manage the case
		// where the connection with zookeeper is lost
		// ie: the process must stop to process messages
		leaderLatch.addListener(new GlobalConfigLeaderListener());
	}	

	/**
	 * This method return true if this instance of the zk client
	 * has the leadership
	 * @return true if it is the leader
	 */
	public boolean isLeader() {
		log.info("");
		return leaderLatch.hasLeadership();
	}
	
	/**
	 * Remove all the nodes in RoQ/
	 */
	public void clear() {
		log.info("");
		
		List<String> rootNodes = RoQZKHelpers.getChildren(client, "");
		
		// If something went wrong, abort.
		if (rootNodes == null) {
			log.debug("Helpers.getChildren() failed. Aborting clean().");
		}
		
		for (String node : rootNodes) {
			// Do not delete the gcm node because it is used for leader election
			// and it is handled by leaderLatch.
			if (!node.equals(cfg.znode_gcm)) {
				RoQZKHelpers.deleteZNodeAndChildren(client, "/"+node);
				log.info("--root node " +node +" deleted");
			}
		}
	}
		
	// Create the znode for the HCM if it does not already exist.
	public void setGCMLeader() {
		RoQUtils utils = RoQUtils.getInstance();
		String path = RoQZKHelpers.makePath(cfg.znode_leaderAddress);
		String address = utils.getLocalIP();
		log.info("Set GCM address: " + address + " in ZK path: " + path);
		//RoQZKHelpers.deleteZNode(client, path);
		RoQZKHelpers.createEphemeralZNode(client, path, address.getBytes());
	}

	/**
	 * Get the list of the HCM registered on the cluster
	 * @return a List of HCM addresses
	 * @throws Exception 
	 */
	public List<Metadata.HCM> getHCMList() throws Exception {
		log.info("");
		
		List<Metadata.HCM> hcms = new ArrayList<Metadata.HCM>();
		Collection<ServiceInstance<HostDetails>> instances = cache.getInstances();
		
		for (ServiceInstance<HostDetails> instance : instances) {
			log.info("host address: " + instance.getAddress());
			hcms.add(new Metadata.HCM(instance.getPayload().getAddress()));
		}

		return hcms;
	}
	
	/**
	 * This method remove explicitly a hcm Znode and its watcher
	 * There is actually two ways to lost a hcm znode
	 * by removing the node explicitly (when we use this) method.
	 * Because the hcm has crashed, in this case the watcher detects the node lost
	 * and the GCM triggers a recovery process
	 * @param hcm
	 * @throws Exception 
	 */
	public void removeHCM(Metadata.HCM hcm) throws Exception {
		log.info("");
		if (instance == null)
			throw new IllegalStateException();
		// RoQZKHelpers.deleteZNode(client, getZKPath(hcm));
		serviceDiscovery.unregisterService(instance);
		serviceDiscovery.close();
	}
	
	/**
	 * This method registers an ephemeral Znode
	 * in zookeeper. This Znode will be watched by 
	 * the GCM, in order to handle HCM crash
	 * @param hcm
	 * @throws Exception 
	 */
	public void registerHCM(HCM hcm) throws Exception {
		// Check whether the HCM is not register
		List<HCM> hcms = getHCMList();
		boolean duplicata = false;
		for (HCM hcmTemp : hcms) {
			if (hcmTemp.equals(hcm)) {
				duplicata = true;
			}
		}
		
		// We cannot duplicate the same hcm in the discovery service
		if (!duplicata) {
			log.info("Registering hcm with address: " + hcm.address);
			instance = ServiceInstance.<HostDetails>builder()
					.name("HCM").payload(new HostDetails(hcm.address)).build();
			String path = RoQZKHelpers.makePath(cfg.znode_hcm);
			JsonInstanceSerializer<HostDetails> serializer = new JsonInstanceSerializer<HostDetails>(HostDetails.class);
			serviceDiscovery = ServiceDiscoveryBuilder.builder(HostDetails.class)
					.client(client).basePath(path).serializer(serializer).thisInstance(instance).build();
			serviceDiscovery.start();
		}
	}
	
	/**
	 * Check whether a logical queue
	 * exists in RoQ.
	 * @param queue
	 * @return
	 */
	public boolean queueExists(Metadata.Queue queue) {
		log.info("");
		return RoQZKHelpers.zNodeExists(client, getZKPath(queue));
	}
	
	/**
	 * This method create a transaction for the queue creation in ZK
	 * @param queueName
	 */
	public void createQTransaction (String queueName, String host) {
		log.info("");
		String path = RoQZKHelpers.makePath(cfg.znode_queueTransactions, queueName);
		RoQZKHelpers.createZNode(client, path, host);
	}
	
	/**
	 * This method fetch if a transaction exists for the queue
	 * @param queueName
	 */
	public String qTransactionExists (String queueName) {
		log.info("");
		String path = RoQZKHelpers.makePath(cfg.znode_queueTransactions, queueName);
		return RoQZKHelpers.getDataString(client, path);
	}
	
	/**
	 * This method remove the transaction node when the transaction has been completed
	 * @param queueName
	 */
	public void removeQTransaction (String queueName) {
		log.info("");
		String path = RoQZKHelpers.makePath(cfg.znode_queueTransactions, queueName);
		RoQZKHelpers.deleteZNode(client, path);
	}
	
	/**
	 * This method create the transaction node for an exchange creation process
	 * @param transID
	 */
	public void createExchangeTransaction (String transID, String targetHost) {
		log.info("");
		String path = RoQZKHelpers.makePath(cfg.znode_exchangeTransactions, transID);
		RoQZKHelpers.createZNode(client, path, targetHost);
	}
	
	/**
	  * This method fetch if a transaction exists for the exchange creation
	 * @param transID
	 */
	public String exchangeTransactionExists (String transID) {
		log.info("");
		String path = RoQZKHelpers.makePath(cfg.znode_exchangeTransactions, transID);
		return RoQZKHelpers.getDataString(client, path);
	}
	
	/**
	 * This method remove the transaction node when the transaction has been completed
	 * @param transID
	 */
	public void removeExchangeTransaction (String transID) {
		log.info("");
		String path = RoQZKHelpers.makePath(cfg.znode_exchangeTransactions, transID);
		RoQZKHelpers.deleteZNode(client, path);
	}
	
	public void createQueue(Metadata.Queue queue, Metadata.HCM hcm, Metadata.Monitor monitor, Metadata.StatMonitor statMonitor) {
		log.info("");
		
		String queuePath = getZKPath(queue);
		String monitorPath = RoQZKHelpers.makePath(queuePath, "monitor");
		String statMonitorPath = RoQZKHelpers.makePath(queuePath, "stat-monitor");
		String hcmPath = RoQZKHelpers.makePath(queuePath, "hcm");
		String exchPath = RoQZKHelpers.makePath(queuePath, "exchanges");
		String scalingPath = RoQZKHelpers.makePath(queuePath, "scaling");
		// RoQZKHelpers.createZNodeAndParents(client, queuePath);
		// Add queue children nodes inside a single transaction
		RoQZKHelpers.createQueueZNodes(client, queuePath, monitorPath, 
				monitor.address, statMonitorPath, statMonitor.address, 
				hcmPath, hcm.address, exchPath, scalingPath);
	}
	
	public void removeQueue(Metadata.Queue queue) {
		log.info("");
		RoQZKHelpers.deleteZNodeAndChildren(client, getZKPath(queue));
	}
	
	/**
	 * @param queue
	 * @param flag  if true, the queue is marked as running,
	 *              otherwise it is marked as stopped
	 */
	public void setRunning(Metadata.Queue queue, boolean flag) {
		log.info("");
		if (flag) {
			// create a "running" node for the selected queue
			RoQZKHelpers.createZNode(client, RoQZKHelpers.makePath(getZKPath(queue), "running"));
		} else {
			// delete the "running" node for the selected queue
			// to mark it as stopped
			RoQZKHelpers.deleteZNode(client, RoQZKHelpers.makePath(getZKPath(queue), "running"));
		}
	}

	/**
	 * @param queue
	 * @return true if the queue is marked as running, false otherwise
	 */
	public boolean isRunning(Metadata.Queue queue) {
		log.info("");
		return RoQZKHelpers.zNodeExists(client, RoQZKHelpers.makePath(getZKPath(queue), "running"));
	}
	
	public List<Metadata.Queue> getQueueList() {
		log.info("");
		
		List<Metadata.Queue> queues = new ArrayList<Metadata.Queue>();
		List<String> znodes = RoQZKHelpers.getChildren(client, cfg.znode_queues);
		
		// If something goes wrong, return an empty list.
		if (znodes == null) {
			log.debug("Helpers.getChildren() failed. Aborting getQueueList().");
			return queues;
		}
		
		for (String node : znodes) {
			queues.add(new Metadata.Queue(node));
		}
		return queues;
	}
	
	public Metadata.HCM getHCM(Metadata.Queue queue) {
		log.info("");
		String path = RoQZKHelpers.makePath(getZKPath(queue), "hcm");
		String data = RoQZKHelpers.getDataString(client, path);
		log.info("HCM address: " + data);
		if (data == null) {
			return null;
		}
		return new Metadata.HCM(data);
	}
	
	public Metadata.Monitor getMonitor(Metadata.Queue queue) {
		log.info("");
		String path = RoQZKHelpers.makePath(getZKPath(queue), "monitor");
		String data = RoQZKHelpers.getDataString(client, path);
		log.info("Monitor address: " + data);
		if (data == null) {
			return null;
		}
		return new Metadata.Monitor(data);
	}
	
	public Metadata.StatMonitor getStatMonitor(Metadata.Queue queue) {
		log.info("");
		String path = RoQZKHelpers.makePath(getZKPath(queue), "stat-monitor");
		String data = RoQZKHelpers.getDataString(client, path);
		log.info("StatMonitor address: " + data);
		if (data == null) {
			return null;
		}
		return new Metadata.StatMonitor(data);
	}
	
	public void setCloudConfig(byte[] cloudConfig) {
		log.info("");
		RoQZKHelpers.createZNodeAndParents(client, cfg.znode_cloud);
		RoQZKHelpers.setData(client, cfg.znode_cloud, cloudConfig);
	}
	
	public byte[] getCloudConfig() {
		log.info("");
		return RoQZKHelpers.getData(client, cfg.znode_cloud);
	}
	public void setNameScalingConfig(String name, Metadata.Queue queue) {
		log.info("");
		String path = RoQZKHelpers.makePath(getZKPath(queue), cfg.znode_scaling, "name");
		RoQZKHelpers.createZNodeAndParents(client, path);
		RoQZKHelpers.setData(client, path, name.getBytes());
	}
	public void setHostScalingConfig(byte[] scalingConfig, Metadata.Queue queue) {
		log.info("");
		setScalingConfig(scalingConfig, queue, "host");
	}
	public void setExchangeScalingConfig(byte[] scalingConfig, Metadata.Queue queue) {
		log.info("");
		setScalingConfig(scalingConfig, queue, "exchange");
	}
	public void setQueueScalingConfig(byte[] scalingConfig, Metadata.Queue queue) {
		log.info("");
		setScalingConfig(scalingConfig, queue, "queue");
	}
	public byte[] getNameScalingConfig(Metadata.Queue queue) {
		log.info("");
		String path = RoQZKHelpers.makePath(getZKPath(queue), cfg.znode_scaling, "name");
		return RoQZKHelpers.getData(client, path);
	}
	public byte[] getHostScalingConfig(Metadata.Queue queue) {
		return getScalingConfig(queue, "host");
	}
	public byte[] getExchangeScalingConfig(Metadata.Queue queue) {
		return getScalingConfig(queue, "exchange");
	}
	public byte[] getQueueScalingConfig(Metadata.Queue queue) {
		return getScalingConfig(queue, "queue");
	}
	
	// Private methods
	
	private String getZKPath(Metadata.Queue queue) {
		return RoQZKHelpers.makePath(cfg.znode_queues, queue.name);
	}
	private String getZKPath(Metadata.HCM hcm) {
		return RoQZKHelpers.makePath(cfg.znode_hcm, hcm.address);
	}
	private void setScalingConfig(byte[] scalingConfig, Metadata.Queue queue, String leafNode) {
		log.info("");
		String path = RoQZKHelpers.makePath(getZKPath(queue), cfg.znode_scaling, leafNode);
		
		RoQZKHelpers.createZNodeAndParents(client, path);
		RoQZKHelpers.setData(client, path, scalingConfig);
	}
	private byte[] getScalingConfig(Metadata.Queue queue, String leafNode) {
		log.info("");
		String path = RoQZKHelpers.makePath(getZKPath(queue), cfg.znode_scaling, leafNode);
		return RoQZKHelpers.getData(client, path);
	}	
}
