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
package org.roqmessaging.management;

import org.apache.log4j.Logger;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.roqmessaging.client.IRoQConnection;
import org.roqmessaging.client.IRoQPublisher;
import org.roqmessaging.client.IRoQSubscriber;
import org.roqmessaging.client.IRoQSubscriberConnection;
import org.roqmessaging.clientlib.factory.IRoQConnectionFactory;
import org.roqmessaging.core.factory.RoQConnectionFactory;
import org.roqmessaging.core.utils.RoQUtils;

/**
 * Class TestLogicalQueue
 * <p>
 * Description: Test the logical queue factory methods.
 * 
 * @author sskhiri
 */
public class TestLogicalQueue {
	private Logger logger = Logger.getLogger(TestLogicalQueue.class);
	private GlobalConfigurationManager configurationManager = null;
	private LogicalQFactory factory = null;
	private HostConfigManager hostConfigManager = null;

	/**
	 * @throws java.lang.Exception
	 */
	@Before
	public void setUp() throws Exception {
		// 1. Start the configuration
		this.logger.info("Initial setup Start global config thread");
		this.logger.info("Start global config...");
		if (configurationManager == null) {
			configurationManager = new GlobalConfigurationManager();
			Thread configThread = new Thread(configurationManager);
			configThread.start();
		}
		this.logger.info("Start host config...");
		if (hostConfigManager == null) {
			hostConfigManager = new HostConfigManager();
			Thread hostThread = new Thread(hostConfigManager);
			hostThread.start();
		}
		this.logger.info("Start factory config...");
		if (factory == null)
			factory = new LogicalQFactory(RoQUtils.getInstance().getLocalIP().toString());
	}

	/**
	 * @throws java.lang.Exception
	 */
	@After
	public void tearDown() throws Exception {
		this.configurationManager.getShutDownMonitor().shutDown();
		this.hostConfigManager.getShutDownMonitor().shutDown();
		this.factory.clean();
		Thread.sleep(3000);
	}

	@Test
	public void testQueueTopologyRequest() {
		logger.info("Start the main test");
		// Let 1 sec to init the thread
		try {
			Thread.sleep(1000);
		} catch (InterruptedException e) {
			logger.error("Error while waiting", e);
		}
		factory.refreshTopology();
	}

	@Test
	public void testCreateQueueRequest() {
		logger.info("Start create queue test");
		// Let 1 sec to init the thread
		try {
			Thread.sleep(1000);
		} catch (InterruptedException e) {
			logger.error("Error while waiting", e);
		}
		// 1. Add new host
		String host = RoQUtils.getInstance().getLocalIP();
		this.configurationManager.addHostManager(host);
		// 2. Create the factory
		try {
			factory.createQueue("Sabri", host);
			IRoQConnectionFactory factory = new RoQConnectionFactory("localhost");
			
			// add a subscriber
			IRoQSubscriberConnection subConnection = factory.createRoQSubscriberConnection("Sabri", "key");
			// 2. Open the connection to the logical queue
			subConnection.open();
			// 3. Register a message listener
			subConnection.setMessageSubscriber(new IRoQSubscriber() {
				public void onEvent(byte[] msg) {
					String content = new String(msg, 0, msg.length);
					assert content.equals("hello");
					logger.info("In message lIstener recieveing :" + content);
				}
			});
			
			// Add a publisher
			// 1. Creating the connection
			IRoQConnection connection = factory.createRoQConnection("Sabri");
			connection.open();
			// 2. Creating the publisher and sending message
			IRoQPublisher publisher = connection.createPublisher();
			publisher.sendMessage("key".getBytes(), "hello".getBytes());

			Thread.sleep(500);
			this.factory.removeQueue("Sabri");
			Thread.sleep(3000);
			// factory.createQueue("Sabri2", host);
		} catch (IllegalStateException e) {
			logger.error("Error while waiting", e);
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

}
