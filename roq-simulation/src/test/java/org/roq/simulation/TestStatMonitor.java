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
package org.roq.simulation;

import org.apache.log4j.Logger;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.roq.simulation.stat.KPISubscriber;
import org.roqmessaging.client.IRoQConnection;
import org.roqmessaging.client.IRoQPublisher;
import org.roqmessaging.client.IRoQSubscriber;
import org.roqmessaging.client.IRoQSubscriberConnection;
import org.roqmessaging.clientlib.factory.IRoQConnectionFactory;
import org.roqmessaging.clientlib.factory.IRoQLogicalQueueFactory;
import org.roqmessaging.core.factory.RoQConnectionFactory;
import org.roqmessaging.core.utils.RoQUtils;
import org.roqmessaging.management.LogicalQFactory;

/**
 * Class TestStatMonitor
 * <p> Description: Test the Statistic monitor behavior
 * 
 * @author sskhiri
 */
public class TestStatMonitor {
	private RoQAllLocalLauncher launcher = null;
	private 	KPISubscriber kpiSubscriber = null;
	private Logger logger = Logger.getLogger(TestStatMonitor.class);
	
	/**
	 * @throws java.lang.Exception
	 */
	@Before
	public void setUp() throws Exception {
		this.launcher = new RoQAllLocalLauncher();
		this.launcher.setUp();
	}

	/**
	 * @throws java.lang.Exception
	 */
	@After
	public void tearDown() throws Exception {
		this.launcher.tearDown();
		this.kpiSubscriber.shutDown();
	}

	@Test
	public void test() {
		//1. Create a Queue
		IRoQLogicalQueueFactory logicalQFactory = new LogicalQFactory(launcher.getConfigurationServer());
		logicalQFactory.createQueue("queue1", RoQUtils.getInstance().getLocalIP());
		
		//2. Init the KPI subscriber
		kpiSubscriber = new KPISubscriber(launcher.getConfigurationServer(), "queue1", false);
		new Thread(kpiSubscriber).start();
		
		//3. Create a subscriber
		IRoQConnectionFactory factory = new RoQConnectionFactory(launcher.getConfigurationServer());
		// add a subscriber
		IRoQSubscriberConnection subConnection = factory.createRoQSubscriberConnection("queue1", "key");
		// Open the connection to the logical queue
		subConnection.open();
		// Register a message listener
		IRoQSubscriber subs = new IRoQSubscriber() {
			public void onEvent(byte[] msg) {
				String content = new String(msg, 0, msg.length);
				assert content.equals("hello");
				//logger.info("In message lIstener recieveing :" + content);
			}
		};
		subConnection.setMessageSubscriber(subs);
		
		//4. Create a publisher// Add a publisher
		// Creating the connection
		IRoQConnection connection = factory.createRoQConnection("queue1");
		connection.open();
		// Creating the publisher and sending message
		IRoQPublisher publisher = connection.createPublisher();
		// Wait for the connection is established before sending the first message
		connection.blockTillReady(10000);
		
		//5 Sending the message
		logger.info("Sending MESSAGES ...");
		for (int i = 0; i < 500; i++) {
			publisher.sendMessage("key".getBytes(), ("hello"+i).getBytes());
		}
		
		//3 Wait &. Check the content
		try {
			Thread.sleep(8000);
			logger.info("Sending MESSAGES ...");
			for (int i = 0; i < 500; i++) {
				publisher.sendMessage("key".getBytes(), ("hello"+i).getBytes());
			}
			Thread.sleep(15000);
			
			//End close connection
			connection.close();
			subConnection.close();
			
			//Delete the queue
			logicalQFactory.removeQueue("queue1");
			Thread.sleep(2000);
			
		} catch (InterruptedException e) {
			e.printStackTrace();
		}

	}

}