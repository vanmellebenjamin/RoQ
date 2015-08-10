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
package org.roq.simulation.test;

import java.net.ConnectException;

import org.apache.log4j.Logger;
import org.junit.After;
import org.junit.Before;
import org.roq.simulation.RoQDockerLauncher;
import org.roqmessaging.client.IRoQConnection;
import org.roqmessaging.client.IRoQPublisher;
import org.roqmessaging.client.IRoQQueueManagement;
import org.roqmessaging.core.factory.RoQConnectionFactory;
import org.roqmessaging.core.factory.RoQQueueManager;

/**
 * Class RQDockerTestCase

 * @author bvanmelle
 */
public class RoQDockerTestCase {
	protected RoQDockerLauncher launcher = null;
	protected Logger logger = Logger.getLogger(RoQDockerTestCase.class);
	protected static RoQConnectionFactory connection = null;
	protected static IRoQQueueManagement queueManager = null;
	
	 /**
	 * @throws java.lang.Exception
	 */
	@Before
	public void setUp() throws Exception {
		this.logger.info("Setup TEST with Docker containers");
		Thread.sleep(3000);
		this.launcher = new RoQDockerLauncher();
		this.launcher.setUp();
		Thread.sleep(10000); // Wait for cluster configuration (HCM registration etc.)
		String zkConnectionString = launcher.getZkConnectionString();
		queueManager = new RoQQueueManager(zkConnectionString, 8, 8000);
		connection = new RoQConnectionFactory(zkConnectionString, 8, 8000);
	}
	
	/**
	 * @throws java.lang.Exception
	 */
	@After
	public void tearDown() throws Exception {
		connection.close();
		queueManager.close();
		this.logger.info("Tear Down TEST");
		this.launcher.tearDown();
		Thread.sleep(4000);
	}
		
	/**
	 * Create a queue
	 * @param qName the name of the queue
	 * @throws Exception
	 */
	public static boolean initQueue(String qName) 
			throws IllegalStateException, ConnectException {
		return queueManager.createQueue(qName);
	}
	
	/**
	 * Create a queue
	 * @param qName the name of the queue
	 * @param the address on which master host is installed
	 * @throws IllegalStateException 
	 * @throws ConnectException 
	 * @throws Exception
	 */
	public static boolean initQueue(String qName, String targetAddress) throws ConnectException, IllegalStateException {
		return queueManager.createQueue(qName, targetAddress);
	}
	
	/**
	 * Check if queue exists
	 * @param qName the name of the queue
	 * @throws Exception
	 */
	public static boolean queueExists(String qName) 
			throws IllegalStateException, ConnectException {
		return queueManager.queueExists(qName);
	}
	
	/**
	 * A thread which send one message every 500ms
	 * @author benjamin
	 *
	 */
	protected class MessageSender implements Runnable {
		private IRoQConnection connection;
		private IRoQPublisher publisher;
		private int nbMessages;
		
		public MessageSender(IRoQConnection connection, int nbMessages) {
			super();
			this.connection = connection;
			this.nbMessages = nbMessages;
			connection.open();
			// Creating the publisher and sending message
			publisher = connection.createPublisher();
		}
		
		@Override
		public void run() {
			connection.blockTillReady(10000);

			// 5 Sending the message
			logger.info("Sending MESSAGES ...");
			for (int i = 0; i < nbMessages; i++) {
				try {
					Thread.sleep(500); // wait 500 ms between each messages
				} catch (InterruptedException e) { e.printStackTrace();	}
				
				publisher.sendMessage("key".getBytes(), ("hello" + i).getBytes());
			}
			
		}
		
	}
	
	/**
	 * Remove a queue
	 * @param qName the name of the queue
	 * @throws Exception
	 */
	public static void rmQueue(String qName) 
			throws IllegalStateException, ConnectException {
		queueManager.removeQueue(qName);
	}
}