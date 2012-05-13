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
package org.roqmessaging.management.serializer;

import java.util.ArrayList;
import java.util.List;

import org.apache.log4j.Logger;
import org.bson.BSON;
import org.bson.BSONObject;
import org.bson.BasicBSONDecoder;
import org.bson.BasicBSONEncoder;
import org.bson.BasicBSONObject;
import org.roqmessaging.management.server.state.QueueManagementState;

/**
 * Class BSONSerializer
 * <p> Description: Responsible for serializing the management configuration in BSON.
 * This class uses the Mongo DB driver for BSON (https://github.com/mongodb/mongo-java-driver/).
 * 
 * @author sskhiri
 */
public class RoQBSONSerializer implements IRoQSerializer {
	private Logger logger = Logger.getLogger(RoQBSONSerializer.class);
	//Bson decoder
	private BasicBSONDecoder decoder = new BasicBSONDecoder();

	/**
	 * @see org.roqmessaging.management.serializer.IRoQSerializer#serialiseQueues(java.util.ArrayList)
	 */
	public byte[] serialiseQueues(List<QueueManagementState> queues) {
		logger.debug("Encoding Qs  in BSON ...");
		//1. We create a list of BSON objects
		List<BSONObject> bsonArray = new ArrayList<BSONObject>();
		//2. We add a line for each Q
		for (QueueManagementState queue_i : queues) {
			BSONObject oQ = new BasicBSONObject();
			oQ.put("Name", queue_i.getName());
			oQ.put("Host", queue_i.getHost());
			oQ.put("State", queue_i.isRunning());
			bsonArray.add(oQ);
		}
		
		//Build the main array containing all queues
		BSONObject  mainQ= new BasicBSONObject();
		mainQ.put("Queues", bsonArray);
		logger.debug(mainQ.toString());
		
		//Encode test
		BasicBSONEncoder encoder = new BasicBSONEncoder();
		return encoder.encode(mainQ);
		
	}

	/**
	 * @see org.roqmessaging.management.serializer.IRoQSerializer#serialiseHosts(java.util.ArrayList)
	 */
	public byte[] serialiseHosts(ArrayList<String> hosts) {
		logger.debug("Encoding hosts in BSON ...");
		// Create the bson object
		BSONObject bsonObject = new BasicBSONObject();
		bsonObject.put("hosts", hosts);
		logger.debug(bsonObject.toString());

		// Encode the object
		return  BSON.encode(bsonObject);
	}

	/**
	 * @see org.roqmessaging.management.serializer.IRoQSerializer#serialiseCMDID(java.lang.String)
	 */
	public byte[] serialiseCMDID(int cmd) {
		BSONObject bsonObject = new BasicBSONObject();
		bsonObject.put("CMD_ID", cmd);
		logger.debug("Encoding CMD ID in BSON= "+bsonObject.toString());
		return BSON.encode(bsonObject);
	}

	/**
	 * @see org.roqmessaging.management.serializer.IRoQSerializer#unSerializeQueues(byte[])
	 */
	public List<QueueManagementState> unSerializeQueues(byte[] encodedQ) {
		logger.debug("Unserializing encoded Q");
		BSONObject decodedQ = decoder.readObject(encodedQ);
		
		//Building the Queue state Array
		@SuppressWarnings("unchecked")
		ArrayList<BSONObject> dedodedList = (ArrayList<BSONObject>) decodedQ.get("Queues");
		List<QueueManagementState> queues = new ArrayList<QueueManagementState>();
		
		//Through the list of decoded object, we re build the states
		for (BSONObject bsonObject : dedodedList) {
			QueueManagementState state_i =  new QueueManagementState((String) bsonObject.get("Name"),
					(String) bsonObject.get("Host"),  (Boolean) bsonObject.get("State"));
			queues.add(state_i);
			logger.debug(state_i.toString());
		}
		return queues;
	}

	/**
	 * @see org.roqmessaging.management.serializer.IRoQSerializer#unSerializeHosts(byte[])
	 */
	@SuppressWarnings("unchecked")
	public List<String> unSerializeHosts(byte[] encodedH) {
		BSONObject newHostObject = decoder.readObject(encodedH);
		return (ArrayList<String>) newHostObject.get("hosts");
	}

}
