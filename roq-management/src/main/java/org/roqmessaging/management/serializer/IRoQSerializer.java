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

import org.roqmessaging.management.server.state.QueueManagementState;

/**
 * Interface IRoQSerializer
 * <p> Description: define the common behavior of all serializer used for communicate with outside.
 * The client are intended to use the adapter factory pattern to select the right implementation.
 * 
 * @author sskhiri
 */
public interface IRoQSerializer {
	
	/**
	 * @param queues the list of queue to serialized
	 * @return the serialised version
	 */
	public byte[] serialiseQueues(List<QueueManagementState> queues);
	
	/**
	 * @param encodedQ the encoded Qs
	 * @return the list of Queue Management state
	 */
	public List<QueueManagementState> unSerializeQueues(byte [] encodedQ);
	
	/**
	 * @param hosts the list of RoQ host (ip address)
	 * @return the serialized message
	 */
	public byte[] serialiseHosts(ArrayList<String> hosts);
	
	/**
	 * @param encodedH the encoded list of hosts 
	 * @return the list of Host ip address
	 */
	public List<String> unSerializeHosts(byte [] encodedH);
	
	/**
	 * @param cmd the cmd ID to serialize
	 * @return the encoded cmd ID
	 */
	public byte[] serialiseCMDID(int cmd);
	
	/**
	 * @param Monitor the monitor host
	 * @param StatMonitor the stat monitor host
	 * @return the serialized byte array
	 */
	public byte[] serialiazeMonitorInfo(String monitor, String statMonitor);
	
}