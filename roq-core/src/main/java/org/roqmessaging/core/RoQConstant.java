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

/**
 * Interface RoQConstant
 * <p> Description: This interface defines the different message types that are exchanges between RoQ elements.
 * 
 * @author Sabri Skhiri
 */
public interface RoQConstant {
	
	//Constants used at monitor level to communicate with elements
	
	/**
	 * Statistic channels on which the  stat are sent
	 */
	public static int CHANNEL_STAT =0;
	
	/**
	 * Communication channel between the exchange and the monitor
	 */
	public static int CHANNEL_EXCHANGE=1;
	
	/**
	 * Channel to configure the initalisation of elements
	 */
	public static int CHANNEL_INIT =2;
	
	/**
	 * Send an init request code.
	 */
	public static int INIT_REQ =2;

	/**
	 * GLobal Configuration request to create a queue
	 */
	public static int CONFIG_CREATE_QUEUE = 1002;
	
	/**
	 * GLobal Configuration request to create a queue 
	 */
	public static int CONFIG_CREATE_QUEUE_OK = 1003;
	
	/**
	 * GLobal Configuration request to create a queue 
	 */
	public static int CONFIG_CREATE_QUEUE_FAIL = 1004;
	

}
