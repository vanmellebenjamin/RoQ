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
package org.roqmessaging.management.launcher;

import org.roqmessaging.management.HostConfigManager;
import org.roqmessaging.management.launcher.hook.ShutDownHook;

/**
 * Class HostConfigManagerLauncher 
 * <p> Description: Launcher for {@linkplain HostConfigManager}
 * Launched by  java -Djava.library.path=/usr/local/lib -cp roq-management-1.0-SNAPSHOT-jar-with-dependencies.jar org.roqmessaging.management.launcher.HostConfigManagerLauncher <global config IP>
 * 
 * @author sskhiri
 */
public class HostConfigManagerLauncher {
	
	/**
	 * @param args no argument shall be provided,  it starts on the port 5100
	 */
	public static void main(String[] args) {
		System.out.println("Starting the local host configuration manager");
		if(args.length <2){
			System.out.println("you must provide the global config manager server address");
			System.exit(0);
		}
		if(args.length !=1 || args.length !=2){
			System.out.println("you must provide  either the< global config manager server address> or < global config manager server address> <network interface to register> ");
			System.exit(0);
		}
		//Init
		HostConfigManager hostManager = null;		
		if(args.length ==1){
			//Just the global config address
			System.out.println("Register on " +args[0]);
			hostManager = new HostConfigManager(args[0]);
		}else{
			System.out.println("Register on " +args[0] +" on network interface "+ args[1]);
			hostManager = new HostConfigManager(args[0],args[1]);
		}
		ShutDownHook hook = new ShutDownHook(hostManager.getShutDownMonitor());
		Runtime.getRuntime().addShutdownHook(hook);
		//Start
		Thread configThread = new Thread(hostManager);
		configThread.start();
		try {
			while (true) {
				Thread.sleep(500);
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
	
}

