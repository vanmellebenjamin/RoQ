package org.roqmessaging.management.monitor;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.concurrent.locks.ReentrantLock;

import org.apache.log4j.Logger;
import org.roqmessaging.core.RoQConstantInternal;
import org.roqmessaging.factory.HostProcessFactory;
import org.roqmessaging.utils.LocalState;
import org.roqmessaging.utils.Time;

import com.google.common.collect.ImmutableList;

public class ProcessMonitor implements Runnable {
	Logger logger = Logger.getLogger(ProcessMonitor.class);
	
	// The timeout after which process is considered a dead
	private int processTimeOut;
	private int maxTimeToStart;
	
	// used to shutdown thread
	public volatile boolean isRunning = true;
	
	private String LocalStatePath;
	
	/**
	 * The maps/lists to manage the processes
	 * the keys are often the port of the process because
	 * it is unique for a given host
	 */
	private HashMap<String, MonitoredProcess> processes = 	  new HashMap<String, MonitoredProcess>();
	private HashMap<String, Integer> processesToAdd = new HashMap<String, Integer>();
	private ArrayList<String> processesFailed =		  new ArrayList<String>();
	private ArrayList<String> processesRunning = 	  new ArrayList<String>();
	
	// Lock to avoid race condition when modifying processesToAdd Map
	private ReentrantLock processLock = new ReentrantLock();
	
	// Allows the thread to start/stop processes
	private HostProcessFactory processFactory;
	
	/**
	 * A class to represent a monitored
	 * process
	 */
	private class MonitoredProcess {
		private final Process process;
		private final String key;
		private final int type;
		
		public MonitoredProcess(Process process, String key, int type) {
			this.process = process;
			this.key = key;
			this.type = type;
		}
		
		public Process getProcess() {
			return process;
		}
		
		public String getKey() {
			return key;
		}
		
		public int getType() {
			return type;
		}
		
	}
	
	public ProcessMonitor(String LocalStatePath, int processTimeout, int maxTimeToStart, HostProcessFactory processFactory) 
				throws IOException {
		super();
		this.LocalStatePath = LocalStatePath;
		this.processTimeOut = processTimeout;
		this.maxTimeToStart = maxTimeToStart;
		this.processFactory = processFactory;
	}
	
	public void addprocess(String id, Process process, int type, String key) {
		logger.info("adding process: " + id + " in process monitoring system");
		processLock.lock();
		processes.put(id, new MonitoredProcess(process, key, type));
		processesToAdd.put(id, Time.currentTimeSecs());
		processLock.unlock();
	}
	
	/**
	 * Get the worker heartbeats in a map
	 * @throws IOException 
	 * 
	 *  
	 */
	public HashMap<String, Long> getprocessesHB() 
			throws IOException {
		LocalState localState;
		HashMap<String, Long> processHbs = new HashMap<String, Long>();
		for (String process : processesRunning) {
			localState = new LocalState(LocalStatePath + "/" + process);
			processHbs.put(process ,(Long) localState.get("HB"));
		}
		return processHbs;
	}

	@Override
	/**
	 * Thread loop can be stopped
	 * by setting isRunning to false
	 */
	public void run() {
		HashMap<String, Long> processesHB;
		logger.info("Starting heartbeat monitor");
		while (isRunning) {
			try {
				logger.info("check up loop");
				// Add the just spawned process
				// to the running list when
				// max start time has elapsed
				startProcessMonitoring();
				// Get heartbeat of each running process
				processesHB = getprocessesHB();
				// reclassify process according to
				// timeout
				classifyProcesses(processesHB);
				// Restart timed out processes
				restartFailedProcesses();
				// Dont waste resources
				Thread.sleep((processTimeOut * 1000) / 2);
			} catch (IOException e) {
				logger.error("Failed to read locState DB");
				e.printStackTrace();
			} catch (InterruptedException e) {
				logger.error("Thread interrupted");
				e.printStackTrace();
			}
		}
		logger.info("heartbeat monitor stopped");
	}
	
	/***
	 * Add the just started processes in the running list
	 * when the max time to start had elapsed
	 */
	private void startProcessMonitoring() {
		processLock.lock();
		ImmutableList<String> processesList = ImmutableList.copyOf(processesToAdd.keySet());
		for (String processID: processesList) {
			if ((Time.currentTimeSecs() - processesToAdd.get(processID)) > maxTimeToStart) {
				logger.info("begin to monitor: " + processID);
				processesToAdd.remove(processID);
				processesRunning.add(processID);
			}
		}
		processLock.unlock();
	}
	
	/**
	 * Move the processes which has time out in
	 * the failed list
	 * @param processesHB
	 */
	private void classifyProcesses(HashMap<String, Long> processesHB) {
		int current = Time.currentTimeSecs();
		ImmutableList<String> processesList = ImmutableList.copyOf(processesRunning);
		for (String processID : processesList) {
			logger.info("check: " + processID);
			if (processesHB.get(processID) == null || current - processesHB.get(processID) > processTimeOut) {
				logger.info("process: " + processID + " has timed out");
				processesFailed.add(processID);
				processesRunning.remove(processID);
			} else {
				logger.info("process: " + processID + " has sent hb");
			}
		}
	}
	
	/**
	 * Restart the processes which failed
	 * @param processesHB
	 */
	private void restartFailedProcesses() {
		ImmutableList<String> processesList = ImmutableList.copyOf(processesFailed);
		String key;
		int type;
		for (String processID : processesList) {
			logger.info("restarting process: " + processID);
			key = processes.get(processID).getKey();
			type = processes.get(processID).getType();
			stopAndRemoveProcess(processID);
			restartProcess(type, key);
			
		}
	}
	
	/**
	 * Stop the process and removes it from the list
	 */
	private void stopAndRemoveProcess(String processID) {
		MonitoredProcess process;
		process = processes.get(processID);
		// kill the process
		if (	process.getType() == RoQConstantInternal.PROCESS_MONITOR) {
			process.getProcess().destroy();
			processesFailed.remove(processID);
			processes.remove(processID);
		}
	}
	
	/**
	 * Restart a process
	 * @param processesHB
	 */
	private boolean restartProcess(int type, String key) {
		switch (type) {
		case RoQConstantInternal.PROCESS_MONITOR:
			processFactory.startNewMonitorProcess(key);
			break;
		case RoQConstantInternal.PROCESS_SCALING:
			
			break;
		case RoQConstantInternal.PROCESS_STAT:
			
			break;
		case RoQConstantInternal.PROCESS_EXCHANGE:
			
			break;
		}
		return false;
	}
	
}
