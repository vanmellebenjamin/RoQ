package org.roqmessaging.zookeeper;

public class Metadata {
	public static class HCM {
		// address format: "x.y.z"
		public String address;
		public HCM(String address) {
			this.address = address;
		}
		
		public String zkNodeString() {
			return address.replace("/", "").replace(":", "");
		}
		
		@Override
		public boolean equals(Object obj) {
			if (this == obj)
				return true;
			if (obj == null)
				return false;
			if (getClass() != obj.getClass())
				return false;
			HCM other = (HCM) obj;
			if (address == null) {
				if (other.address != null)
					return false;
			} else if (!address.equals(other.address))
				return false;
			return true;
		}
	}
	
	public static class BackupMonitor {
		public String hcmAddress;
		public String monitorAddress;
		public String statMonitorAddress;
		
		public BackupMonitor(String state) {
			String[] elem = state.split(",");
			hcmAddress = elem[0];
			monitorAddress = elem[1];
			statMonitorAddress = elem[2];
		}
		
		public String zkNodeString() {
			return hcmAddress.replace("/", "").replace(":", "");
		}
		
		public String getData() {
			return hcmAddress + "," + monitorAddress + "," + statMonitorAddress;
		}
		
		@Override
		public boolean equals(Object obj) {
			if (this == obj)
				return true;
			if (obj == null)
				return false;
			if (getClass() != obj.getClass())
				return false;
			BackupMonitor other = (BackupMonitor) obj;
			if (hcmAddress == null) {
				if (other.hcmAddress != null)
					return false;
			} else if (monitorAddress == null) {
				if (other.monitorAddress != null)
					return false;
			} else if (statMonitorAddress == null) {
				if (other.statMonitorAddress != null)
					return false;
			} else if (!(monitorAddress.equals(other.monitorAddress) 
					&& hcmAddress.equals(other.hcmAddress) 
					&& statMonitorAddress.equals(other.statMonitorAddress)))
				return false;
			return true;
		}
	}
	
	public static class Queue {
		public String name;
		public Queue(String name) {
			this.name = name;
		}
		@Override
		public boolean equals(Object obj) {
			if (this == obj)
				return true;
			if (obj == null)
				return false;
			if (getClass() != obj.getClass())
				return false;
			Queue other = (Queue) obj;
			if (name == null) {
				if (other.name != null)
					return false;
			} else if (!name.equals(other.name))
				return false;
			return true;
		}
	}
	
	public static class Monitor {
		// address format: "tcp://x.y.z:port"
		public String address;
		public Monitor(String address) {
			this.address = address;
		}
		
		public String zkNodeString() {
			return address.replace("/", "").replace(":", "");
		}
		
		@Override
		public boolean equals(Object obj) {
			if (this == obj)
				return true;
			if (obj == null)
				return false;
			if (getClass() != obj.getClass())
				return false;
			Monitor other = (Monitor) obj;
			if (address == null) {
				if (other.address != null)
					return false;
			} else if (!address.equals(other.address))
				return false;
			return true;
		}
	}
	
	public static class Exchange {
		public String address;
		public Exchange(String address) {
			this.address = address;
		}
		
		public String zkNodeString() {
			return address.replace("/", "").replace(":", "");
		}
		
		@Override
		public boolean equals(Object obj) {
			if (this == obj)
				return true;
			if (obj == null)
				return false;
			if (getClass() != obj.getClass())
				return false;
			Exchange other = (Exchange) obj;
			if (address == null) {
				if (other.address != null)
					return false;
			} else if (!address.equals(other.address))
				return false;
			return true;
		}
	}
	
	public static class StatMonitor {
		// address format: "tcp://x.y.z:port"
		public String address;
		public StatMonitor(String address) {
			this.address = address;
		}
		@Override
		public boolean equals(Object obj) {
			if (this == obj)
				return true;
			if (obj == null)
				return false;
			if (getClass() != obj.getClass())
				return false;
			StatMonitor other = (StatMonitor) obj;
			if (address == null) {
				if (other.address != null)
					return false;
			} else if (!address.equals(other.address))
				return false;
			return true;
		}
	}
	
	// Private constructor because Metadata is just a namespace.
	private Metadata() {}
}