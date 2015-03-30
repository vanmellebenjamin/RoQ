package org.roqmessaging.zookeeper;

public class CoreMetadata {
	public static class Monitor {
		// address format: "x.y.z"
		public String monitorID;
		public Monitor(String monitorID) {
			this.monitorID = monitorID;
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
			if (monitorID == null) {
				if (other.monitorID != null)
					return false;
			} else if (!monitorID.equals(other.monitorID))
				return false;
			return true;
		}
	}

	public static class Exchange {
		// address format: "x.y.z"
		public String monitorID;
		public String exchangeID;
		public Exchange(String monitorID, String exchangeID) {
			this.exchangeID = exchangeID;
			this.monitorID = monitorID;
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
			if (exchangeID == null) {
				if (other.exchangeID != null)
					return false;
			} else if (monitorID == null) {
				if (other.monitorID != null)
					return false;
			} else if (!(exchangeID.equals(other.exchangeID) && monitorID.equals(other.monitorID)))
				return false;
			return true;
		}
	}
	
	// Private constructor because Metadata is just a namespace.
	protected CoreMetadata() {}
}
