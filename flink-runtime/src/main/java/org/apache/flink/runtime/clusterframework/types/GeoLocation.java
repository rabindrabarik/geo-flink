package org.apache.flink.runtime.clusterframework.types;

import java.io.Serializable;

public class GeoLocation implements Serializable {

	private static final long serialVersionUID = 1L;

	public static GeoLocation UNKNOWN = new GeoLocation("UNKNOWN");

	private String key;

	public GeoLocation(String key) {
		this.key = key;
	}

	public String getKey() {
		return key;
	}

	@Override
	public String toString() {
		return key;
	}

	@Override
	public boolean equals(Object o) {
		if (this == o) {
			return true;
		} else if (o == null || o.getClass() != getClass()) {
			return false;
		} else {
			return key.equals(((GeoLocation) o).key);
		}
	}
}
