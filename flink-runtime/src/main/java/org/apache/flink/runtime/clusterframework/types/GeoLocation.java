package org.apache.flink.runtime.clusterframework.types;

public class GeoLocation {
	public static GeoLocation UNKNOWN = new GeoLocation("UNKNOWN");

	private String geoLocationName;

	public GeoLocation(String geoLocationName) {
		this.geoLocationName = geoLocationName;
	}

	public String getGeoLocationName() {
		return geoLocationName;
	}

	@Override
	public String toString() {
		return geoLocationName;
	}

	@Override
	public boolean equals(Object o) {
		if (this == o) {
			return true;
		} else if (o == null || o.getClass() != getClass()) {
			return false;
		} else {
			return geoLocationName.equals(((GeoLocation) o).geoLocationName);
		}
	}
}
