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
}
