package org.apache.flink.runtime.clusterframework.types;

public class GeoLocation {
	public static GeoLocation UNKNOWN = new GeoLocation("UNKNOWN");

	public static GeoLocation create(String geoLocationName) {
		return new GeoLocation(geoLocationName);
	}

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
