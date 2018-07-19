package org.apache.flink.streaming.api.operators;

import org.apache.flink.runtime.clusterframework.types.GeoLocation;
import org.apache.flink.runtime.jobmanager.scheduler.GeoScheduler;

public interface GeoLocationAwareOperator {
	/**
	 * @return the {@link GeoLocation} key for this stream source. If set to the key of the {@link GeoLocation}
	 * where the data is produced it will lead to better scheduling decisions when using a {@link GeoScheduler}
	 * */
	String getGeoLocationKey();

	/**
	 * Sets the {@link GeoLocation} key for this stream source. If set to the key of the {@link GeoLocation}
	 * where the data is produced it will lead to better scheduling decisions when using a {@link GeoScheduler}
	 * */
	void setGeoLocationKey(String geoLocationKey);
}

