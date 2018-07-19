package org.apache.flink.runtime.jobmanager.scheduler;

import org.apache.flink.runtime.clusterframework.types.GeoLocation;

public interface BandwidthProvider {
	double getBandwidth(GeoLocation from, GeoLocation to);

	boolean hasBandwidth(GeoLocation locationFrom, GeoLocation locationTo);
}
