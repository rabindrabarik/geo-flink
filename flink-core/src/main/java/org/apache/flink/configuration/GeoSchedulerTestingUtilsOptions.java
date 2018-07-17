package org.apache.flink.configuration;

public class GeoSchedulerTestingUtilsOptions {
	public static ConfigOption<Integer> slotsForTaskManagerAtIndex(int index) {
		return new ConfigOption<Integer>("task-manager-" + index + "-slots", 1);
	}

	public static ConfigOption<String> geoLocationForTaskManagerAtIndex(int index) {
		return new ConfigOption<String>("task-manager-" + index + "-geo-location", "UNKNOWN");
	}
}
