package org.apache.flink.runtime.metrics.groups;

import org.apache.flink.runtime.metrics.MetricRegistry;

public class GeoLocationMetricGroup extends GenericMetricGroup {
	public GeoLocationMetricGroup(MetricRegistry registry, AbstractMetricGroup parent, String name) {
		super(registry, parent, name);
	}
}
