package org.apache.flink.runtime.executiongraph;

import org.apache.flink.runtime.clusterframework.types.GeoLocation;
import org.apache.flink.runtime.jobgraph.JobVertex;

import java.util.Map;
import java.util.Set;

public class OptimisationProblem {
	private Set<JobVertex> vertices;
	private Set<GeoLocation> locations;
	private Map<GeoLocation, Map<GeoLocation, Double>> bandwidths;
	private Map<GeoLocation, Integer> slots;

}
