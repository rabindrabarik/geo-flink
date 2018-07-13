package org.apache.flink.runtime.jobmanager.scheduler;

import org.apache.flink.runtime.clusterframework.types.GeoLocation;
import org.apache.flink.runtime.instance.AckingDummyActorGateway;
import org.apache.flink.runtime.instance.Instance;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.runtime.jobmanager.scheduler.TestJobGraphs.SimpleJobGraph;

import java.util.HashSet;
import java.util.Set;

public class SimpleJobGraphSchedulingDecisionTest extends SchedulingDecisionTestFramework {

	private final SimpleJobGraph simpleJobGraph = new SimpleJobGraph(4);
	private final Set<Instance> instanceSet = new HashSet<>();


	public SimpleJobGraphSchedulingDecisionTest() {
		for(int i = 0; i < 4; i ++) {
			instanceSet.add(SchedulerTestUtils.getRandomInstance(4, AckingDummyActorGateway.INSTANCE, new GeoLocation("location_" + i)));
		}
	}

	@Override
	protected JobGraph jobGraph() {
		return simpleJobGraph.getJobGraph();
	}

	@Override
	protected Set<Instance> instanceSet() {
		return instanceSet;
	}
}
