package org.apache.flink.runtime.jobmanager.scheduler;

import org.apache.flink.runtime.jobmanager.scheduler.instanceSets.DistributedInstances;
import org.apache.flink.runtime.jobmanager.scheduler.instanceSets.InstanceSet;
import org.apache.flink.runtime.jobmanager.scheduler.schedulingDecisionFramework.SchedulingDecisionTestFramework;
import org.apache.flink.runtime.jobmanager.scheduler.testJobGraphs.SimpleJobGraph;
import org.apache.flink.runtime.jobmanager.scheduler.testJobGraphs.TestJobGraph;

public class SimpleJobGraphSchedulingDecisionTest extends SchedulingDecisionTestFramework {

	private final SimpleJobGraph simpleJobGraph = new SimpleJobGraph(4);
	private final DistributedInstances instanceSet = new DistributedInstances(4);

	@Override
	protected TestJobGraph jobGraph() {
		return simpleJobGraph;
	}

	@Override
	protected InstanceSet instanceSet() {
		return instanceSet;
	}
}
