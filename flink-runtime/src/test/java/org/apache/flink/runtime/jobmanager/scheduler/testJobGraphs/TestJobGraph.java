package org.apache.flink.runtime.jobmanager.scheduler.testJobGraphs;

import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.runtime.jobmanager.scheduler.WritableType;

public abstract class TestJobGraph extends WritableType {
	public abstract JobGraph getJobGraph();
}
