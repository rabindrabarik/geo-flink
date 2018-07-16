package writableTypes;

import org.apache.flink.runtime.jobgraph.JobGraph;

public abstract class TestJobGraph extends WritableType {
	public abstract JobGraph getJobGraph();
}
