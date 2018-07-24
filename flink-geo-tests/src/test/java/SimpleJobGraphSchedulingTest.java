import testingFrameworks.JobGraphSchedulingTestFramework;
import writableTypes.DistributedInstances;
import writableTypes.SimpleJobGraph;
import writableTypes.TestInstanceSet;
import writableTypes.TestJobGraph;

public class SimpleJobGraphSchedulingTest extends JobGraphSchedulingTestFramework {

	private final SimpleJobGraph simpleJobGraph = new SimpleJobGraph(10);
	private final DistributedInstances instanceSet = new DistributedInstances(10);

	@Override
	protected TestJobGraph jobGraph() {
		return simpleJobGraph;
	}

	@Override
	protected TestInstanceSet instanceSet() {
		return instanceSet;
	}
}
