import testingFrameworks.JobGraphSchedulingTestFramework;
import writableTypes.CentralAndEdgeInstances;
import writableTypes.DistributedInstances;
import writableTypes.SimpleJobGraph;
import writableTypes.TestInstanceSet;
import writableTypes.TestJobGraph;

public class SimpleJobGraphSchedulingTest extends JobGraphSchedulingTestFramework {

	private final SimpleJobGraph simpleJobGraph = new SimpleJobGraph(4, 151);
	private final CentralAndEdgeInstances instanceSet = new CentralAndEdgeInstances(150, 4, 4);

	@Override
	protected TestJobGraph jobGraph() {
		return simpleJobGraph;
	}

	@Override
	protected TestInstanceSet instanceSet() {
		return instanceSet;
	}
}
