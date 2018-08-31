import testingFrameworks.JobGraphSchedulingTestFramework;
import writableTypes.CentralAndEdgeInstances;
import writableTypes.DistributedInstances;
import writableTypes.SimpleJobGraph;
import writableTypes.TestInstanceSet;
import writableTypes.TestJobGraph;

public class SimpleJobGraphSchedulingTest extends JobGraphSchedulingTestFramework {

	private final SimpleJobGraph simpleJobGraph = new SimpleJobGraph(198, (150*584+192)/198);
	private final CentralAndEdgeInstances instanceSet = new CentralAndEdgeInstances(150, 192, 584);

	@Override
	protected TestJobGraph jobGraph() {
		return simpleJobGraph;
	}

	@Override
	protected TestInstanceSet instanceSet() {
		return instanceSet;
	}
}
