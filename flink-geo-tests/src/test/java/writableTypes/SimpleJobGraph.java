package writableTypes;

import invokables.VoidInvokable;
import org.apache.flink.runtime.io.network.partition.ResultPartitionType;
import org.apache.flink.runtime.jobgraph.DistributionPattern;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.runtime.jobgraph.JobVertex;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * A Job Graph that resembles 3 inputs, each followed by a map tasks, all aggregated by a reduce task.
 */
public class SimpleJobGraph extends TestJobGraph {

	private Set<JobVertex> vertices = new HashSet<>();

	private List<JobVertex> inputs;
	private List<JobVertex> maps;

	private JobVertex reduce;


	private JobGraph jobGraph;

	public SimpleJobGraph(int mapTasks) {
		this(mapTasks, 4);
	}

	public SimpleJobGraph(int mapTasks, int maxParallelism) {

		params = new Object[2];
		params[0] = mapTasks;
		params[1] = maxParallelism;

		inputs = new ArrayList<>();
		maps = new ArrayList<>();

		for (int i = 0; i < mapTasks; i++) {
			JobVertex in = new JobVertex("input" + i);
			JobVertex m = new JobVertex("map" + i);

			m.connectNewDataSetAsInput(in, DistributionPattern.POINTWISE, ResultPartitionType.PIPELINED);

			inputs.add(in);
			maps.add(m);
		}

		reduce = new JobVertex("reduce");

		for (JobVertex m : maps) {
			reduce.connectNewDataSetAsInput(m, DistributionPattern.ALL_TO_ALL, ResultPartitionType.PIPELINED);
		}

		vertices.addAll(inputs);
		vertices.addAll(maps);
		vertices.add(reduce);

		for (JobVertex vertex : vertices) {
			if (vertex.getParallelism() < 1) {
				vertex.setParallelism(1);
			}
			if (vertex.getMaxParallelism() < 1) {
				vertex.setMaxParallelism(maxParallelism);
			}

			vertex.setInvokableClass(VoidInvokable.class);
		}

		for (JobVertex map : maps) {
			map.setSelectivity(0.5);
		}

		jobGraph = new JobGraph(getVertices());
	}


	public JobVertex[] getVertices() {
		return vertices.toArray(new JobVertex[0]);
	}

	public JobGraph getJobGraph() {
		return jobGraph;
	}

	public List<JobVertex> getInputs() {
		return inputs;
	}

	public List<JobVertex> getMaps() {
		return maps;
	}

	public JobVertex getReduce() {
		return reduce;
	}
}
