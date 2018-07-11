package org.apache.flink.runtime.jobmanager.scheduler.TestJobGraphs;

import org.apache.flink.runtime.jobgraph.DistributionPattern;
import org.apache.flink.runtime.jobgraph.IntermediateDataSet;
import org.apache.flink.runtime.jobgraph.JobVertex;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

public class SimpleJobGraph {

	public final static int MAP_TASKS = 3;

	public Set<JobVertex> vertices = new HashSet<>();

	public SimpleJobGraph() {

		List<JobVertex> inputs = new ArrayList<>();
		List<JobVertex> maps = new ArrayList<>();
		JobVertex map;
		for(int i = 0; i < MAP_TASKS; i ++) {
			JobVertex in = new JobVertex("input" + i);
			JobVertex m = new JobVertex("map" + i);

			for (IntermediateDataSet ids : in.getProducedDataSets()) {
				m.connectDataSetAsInput(ids, DistributionPattern.POINTWISE);
			}

			inputs.add(in);
			maps.add(m);
		}

		JobVertex reduce = new JobVertex("reduce");

		for(JobVertex m : maps) {
			for(IntermediateDataSet ids : m.getProducedDataSets()) {
				reduce.connectDataSetAsInput(ids, DistributionPattern.POINTWISE);
			}
		}

		vertices.addAll(inputs);
		vertices.addAll(maps);
		vertices.add(reduce);
		setVoidInvokable(vertices);
	}

	public JobVertex[] getVertices() {
		return vertices.toArray(new JobVertex[0]);
	}

	public void setVoidInvokable(Iterable<JobVertex> vertices) {
		for(JobVertex v : vertices) {
			v.setInvokableClass(VoidInvokable.class);
		}
	}

}
