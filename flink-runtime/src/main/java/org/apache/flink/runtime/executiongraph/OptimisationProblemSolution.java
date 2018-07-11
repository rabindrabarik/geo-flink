package org.apache.flink.runtime.executiongraph;

import gurobi.GRB;
import gurobi.GRBException;
import gurobi.GRBModel;
import gurobi.GRBVar;
import org.apache.flink.runtime.clusterframework.types.GeoLocation;
import org.apache.flink.runtime.jobgraph.JobVertex;
import org.apache.flink.runtime.util.GRBUtils;
import org.apache.flink.types.TwoKeysMap;

import java.util.HashMap;
import java.util.Map;

public class OptimisationProblemSolution {
	private Map<JobVertex, GeoLocation> placement;
	private Map<JobVertex, Integer> parallelism;
	private double networkCost;
	private double executionTime;

	public OptimisationProblemSolution(Map<JobVertex, GeoLocation> placement, Map<JobVertex, Integer> parallelism, double networkCost, double executionTime) {
		this.placement = placement;
		this.networkCost = networkCost;
		this.executionTime = executionTime;
		this.parallelism = parallelism;
	}

	public static OptimisationProblemSolution fromSolvedModel(GRBModel solvedModel, TwoKeysMap<JobVertex, GeoLocation, GRBVar> placementVarMap, Map<JobVertex, GRBVar> parallelismVarMap, GRBVar executionTime, GRBVar networkCost) throws GRBException {
		if (solvedModel.get(GRB.IntAttr.Status) != GRB.Status.OPTIMAL && solvedModel.get(GRB.IntAttr.Status) != GRB.Status.SUBOPTIMAL) {
			throw new IllegalArgumentException("Solve the model first");
		}

		Map<JobVertex, GeoLocation> placement = makePlacementMap(placementVarMap);
		Map<JobVertex, Integer> parallelism = makeParallelismMap(parallelismVarMap);

		return new OptimisationProblemSolution(placement, parallelism, networkCost.get(GRB.DoubleAttr.X), executionTime.get(GRB.DoubleAttr.X));
	}

	private static Map<JobVertex, GeoLocation> makePlacementMap(TwoKeysMap<JobVertex, GeoLocation, GRBVar> placementVarMap) throws GRBException {
		Map<JobVertex, GeoLocation> placement = new HashMap<>();
		for (TwoKeysMap.Entry<JobVertex, GeoLocation, GRBVar> placementVarEntry : placementVarMap.entrySet()) {
			if (placementVarEntry.getValue().get(GRB.DoubleAttr.X) == 1d) {
				placement.put(placementVarEntry.getKey1(), placementVarEntry.getKey2());
			}
		}
		return placement;
	}

	private static Map<JobVertex, Integer> makeParallelismMap(Map<JobVertex, GRBVar> parallelismVarMap) throws GRBException {
		Map<JobVertex, Integer> parallelism = new HashMap<>();
		for (Map.Entry<JobVertex, GRBVar> parallelismVarEntry : parallelismVarMap.entrySet()) {
			parallelism.put(parallelismVarEntry.getKey(), (int) parallelismVarEntry.getValue().get(GRB.DoubleAttr.X));
		}
		return parallelism;
	}

	public Map<JobVertex, GeoLocation> getPlacementMap() {
		return placement;
	}

	public Map<JobVertex, Integer> getParallelismMap() {
		return parallelism;
	}

	public GeoLocation getPlacement(JobVertex vertex) {return placement.get(vertex);}

	public Integer getParallelism(JobVertex vertex) {return parallelism.get(vertex);}

	public double getNetworkCost() {
		return networkCost;
	}

	public double getExecutionTime() {
		return executionTime;
	}

	@Override
	public String toString() {
		StringBuilder out = new StringBuilder("\n");

		out.append("\n\n PLACEMENT:");
		out.append(GRBUtils.mapToString(placement));

		out.append("\n\n PARALLELISM:");
		out.append(GRBUtils.mapToString(parallelism));

		return out.toString();
	}
}
