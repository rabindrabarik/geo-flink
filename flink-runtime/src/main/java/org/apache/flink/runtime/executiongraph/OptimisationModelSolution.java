package org.apache.flink.runtime.executiongraph;

import gurobi.GRB;
import gurobi.GRBException;
import gurobi.GRBModel;
import gurobi.GRBVar;
import org.apache.flink.runtime.clusterframework.types.GeoLocation;
import org.apache.flink.runtime.jobgraph.JobVertex;
import org.apache.flink.runtime.util.GRBUtils;
import org.apache.flink.types.TwoKeysMap;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class OptimisationModelSolution {
	private Map<JobVertex, List<GeoLocation>> placement;
	private Map<JobVertex, Integer> parallelism;
	private double networkCost;
	private double executionSpeed;
	private double modelExecutionTime;

	public OptimisationModelSolution(Map<JobVertex, List<GeoLocation>> placement, Map<JobVertex, Integer> parallelism, double networkCost, double executionSpeed, double modelExecutionTime) {
		this.placement = placement;
		this.networkCost = networkCost;
		this.executionSpeed = executionSpeed;
		this.parallelism = parallelism;
		this.modelExecutionTime = modelExecutionTime;
	}

	public static OptimisationModelSolution fromSolvedModel(GRBModel solvedModel, TwoKeysMap<JobVertex, GeoLocation, GRBVar> placementVarMap, Map<JobVertex, GRBVar> parallelismVarMap, GRBVar executionTime, GRBVar networkCost) throws GRBException {
		if (!GRBUtils.isSolved(solvedModel)) {
			return null;
		}

		Map<JobVertex, List<GeoLocation>> placement = makePlacementMap(placementVarMap);
		Map<JobVertex, Integer> parallelism = makeParallelismMap(parallelismVarMap);

		return new OptimisationModelSolution(placement, parallelism, networkCost.get(GRB.DoubleAttr.X), executionTime.get(GRB.DoubleAttr.X), solvedModel.get(GRB.DoubleAttr.Runtime));
	}

	private static Map<JobVertex, List<GeoLocation>> makePlacementMap(TwoKeysMap<JobVertex, GeoLocation, GRBVar> placementVarMap) throws GRBException {
		Map<JobVertex, List<GeoLocation>> placement = new HashMap<>();
		for (TwoKeysMap.Entry<JobVertex, GeoLocation, GRBVar> placementVarEntry : placementVarMap.entrySet()) {
			if (placementVarEntry.getValue().get(GRB.DoubleAttr.X) == 1d) {
				placement.putIfAbsent(placementVarEntry.getKey1(), new ArrayList<>());
				placement.get(placementVarEntry.getKey1()).add(placementVarEntry.getKey2());
			}
		}
		return placement;
	}

	private static Map<JobVertex, Integer> makeParallelismMap(Map<JobVertex, GRBVar> parallelismVarMap) throws GRBException {
		Map<JobVertex, Integer> parallelism = new HashMap<>();
		for (Map.Entry<JobVertex, GRBVar> parallelismVarEntry : parallelismVarMap.entrySet()) {
			parallelism.put(parallelismVarEntry.getKey(), (int) Math.round(parallelismVarEntry.getValue().get(GRB.DoubleAttr.X)));
		}
		return parallelism;
	}

	public Map<JobVertex, List<GeoLocation>> getPlacementMap() {
		return placement;
	}

	public Map<JobVertex, Integer> getParallelismMap() {
		return parallelism;
	}

	public List<GeoLocation> getPlacement(JobVertex vertex) {return placement.get(vertex);}

	public Integer getParallelism(JobVertex vertex) {return parallelism.get(vertex);}

	public double getNetworkCost() {
		return networkCost;
	}

	public double getExecutionSpeed() {
		return executionSpeed;
	}

	public double getModelExecutionTime() {
		return modelExecutionTime;
	}

	@Override
	public String toString() {
		StringBuilder out = new StringBuilder("\n");

		out.append("\n\n PLACEMENT:");
		out.append(GRBUtils.mapToString(placement));

		out.append("\n\n PARALLELISM:");
		out.append(GRBUtils.mapToString(parallelism));

		out.append("\n\n Model execution time: ").append(modelExecutionTime);
		out.append("\n\n Streaming app network cost: ").append(networkCost);
		out.append("\n\n Streaming app execution speed: ").append(executionSpeed).append("\n\n");

		return out.toString();
	}
}
