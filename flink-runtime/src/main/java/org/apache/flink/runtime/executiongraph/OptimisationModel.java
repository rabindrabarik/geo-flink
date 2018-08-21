package org.apache.flink.runtime.executiongraph;

import gurobi.GRB;
import gurobi.GRBEnv;
import gurobi.GRBException;
import gurobi.GRBLinExpr;
import gurobi.GRBModel;
import gurobi.GRBQuadExpr;
import gurobi.GRBVar;
import org.apache.flink.runtime.clusterframework.types.GeoLocation;
import org.apache.flink.runtime.jobgraph.JobEdge;
import org.apache.flink.runtime.jobgraph.JobVertex;
import org.apache.flink.runtime.jobmanager.scheduler.BandwidthProvider;
import org.apache.flink.runtime.jobmanager.scheduler.StaticBandwidthProvider;
import org.apache.flink.runtime.util.GRBUtils;
import org.apache.flink.types.TwoKeysMap;
import org.apache.flink.types.TwoKeysMultiMap;
import org.apache.flink.util.Preconditions;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

public abstract class OptimisationModel {
	// Constants //
	protected final Iterable<JobVertex> vertices;
	protected final Set<GeoLocation> locations;
	protected final BandwidthProvider bandwidthProvider;
	protected final Map<GeoLocation, Integer> slots;
	protected final Map<JobVertex, GeoLocation> placedVertices;


	// Variables //
	protected final Map<JobVertex, GRBVar> parallelism = new HashMap<>();
	protected final TwoKeysMap<JobVertex, GeoLocation, GRBVar> placement = new TwoKeysMultiMap<>();
	protected GRBEnv grbEnv;
	protected GRBModel model;


	// Objective //
	protected GRBVar networkCost;
	protected GRBVar executionSpeed;

	// Weights //
	protected OptimisationModelParameters parameters;

	public OptimisationModel(Collection<JobVertex> vertices,
							 Set<GeoLocation> locations,
							 Map<JobVertex, GeoLocation> placedVertices,
							 BandwidthProvider bandwidthProvider,
							 Map<GeoLocation, Integer> slots,
							 OptimisationModelParameters parameters) throws GRBException {

		this.grbEnv = new GRBEnv();
		this.model = new GRBModel(grbEnv);

		this.vertices = Preconditions.checkNotNull(vertices);
		this.locations = Preconditions.checkNotNull(locations);

		if (bandwidthProvider != null) {
			this.bandwidthProvider = bandwidthProvider;
		} else {
			this.bandwidthProvider = new StaticBandwidthProvider(new TwoKeysMultiMap<>());
		}

		this.placedVertices = Preconditions.checkNotNull(placedVertices);

		this.slots = slots;

		this.parameters = parameters;

		//double the time spent on heuristics
		model.set(GRB.DoubleParam.Heuristics, 0.1);

		model.set(GRB.DoubleParam.TimeLimit, this.parameters.getTimeForEachTaskBeforeHeuristicSolution() * vertices.size());

		init();
	}

	public abstract void init() throws GRBException;

	public OptimisationModelSolution optimize() throws GRBException {
		model.optimize();
		return OptimisationModelSolution.fromSolvedModel(model, placement, parallelism, executionSpeed, networkCost);
	}

	public String solutionString() {
		try {
			if (!isSolved()) {
				return "Solve the model first";
			}
		} catch (GRBException e) {
			e.printStackTrace();
		}

		String out = "";
		try {
			out += "\n------ SPLITTING ARRAY ------\n";
			out += GRBUtils.mapToString(model, parallelism);

			out += "\n\n------ PLACEMENT MATRIX ------";
			out += "\nproblem[tasks][sites] =\n\n";
			out += GRBUtils.twoKeysMapToString(model, placement);

			out += "\n------ EXECUTION SPEED ------ \n";
			out += executionSpeed.get(GRB.DoubleAttr.X);

			out += "\n\n------ NETWORK COST ------ \n";
			out += networkCost.get(GRB.DoubleAttr.X);

			out += "\n\n------ OBJECTIVE ------ \n";
			out += executionSpeed.get(GRB.DoubleAttr.X) + " * " + parameters.getExecutionSpeedWeight() + " + " + networkCost.get(GRB.DoubleAttr.X) + " * " + parameters.getNetworkCostWeight() + " = " + model.get(GRB.DoubleAttr.ObjVal);

			out += "\n\n------ MODEL RUNTIME ------ \n";
			out += model.get(GRB.DoubleAttr.Runtime) + " s";

		} catch (GRBException e) {
			e.printStackTrace();
		}

		return out;
	}

	public boolean isSolved() throws GRBException {
		return GRBUtils.isSolved(this.model);
	}

	/**
	 * max parallelism is -1f for unbounded values. changing to GRB.Infinity to solve the model
	 */
	protected double getMaxParallelism(JobVertex vertex) {
		double maxParallelism = vertex.getMaxParallelism();
		if(maxParallelism <= 0) {
			maxParallelism = vertex.getParallelism();
		}
		return maxParallelism;
	}

	protected String getVariableString(String variableName, Object... params) {
		StringBuilder out = new StringBuilder(variableName);
		for (Object param : params) {
			out.append("_");
			if (param.toString().length() >= 100) {
				out.append(param.toString(), 0, 100);
			} else {
				out.append(param.toString());
			}

		}
		return out.toString();
	}
}
