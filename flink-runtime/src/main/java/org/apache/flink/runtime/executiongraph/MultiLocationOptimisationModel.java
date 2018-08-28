package org.apache.flink.runtime.executiongraph;

import gurobi.GRB;
import gurobi.GRBException;
import gurobi.GRBLinExpr;
import gurobi.GRBQuadExpr;
import gurobi.GRBVar;
import org.apache.flink.runtime.clusterframework.types.GeoLocation;
import org.apache.flink.runtime.jobgraph.JobEdge;
import org.apache.flink.runtime.jobgraph.JobVertex;
import org.apache.flink.runtime.jobmanager.scheduler.BandwidthProvider;
import org.apache.flink.types.TwoKeysMap;
import org.apache.flink.types.TwoKeysMultiMap;

import java.util.Collection;
import java.util.Map;
import java.util.Set;

/**
 * This model is able to spread a task across multiple locations, at the cost of a less exact network cost measurement
 */
public class MultiLocationOptimisationModel extends OptimisationModel {
	/**
	 * t[o][x] represents the number of subtasks of o placed at site x.
	 */
	private TwoKeysMap<JobVertex, GeoLocation, GRBVar> t;

	public MultiLocationOptimisationModel(Collection<JobVertex> vertices, Set<GeoLocation> locations, BandwidthProvider bandwidthProvider, Map<GeoLocation, Integer> slots, OptimisationModelParameters parameters) throws GRBException {
		super(vertices, locations, bandwidthProvider, slots, parameters);
	}

	public void init() throws GRBException {
		t = new TwoKeysMultiMap<>();
		addTMatrixVariables();
	}

	private void addTMatrixVariables() throws GRBException {
		for (JobVertex vertex : vertices) {
			GRBLinExpr rhsSplittingConstraint = new GRBLinExpr();
			for (GeoLocation location : locations) {
				String name = getVariableString("t_", vertex, location);
				t.put(vertex, location, model.addVar(0d, getMaxParallelism(vertex), 0.0, GRB.INTEGER, name));

				GRBLinExpr rhsLessThanMaxParallelism = new GRBLinExpr();
				rhsLessThanMaxParallelism.addTerm(getMaxParallelism(vertex), placement.get(vertex, location));
				model.addConstr(t.get(vertex, location), GRB.LESS_EQUAL, rhsLessThanMaxParallelism, name);

				model.addConstr(t.get(vertex, location), GRB.LESS_EQUAL, slots.get(location), name);

				model.addConstr(t.get(vertex, location), GRB.GREATER_EQUAL, placement.get(vertex, location), name);


				rhsSplittingConstraint.addTerm(1d, t.get(vertex, location));
			}

			model.addConstr(parallelism.get(vertex), GRB.EQUAL, rhsSplittingConstraint, getVariableString("splitting_", vertex));
		}
	}

	@Override
	protected GRBQuadExpr makeNetworkCostExpression() throws GRBException {
		GRBQuadExpr expr = new GRBQuadExpr();
		for (JobVertex vertexTo : vertices) {
			for (JobEdge edge : vertexTo.getInputs()) {
				JobVertex vertexFrom = edge.getSource().getProducer();

				GRBVar countTaskFromWithoutTaskTo = addCountTask1WithoutTask2(vertexFrom, vertexTo);
				expr.addTerm(edge.getWeight(), countTaskFromWithoutTaskTo);

				GRBVar countTaskToWithoutTaskFrom = addCountTask1WithoutTask2(vertexTo, vertexFrom);
				expr.addTerm(edge.getWeight(), countTaskToWithoutTaskFrom);
			}
		}
		return expr;
	}

	private GRBVar addCountTask1WithoutTask2(JobVertex vertex1, JobVertex vertex2) throws GRBException {
		String name = getVariableString("count_", vertex1, "without", vertex2);
		GRBVar countTask1WithoutTask2 = model.addVar(0d, GRB.INFINITY, 0.0, GRB.CONTINUOUS, name);
		GRBQuadExpr rhsCountTask1WithoutTask2 = new GRBQuadExpr();

		for (GeoLocation location : locations) {
			GRBVar oneMinusTask2Site = addOneMinusPlacement(vertex2, location);
			rhsCountTask1WithoutTask2.addTerm(1d, placement.get(vertex1, location), oneMinusTask2Site);

		}
		model.addQConstr(countTask1WithoutTask2, GRB.EQUAL, rhsCountTask1WithoutTask2, name);

		return countTask1WithoutTask2;
	}

	private GRBVar addOneMinusPlacement(JobVertex vertex, GeoLocation location) throws GRBException {
		String name = getVariableString("one_minus_placement_", vertex, location);
		GRBVar oneMinusPlacementTaskFromSiteTo = model.addVar(0d, 1d, 0.0, GRB.BINARY, name);

		GRBLinExpr rhsOneMinusPlacementTaskFromSiteTo = new GRBLinExpr();
		rhsOneMinusPlacementTaskFromSiteTo.addConstant(1d);
		rhsOneMinusPlacementTaskFromSiteTo.addTerm(-1d, placement.get(vertex, location));

		model.addConstr(oneMinusPlacementTaskFromSiteTo, GRB.EQUAL, rhsOneMinusPlacementTaskFromSiteTo, name);

		return oneMinusPlacementTaskFromSiteTo;
	}


	@Override
	protected GRBLinExpr makeExecutionSpeedExpression() {
		GRBLinExpr expr = new GRBLinExpr();
		for (JobVertex vertex : vertices) {
			expr.addTerm(-vertex.getWeight(), parallelism.get(vertex));
		}
		return expr;
	}
}
