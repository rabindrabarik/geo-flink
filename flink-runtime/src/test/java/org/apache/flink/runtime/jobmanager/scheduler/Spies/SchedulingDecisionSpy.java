package org.apache.flink.runtime.jobmanager.scheduler.Spies;

import org.apache.flink.runtime.clusterframework.types.GeoLocation;
import org.apache.flink.runtime.executiongraph.ExecutionEdge;
import org.apache.flink.runtime.executiongraph.ExecutionEdgeIndexingTest;
import org.apache.flink.runtime.executiongraph.ExecutionGraph;
import org.apache.flink.runtime.executiongraph.ExecutionJobVertex;
import org.apache.flink.runtime.executiongraph.ExecutionVertex;
import org.apache.flink.runtime.jobgraph.JobVertex;
import org.apache.flink.runtime.jobgraph.JobVertexID;
import org.apache.flink.runtime.jobmaster.LogicalSlot;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;

/**
 * This object can be passed to a {@link SpyableScheduler}. It will record all the scheduling decisions. The underlying map is synchronised.
 */
public class SchedulingDecisionSpy {

	private final static Logger log = LoggerFactory.getLogger(SchedulingDecisionSpy.class);

	private final Map<ExecutionVertex, LogicalSlot> assignements = Collections.synchronizedMap(new HashMap<>());

	/**
	 * The set of vertices that are known to be in the ExecutionGraph, cached to avoid iterations
	 */
	private final Set<ExecutionVertex> knownVertices = new HashSet<>();

	/**
	 * This spy will only accept assignements about this graph
	 */
	private final ExecutionGraph graph;

	private final Map<JobVertex, GeoLocation> placedVertices;

	/**
	 * @param graph the ExecutionGraph to track the assignments of
	 */
	public SchedulingDecisionSpy(ExecutionGraph graph, Map<JobVertex, GeoLocation> placedVertices) {
		this.graph = graph;
		this.placedVertices = placedVertices == null ? new HashMap<>() : placedVertices;
	}

	public GeoLocation getGeoLocationFor(ExecutionVertex vertex) {
		return assignements.get(vertex).getTaskManagerLocation().getGeoLocation();
	}

	/**
	 * Tell this spy where the slot has been assigned.
	 */
	public void addAssignementFor(ExecutionVertex vertex, LogicalSlot slot) {
		if (!isInGraph(vertex)) {
			throw new IllegalArgumentException("The vertex is not in the graph this SchedulingDecisionSpy was initialised with");
		}

		assignements.put(vertex, slot);
	}

	private boolean isInGraph(ExecutionVertex vertex) {
		if (knownVertices.contains(vertex)) {
			return true;
		}
		boolean isInGraph = false;
		Iterator<ExecutionVertex> graphIterator = graph.getAllExecutionVertices().iterator();
		while (graphIterator.hasNext() && !isInGraph) {
			ExecutionVertex otherVertex = graphIterator.next();
			if (otherVertex.equals(vertex)) {
				isInGraph = true;
				knownVertices.add(vertex);
			}
		}
		return isInGraph;
	}

	/**
	 * Calculate the network cost on current assignements.
	 * For this method to return the correct value, all the scheduling decisions must be communicated before running it.
	 * <p>
	 * The cost is calculated as:
	 * For all incoming edge of each vertex, check where the producer is and add the correct fraction of the edge weight to the network cost
	 * if the producer is not on the same geo location.
	 */
	public double calculateNetworkCost() {
		double networkCost = 0;

		for (ExecutionVertex consumer : graph.getAllExecutionVertices()) {
			LogicalSlot consumerAssignment = assignements.get(consumer);
			if (consumerAssignment == null) {
				log.error("For this method to return the correct value," +
					" all the scheduling decisions must be communicated before running it. " +
					"ExecutionVertex " + consumer + " scheduling decision can't be found");
				return -1;
			}
			GeoLocation consumerLocation = consumerAssignment.getTaskManagerLocation().getGeoLocation();

			int numberOfInputsToConsumer = consumer.getNumberOfInputs();

			if (numberOfInputsToConsumer == 0) {
				/*"consumer" is actually an input vertex that reads an input dataset. As Flink does not have a notion of pinned vertices,
				 * if the input vertex is pinned to a position it means that the dataset is there. So if a scheduler schedules an input
				 * subtask away from its dataset (the pinned position) it has to pay the cost of moving (part of) the dataset */
				if (placedVertices.get(consumer.getJobVertex().getJobVertex()) == null || consumerLocation != placedVertices.get(consumer.getJobVertex().getJobVertex())) {
					networkCost += consumer.getJobVertex().getJobVertex().getSelectivity() / consumer.getTotalNumberOfParallelSubtasks();
				}
			}

			for (int inputNumber = 0; inputNumber < numberOfInputsToConsumer; inputNumber++) {
				//for each input to the JobVertex, get all ExecutionEdge
				ExecutionEdge[] inputEdgesToConsumer = consumer.getInputEdges(inputNumber);
				for (ExecutionEdge inputEdgeToConsumer : inputEdgesToConsumer) {
					//for each parallel ExecutionEdge of that input get where the ExecutionVertex they come from is placed
					ExecutionVertex producer = inputEdgeToConsumer.getSource().getProducer();
					LogicalSlot producerAssignment = assignements.get(producer);
					if (producerAssignment == null) {
						log.error("For this method to return the correct value," +
							" all the scheduling decisions must be communicated before running it. " +
							"ExecutionVertex " + producer + " scheduling decision can't be found");
						return -1;
					}
					GeoLocation producerLocation = producerAssignment.getTaskManagerLocation().getGeoLocation();

					if (!producerLocation.equals(consumerLocation)) {
						//if it is not placed in the same geolocation of this consumer
						//add to the network cost
						//     consumer.getJobVertex.getInputs[id_of_the_ExecutionEdge].getWeight / (how_many_parallel_ExecutionEdge_to_this_consumer * number_of_consumers)
						//assumption verified: the n_th ExecutionEdge[] (index inputNumber) contains edges from the n_th input to the JobVertex
						/**{@link ExecutionEdgeIndexingTest}*/

						networkCost += consumer.getJobVertex().getJobVertex().getInputs().get(inputNumber).getWeight() / (inputEdgesToConsumer.length * consumer.getTotalNumberOfParallelSubtasks());
					}
				}
			}
		}


		return networkCost;
	}

	/**
	 * Calculate the execution speed on current assignements.
	 * For this method to return the correct value, all the scheduling decisions must be communicated before running it.
	 * <p>
	 * The time is calculated as:
	 * For all the JobVertex, sum how much they are parallelised and multiply by their weight. (The higher the result the faster the application).
	 */
	public double calculateExecutionSpeed() {
		double executionSpeed = 0;

		for (JobVertexID jobVertexID : graph.getAllVertices().keySet()) {
			ExecutionJobVertex executionJobVertex = graph.getJobVertex(jobVertexID);
			executionSpeed += executionJobVertex.getJobVertex().getParallelism() * executionJobVertex.getJobVertex().getWeight();
		}

		return executionSpeed;
	}


	public String getAssignementsString() {
		String out = "";

		for (Map.Entry<ExecutionVertex, LogicalSlot> entry : assignements.entrySet()) {
			out += entry.toString() + "\n";
		}

		return out;
	}
}
