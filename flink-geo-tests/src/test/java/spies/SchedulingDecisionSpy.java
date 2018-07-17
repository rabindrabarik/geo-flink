package spies;

import org.apache.flink.runtime.clusterframework.types.GeoLocation;
import org.apache.flink.runtime.executiongraph.ExecutionEdge;
import org.apache.flink.runtime.executiongraph.ExecutionGraph;
import org.apache.flink.runtime.executiongraph.ExecutionJobVertex;
import org.apache.flink.runtime.executiongraph.ExecutionVertex;
import org.apache.flink.runtime.jobgraph.JobVertex;
import org.apache.flink.runtime.jobgraph.JobVertexID;
import org.apache.flink.runtime.jobmanager.scheduler.GeoScheduler;
import org.apache.flink.runtime.jobmaster.LogicalSlot;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

/**
 * This object can be passed to a {@link SpyableScheduler}. It will record all the scheduling decisions. The underlying map is synchronised.
 */
public class SchedulingDecisionSpy {

	private final static Logger log = LoggerFactory.getLogger(SchedulingDecisionSpy.class);

	/**
	 * A map storing where each execution vertex was scheduled.
	 */
	private final Map<ExecutionVertex, LogicalSlot> schedulingDecisions = Collections.synchronizedMap(new HashMap<>());

	/**
	 * The vertices that are considered as already placed by the {@link GeoScheduler}.
	 */
	private final Map<JobVertex, GeoLocation> placedVertices = new HashMap<>();

	/**
	 * The time it took to solve the model associated with the execution graph (in seconds).
	 */
	private Map<ExecutionGraph, Double> modelSolveTimes = new HashMap<>();

	/**
	 * All the {@link ExecutionGraph} this spy knows scheduling decisions about.
	 * */
	private Set<ExecutionGraph> executionGraphs = new HashSet<>();

	/**
	 * Adds vertices that are considered as already placed by the {@link GeoScheduler}.
	 * */
	public void addPlacedVertices(Map<JobVertex, GeoLocation> placedVertices) {
		this.placedVertices.putAll(placedVertices);
	}

	/**
	 * Returns where the execution vertex was scheduled.
	 */
	public GeoLocation getSchedulingDecisionFor(ExecutionVertex vertex) {
		return schedulingDecisions.get(vertex).getTaskManagerLocation().getGeoLocation();
	}

	/**
	 * Tell this spy where the task has been scheduled.
	 */
	public void setSchedulingDecisionFor(ExecutionVertex vertex, LogicalSlot slot) {
		executionGraphs.add(vertex.getExecutionGraph());
		schedulingDecisions.put(vertex, slot);
	}

	/**
	 * Tell this spy how long it took to solve the executionGraph model (in seconds).
	 */
	public void setModelSolveTime(ExecutionGraph executionGraph, double modelSolveTime) {
		modelSolveTimes.put(executionGraph, modelSolveTime);
	}

	/**
	 * @return The time it took to solve the model associated with the execution graph (in seconds).
	 * */
	public double getModelSolveTime(ExecutionGraph executionGraph) {
		if(modelSolveTimes.containsKey(executionGraph)) {
			return modelSolveTimes.get(executionGraph);
		} else {
			return 0;
		}
	}

	/**
	 * @return all the {@link ExecutionGraph} this spy knows scheduling decisions about.
	 */
	public Set<ExecutionGraph> getGraphs() {
		return executionGraphs;
	}

	/**
	 * Calculate the network cost on current schedulingDecisions.
	 * For this method to return the correct value, all the scheduling decisions must be communicated before running it.
	 * <p>
	 * The cost is calculated as:
	 * For all incoming edge of each vertex, check where the producer is and add the correct fraction of the edge weight to the network cost
	 * if the producer is not on the same geo location.
	 */
	public double calculateNetworkCost(ExecutionGraph graph) {
		double networkCost = 0;

		for (ExecutionVertex consumer : graph.getAllExecutionVertices()) {
			LogicalSlot consumerAssignment = schedulingDecisions.get(consumer);
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
					LogicalSlot producerAssignment = schedulingDecisions.get(producer);
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
	 * Calculate the execution speed on current schedulingDecisions.
	 * For this method to return the correct value, all the scheduling decisions must be communicated before running it.
	 * <p>
	 * The time is calculated as:
	 * For all the JobVertex, sum how much they are parallelised and multiply by their weight. (The higher the result the faster the application).
	 */
	public double calculateExecutionSpeed(ExecutionGraph graph) {
		double executionSpeed = 0;

		for (JobVertexID jobVertexID : graph.getAllVertices().keySet()) {
			ExecutionJobVertex executionJobVertex = graph.getJobVertex(jobVertexID);
			executionSpeed += executionJobVertex.getJobVertex().getParallelism() * executionJobVertex.getJobVertex().getWeight();
		}

		return executionSpeed;
	}


	public String getAssignementsString() {
		String out = "";

		for (Map.Entry<ExecutionVertex, LogicalSlot> entry : schedulingDecisions.entrySet()) {
			out += entry.toString() + "\n";
		}

		return out;
	}
}
