package spies;

import org.apache.flink.runtime.clusterframework.types.GeoLocation;
import org.apache.flink.runtime.executiongraph.ExecutionEdge;
import org.apache.flink.runtime.executiongraph.ExecutionGraph;
import org.apache.flink.runtime.executiongraph.ExecutionJobVertex;
import org.apache.flink.runtime.executiongraph.ExecutionVertex;
import org.apache.flink.runtime.jobgraph.JobEdge;
import org.apache.flink.runtime.jobgraph.JobVertex;
import org.apache.flink.runtime.jobgraph.JobVertexID;
import org.apache.flink.runtime.jobmanager.scheduler.BandwidthProvider;
import org.apache.flink.runtime.jobmanager.scheduler.GeoScheduler;
import org.apache.flink.runtime.jobmanager.scheduler.StaticBandwidthProvider;
import org.apache.flink.runtime.jobmaster.LogicalSlot;
import org.apache.flink.types.TwoKeysMultiMap;
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
	 * The bandwidth provider for the cluster, that can tell this spy what the bandwidth between two sites is.
	 * */
	private BandwidthProvider bandwidthProvider;
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

	/*
	 @return the bandwidth provider for the cluster, that can tell this spy what the bandwidth between two sites is
	* **/
	public BandwidthProvider getBandwidthProvider() {
		return bandwidthProvider;
	}

	/**
	 * Sets the bandwidth provider for the cluster, that can tell this spy what the bandwidth between two sites is
	 */
	public void setBandwidthProvider(BandwidthProvider bandwidthProvider) {
		this.bandwidthProvider = bandwidthProvider;
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

		preparePlacedVertices(graph);

		if(bandwidthProvider == null) {
			bandwidthProvider = new StaticBandwidthProvider(new TwoKeysMultiMap<>());
		}

		for (ExecutionVertex consumer : graph.getAllExecutionVertices()) {

			GeoLocation consumerLocation = getLocationFromAssignement(consumer);
			if(consumerLocation == GeoLocation.UNKNOWN) {
				return -1;
			}

			int numberOfInputsToConsumer = consumer.getNumberOfInputs();

			if (numberOfInputsToConsumer == 0) {
				//CASE 1: CONSUMER IS AN INPUT
				networkCost += additionForSource(consumer, consumerLocation);

			} else if(consumer.getProducedPartitions().isEmpty()) {
				//CASE 2: CONSUMER IS AN OUTPUT
				networkCost += additionForSink(consumer, consumerLocation);

			} else {
				//CASE 3: CONSUMER IS NEITHER AN INPUT NOR AN OUTPUT
				for (int inputNumber = 0; inputNumber < numberOfInputsToConsumer; inputNumber++) {
					//for each input to the JobVertex, get all ExecutionEdge
					ExecutionEdge[] inputEdgesToConsumer = consumer.getInputEdges(inputNumber);
					for (ExecutionEdge inputEdgeToConsumer : inputEdgesToConsumer) {
						networkCost += additionForExecutionEdge(consumerLocation, inputNumber, inputEdgesToConsumer.length, inputEdgeToConsumer);
					}
				}
			}
		}


		return networkCost;
	}

	private void preparePlacedVertices(ExecutionGraph graph) {
		for (ExecutionJobVertex ejv : graph.getVerticesTopologically()) {
			if(ejv.getJobVertex().getGeoLocationKey() != null) {
				placedVertices.put(ejv.getJobVertex(), new GeoLocation(ejv.getJobVertex().getGeoLocationKey()));
			}
		}
	}

	private GeoLocation getLocationFromAssignement(ExecutionVertex vertex) {
		LogicalSlot producerAssignment = schedulingDecisions.get(vertex);

		if (producerAssignment == null) {
			log.error("For this method to return the correct value," +
				" all the scheduling decisions must be communicated before running it. " +
				"ExecutionVertex " + vertex + " scheduling decision can't be found");
			return GeoLocation.UNKNOWN;
		}

		return  producerAssignment.getTaskManagerLocation().getGeoLocation();
	}

	/**
	 * "source" reads an input dataset/queue/etc. As Flink does not have a notion of pinned vertices,
	 * if the input vertex is pinned to a position it means that the dataset is there. So if a scheduler schedules an input
	 * subtask away from its dataset (the pinned position) it has to pay the cost of moving (part of) the dataset.
	 */
	private double additionForSource(ExecutionVertex source, GeoLocation sourceLocation) {
		if (placedVertices.get(source.getJobVertex().getJobVertex()) != null && !sourceLocation.equals(placedVertices.get(source.getJobVertex().getJobVertex()))) {
			double bandwidth = bandwidthProvider.getBandwidth(placedVertices.get(source.getJobVertex().getJobVertex()), sourceLocation);
			return (source.getJobVertex().getJobVertex().getSelectivity() / source.getTotalNumberOfParallelSubtasks()) / bandwidth;
		} else {
			return 0;
		}
	}

	/**
	 * "sink" is an output vertex that produces an input dataset/writes to a queue/etc. As Flink does not have a notion of pinned vertices,
	 * if the output vertex is pinned to a position it means that the dataset needs to be produced there. So if a scheduler schedules an input
	 * subtask away from the pinned position it has to pay the cost of moving (part of) the produced dataset
	 */
	private double additionForSink(ExecutionVertex sink, GeoLocation sinkLocation) {
		double networkCost = 0;

		if (placedVertices.get(sink.getJobVertex().getJobVertex()) != null && !sinkLocation.equals(placedVertices.get(sink.getJobVertex().getJobVertex()))) {
			for (JobEdge jobEdge : sink.getJobVertex().getJobVertex().getInputs()) {
				double bandwidth = bandwidthProvider.getBandwidth(sinkLocation, placedVertices.get(sink.getJobVertex().getJobVertex()));
				networkCost += (jobEdge.getWeight() / sink.getTotalNumberOfParallelSubtasks()) / bandwidth;
			}
		}
		return networkCost;
	}

	private double additionForExecutionEdge(GeoLocation consumerLocation, int inputNumber, int numberOfParallelEdges, ExecutionEdge executionEdge) {
		double networkCost = 0;

		//gets this edge producer and consumer
		ExecutionVertex producer = executionEdge.getSource().getProducer();
		ExecutionVertex consumer = executionEdge.getTarget();

		//get where the ExecutionVertex this edge comes from is placed
		GeoLocation producerLocation = getLocationFromAssignement(producer);
		if(producerLocation == GeoLocation.UNKNOWN) {
			//return -1;
		}

		if (!producerLocation.equals(consumerLocation)) {
			//if it is not placed in the same geolocation of this consumer
			//add to the network cost
			//     consumer.getJobVertex.getInputs[id_of_the_ExecutionEdge].getWeight / (how_many_parallel_ExecutionEdge_to_this_consumer * number_of_consumers)
			//assumption verified: the n_th ExecutionEdge[] (index inputNumber) contains edges from the n_th input to the JobVertex
			//{@link ExecutionEdgeIndexingTest}

			double bandwidth = bandwidthProvider.getBandwidth(producerLocation, consumerLocation);
			networkCost += (consumer.getJobVertex().getJobVertex().getInputs().get(inputNumber).getWeight() / (numberOfParallelEdges * consumer.getTotalNumberOfParallelSubtasks())) / bandwidth;
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
