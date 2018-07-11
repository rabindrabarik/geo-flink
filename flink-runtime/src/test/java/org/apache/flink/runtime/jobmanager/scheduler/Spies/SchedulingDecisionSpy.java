package org.apache.flink.runtime.jobmanager.scheduler.Spies;

import com.sun.xml.internal.bind.v2.TODO;
import org.apache.flink.runtime.clusterframework.types.GeoLocation;
import org.apache.flink.runtime.executiongraph.ExecutionEdge;
import org.apache.flink.runtime.executiongraph.ExecutionVertex;
import org.apache.flink.runtime.jobmaster.LogicalSlot;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

/**
 * This object can be passed to a {@link SpyableScheduler}. It will record all the scheduling decisions. The underlying map is synchronised.
 */
public class SchedulingDecisionSpy {
	private final Map<ExecutionVertex, LogicalSlot> assignements = Collections.synchronizedMap(new HashMap<>());

	public GeoLocation getGeoLocationFor(ExecutionVertex vertex) {
		return assignements.get(vertex).getTaskManagerLocation().getGeoLocation();
	}

	/**
	 * Tell this spy where the slot has been assigned.
	 */
	public LogicalSlot addAssignementFor(ExecutionVertex vertex, LogicalSlot slot) {
		return assignements.put(vertex, slot);
	}

	/**
	 * Calculate the network cost on current assignements. Attention: this method will lock the assignements map.
	 * */
	public double calculateNetworkCost() {
		double networkCost = 0;
		synchronized (assignements) {

			for (Map.Entry<ExecutionVertex, LogicalSlot> entry : assignements.entrySet()) {
				;
			/*TODO:
				* For all incoming edges, check where the producer is and add the correct fraction of the edge weight to the network cost
				  if the producer is not on the same geo location.
			 */
				ExecutionVertex executionVertex = entry.getKey();
				GeoLocation executionVertexLocation = entry.getValue().getTaskManagerLocation().getGeoLocation();
				int numberOfInputs = executionVertex.getNumberOfInputs();
				for (int inputNumber = 0; inputNumber < numberOfInputs; inputNumber++) {
					ExecutionEdge[] inputEdges = executionVertex.getInputEdges(inputNumber);
					for (ExecutionEdge executionEdge : inputEdges) {
						if(! assignements.get(executionEdge.getSource().getProducer()).getTaskManagerLocation().getGeoLocation().equals(executionVertexLocation)) {
							//TODO: divide by number of parallel edges
							networkCost += executionVertex.getJobVertex().getJobVertex().getInputs().get(executionEdge.getInputNum()).getWeight();
						}
					}
				}
			}
		}

		return networkCost;
	}
}
