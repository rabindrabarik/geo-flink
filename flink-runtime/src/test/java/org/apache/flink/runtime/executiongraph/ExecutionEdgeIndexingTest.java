package org.apache.flink.runtime.executiongraph;

import org.apache.flink.runtime.JobException;
import org.apache.flink.runtime.client.JobExecutionException;
import org.apache.flink.runtime.io.network.partition.ResultPartitionType;
import org.apache.flink.runtime.jobgraph.DistributionPattern;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.runtime.jobgraph.JobVertex;
import org.apache.flink.runtime.testutils.invokables.VoidInvokable;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.flink.runtime.jobmanager.scheduler.SchedulerTestUtils.makeExecutionGraph;


/**
 * This test tests the assumption that the n_th ExecutionEdge[] (index inputNumber) contains edges from the n_th input to the JobVertex.
 * It is necessary for SchedulingDecisionSpy::calculateNetworkCost
 */
public class ExecutionEdgeIndexingTest {
	JobVertex[] vertices;
	ExecutionGraph executionGraph;

	private final static Logger log = LoggerFactory.getLogger(ExecutionEdgeIndexingTest.class);

	@Before
	public void setup() throws JobException, JobExecutionException {
		vertices = new JobVertex[4];
		vertices[0] = new JobVertex("jobVertexProducer0");
		vertices[1] = new JobVertex("jobVertexProducer1");
		vertices[2] = new JobVertex("jobVertexProducer2");

		vertices[3] = new JobVertex("jobVertexConsumer");

		for (int i = 0; i < 3; i ++) {
			vertices[3].connectNewDataSetAsInput(vertices[i], DistributionPattern.POINTWISE, ResultPartitionType.PIPELINED);
		}

		for (JobVertex vertex : vertices) {
			vertex.setParallelism(4);
			vertex.setInvokableClass(VoidInvokable.class);
		}

		executionGraph = makeExecutionGraph(new JobGraph(vertices));
	}

	@Test
	public void testExecutionEdgeIndexing() {
		for (ExecutionVertex consumerExecutionVertex : executionGraph.getAllExecutionVertices()) {
			//for all the ExecutionVertex
			int numberOfInputs = consumerExecutionVertex.getNumberOfInputs();
			for (int inputNumber = 0; inputNumber < numberOfInputs; inputNumber++) {
				//for all the JobVertex inputs of the JobVertex this ExecutionVertex is from
				ExecutionEdge[] edgesToConsumerJobVertex = consumerExecutionVertex.getInputEdges(inputNumber);

				for (ExecutionEdge edgeToConsumerJobVertex : edgesToConsumerJobVertex) {

					//verify assumption: getting the source JobVertex using a numerical ID from the consumer ExecutionVertex
					//or from the consumer JobVertex yields the same result
					Assert.assertEquals(
						edgeToConsumerJobVertex.getSource().getProducer().getJobVertex().getJobVertex(),
						consumerExecutionVertex.getJobVertex().getJobVertex().getInputs().get(inputNumber).getSource().getProducer());
				}
			}
		}
	}

}

