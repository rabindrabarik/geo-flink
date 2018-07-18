package testingFrameworks;

import org.apache.flink.runtime.executiongraph.ExecutionGraph;
import org.apache.flink.runtime.jobmanager.scheduler.Scheduler;
import spies.SchedulingDecisionSpy;
import testOutputWriter.TestOutputImpl;
import testOutputWriter.TestOutputWriter;

public class SchedulingTestFrameworkUtils {
	public static void writeTestOutcome(ExecutionGraph executionGraph, SchedulingDecisionSpy spy, Scheduler scheduler, TestOutputWriter<TestOutputImpl> writer, String jobString, String instanceString) {
		double networkCost = spy.calculateNetworkCost(executionGraph);
		double executionSpeed = spy.calculateExecutionSpeed(executionGraph);

		System.out.println("network cost: " + networkCost);
		System.out.println("execution speed: " + executionSpeed);
		System.out.println("\n\n");
		System.out.println(spy.getAssignementsString());

		writer.write(new TestOutputImpl(
			networkCost,
			executionSpeed,
			scheduler.getClass().getSimpleName(),
			jobString,
			instanceString,
			Math.round(spy.getModelSolveTime(executionGraph) * 1000)));
	}
}
