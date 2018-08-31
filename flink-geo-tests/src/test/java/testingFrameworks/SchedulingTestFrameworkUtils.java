package testingFrameworks;

import org.apache.flink.api.common.JobID;
import org.apache.flink.runtime.executiongraph.ExecutionGraph;
import org.apache.flink.runtime.jobmanager.scheduler.Scheduler;
import spies.SchedulingDecisionSpy;
import testOutputWriter.TestOutputImpl;
import testOutputWriter.TestOutputWriter;

public class SchedulingTestFrameworkUtils {
	public static void writeTestOutcome(JobID jobId, SchedulingDecisionSpy spy, Scheduler scheduler, TestOutputWriter<TestOutputImpl> writer, String jobString, String instanceString) {
		double networkCost = spy.calculateNetworkCost(jobId);
		double executionSpeed = spy.calculateExecutionSpeed(jobId);

		writer.write(new TestOutputImpl(
			networkCost,
			executionSpeed,
			scheduler.getClass().getSimpleName(),
			jobString,
			instanceString,
			Math.round(spy.getModelSolveTime(jobId) * 1000)));
	}
}
