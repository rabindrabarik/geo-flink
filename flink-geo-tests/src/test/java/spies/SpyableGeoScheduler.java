package spies;

import org.apache.flink.api.common.time.Time;
import org.apache.flink.runtime.clusterframework.types.SlotProfile;
import org.apache.flink.runtime.executiongraph.ExecutionGraph;
import org.apache.flink.runtime.executiongraph.OptimisationModelSolution;
import org.apache.flink.runtime.jobmanager.scheduler.GeoScheduler;
import org.apache.flink.runtime.jobmanager.scheduler.ScheduledUnit;
import org.apache.flink.runtime.jobmaster.LogicalSlot;
import org.apache.flink.runtime.jobmaster.SlotRequestId;

import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;

public class SpyableGeoScheduler extends GeoScheduler implements SpyableScheduler {
	/**
	 * Creates a new scheduler.
	 *
	 * @param executor the executor to run futures on
	 */
	public SpyableGeoScheduler(Executor executor) {
		super(executor);
	}

	private Set<SchedulingDecisionSpy> spies = new HashSet<>();

	@Override
	public void addSchedulingDecisionSpy(SchedulingDecisionSpy spy) {
		spies.add(spy);
	}

	@Override
	public CompletableFuture<LogicalSlot> allocateSlot(SlotRequestId slotRequestId, ScheduledUnit task, boolean allowQueued, SlotProfile slotProfile, Time allocationTimeout) {
		CompletableFuture<LogicalSlot> scheduledSlotFuture = super.allocateSlot(slotRequestId, task, allowQueued, slotProfile, allocationTimeout);
		scheduledSlotFuture.whenCompleteAsync((value, exception) -> {
			if (value != null) {
				for (SchedulingDecisionSpy spy : spies) {
					spy.setSchedulingDecisionFor(task.getTaskToExecute().getVertex(), value);
				}
			}
		});
		return scheduledSlotFuture;
	}

	@Override
	public void addGraphSolution(ExecutionGraph executionGraph, OptimisationModelSolution solution) {
		super.addGraphSolution(executionGraph, solution);
		for (SchedulingDecisionSpy spy : spies) {
			spy.setModelSolveTime(executionGraph, solution.getExecutionTime());

		}
	}
}
