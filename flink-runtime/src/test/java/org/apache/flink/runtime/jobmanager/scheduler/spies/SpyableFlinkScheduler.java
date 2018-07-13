package org.apache.flink.runtime.jobmanager.scheduler.spies;

import org.apache.flink.api.common.time.Time;
import org.apache.flink.runtime.clusterframework.types.SlotProfile;
import org.apache.flink.runtime.jobmanager.scheduler.ScheduledUnit;
import org.apache.flink.runtime.jobmanager.scheduler.Scheduler;
import org.apache.flink.runtime.jobmaster.LogicalSlot;
import org.apache.flink.runtime.jobmaster.SlotRequestId;

import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;

public class SpyableFlinkScheduler extends Scheduler implements SpyableScheduler {
	/**
	 * Creates a new scheduler.
	 *
	 * @param executor the executor to run futures on
	 */
	public SpyableFlinkScheduler(Executor executor) {
		super(executor);
	}

	private Set<SchedulingDecisionSpy> spies = new HashSet<>();

	@Override
	public void addSchedulingDecisionSpy(SchedulingDecisionSpy spy) {
		spies.add(spy);
	}

	@Override
	public CompletableFuture<LogicalSlot> allocateSlot(SlotRequestId slotRequestId, ScheduledUnit task, boolean allowQueued, SlotProfile slotProfile, Time allocationTimeout) {
		CompletableFuture <LogicalSlot> scheduledSlotFuture = super.allocateSlot(slotRequestId, task, allowQueued, slotProfile, allocationTimeout);
		scheduledSlotFuture.whenCompleteAsync((value, exception) -> {
			if(value != null) {
				for(SchedulingDecisionSpy spy : spies) {
					spy.addAssignementFor(task.getTaskToExecute().getVertex(), value);
				}
			}
		});
		return scheduledSlotFuture;
	}
}
