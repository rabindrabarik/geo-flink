package spies;

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

	private Set<SchedulingDecisionSpy> spies = new HashSet<>();

	/**
	 * Creates a new scheduler.
	 *
	 * @param executor the executor to run futures on
	 * @param spy a spy to notify of scheduling decisions
	 */
	public SpyableFlinkScheduler(Executor executor, SchedulingDecisionSpy spy) {
		super(executor);
		spies.add(spy);
	}

	public SpyableFlinkScheduler(Executor executor) {
		super(executor);
	}

	@Override
	public void addSchedulingDecisionSpy(SchedulingDecisionSpy spy) {
		spies.add(spy);
	}

	@Override
	public Set<SchedulingDecisionSpy> getSpies() {
		return spies;
	}

	@Override
	public CompletableFuture<LogicalSlot> allocateSlot(SlotRequestId slotRequestId, ScheduledUnit task, boolean allowQueued, SlotProfile slotProfile, Time allocationTimeout) {
		CompletableFuture <LogicalSlot> scheduledSlotFuture = super.allocateSlot(slotRequestId, task, allowQueued, slotProfile, allocationTimeout);
		scheduledSlotFuture.whenCompleteAsync((value, exception) -> {
			if(value != null) {
				for(SchedulingDecisionSpy spy : spies) {
					spy.setSchedulingDecisionFor(task.getTaskToExecute().getVertex(), value);
				}
			}
		});
		return scheduledSlotFuture;
	}
}
