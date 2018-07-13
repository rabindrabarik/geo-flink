package org.apache.flink.runtime.jobmanager.scheduler.spies;

public interface SpyableScheduler {

	/**
	 * Adds this spy to the set of spies to notify when shceduling decisions happen.
	 * */
	void addSchedulingDecisionSpy(SchedulingDecisionSpy spy);
}
