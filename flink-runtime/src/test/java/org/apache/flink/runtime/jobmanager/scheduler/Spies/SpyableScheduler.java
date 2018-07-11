package org.apache.flink.runtime.jobmanager.scheduler.Spies;

public interface SpyableScheduler {

	/**
	 * Adds this spy to the set of spies to notify when shceduling decisions happen.
	 * */
	void addSchedulingDecisionSpy(SchedulingDecisionSpy spy);
}
