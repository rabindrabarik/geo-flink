package spies;

import java.util.Set;

public interface SpyableScheduler {

	/**
	 * Adds this spy to the set of spies to notify when shceduling decisions happen.
	 * */
	void addSchedulingDecisionSpy(SchedulingDecisionSpy spy);

	/**
	 * @return the set of spies to notify when shceduling decisions happen.
	 * */
	Set<SchedulingDecisionSpy> getSpies();

}
