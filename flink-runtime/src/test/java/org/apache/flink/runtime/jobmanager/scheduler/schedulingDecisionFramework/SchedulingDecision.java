package org.apache.flink.runtime.jobmanager.scheduler.schedulingDecisionFramework;

import org.apache.flink.api.java.tuple.Tuple5;

public class SchedulingDecision extends Tuple5<Double, Double, String, String, String> {
	public SchedulingDecision(Double networkCost, Double executionSpeed, String schedulerClassName, String jobGraphClassName, String instanceSetClassName) {
		this.f0 = networkCost;
		this.f1 = executionSpeed;
		this.f2 = schedulerClassName;
		this.f3 = jobGraphClassName;
		this.f4 = jobGraphClassName;
	}
}
