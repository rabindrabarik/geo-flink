package org.apache.flink.runtime.jobmanager.scheduler;

import org.apache.flink.runtime.executiongraph.Execution;
import org.apache.flink.runtime.jobmaster.LogicalSlot;

import java.util.HashMap;
import java.util.Map;

public class SchedulingDecision {
	private Map<Execution, LogicalSlot> decisions = new HashMap<>();

	public LogicalSlot getDecision(Execution ex) {
		return decisions.get(ex);
	}

	public void setDecision(Execution ex, LogicalSlot slot) {
		this.decisions.put(ex, slot);
	}
}
