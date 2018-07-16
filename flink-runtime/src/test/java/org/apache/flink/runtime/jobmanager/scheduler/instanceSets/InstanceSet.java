package org.apache.flink.runtime.jobmanager.scheduler.instanceSets;

import org.apache.flink.runtime.instance.Instance;
import org.apache.flink.runtime.jobmanager.scheduler.WritableType;

import java.util.Set;

public abstract class InstanceSet extends WritableType {
	public abstract Set<Instance> getInstances();
}
