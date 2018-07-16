package writableTypes;

import org.apache.flink.runtime.instance.Instance;

import java.util.Set;

public abstract class TestInstanceSet extends WritableType {
	public abstract Set<Instance> getInstances();
}
