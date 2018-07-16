package org.apache.flink.streaming.api.collector.selector;

import java.util.Map;

/**
 * Interface that extends {@link OutputSelector} adding a method to get the selectivities for the each output.
 */
public interface SelectivityAwareOutputSelector<OUT> extends OutputSelector<OUT> {

	/**
	 * @return a mapping from the output names to the respective selectivity.
	 */
	Map<String, Double> selectivities();
}
