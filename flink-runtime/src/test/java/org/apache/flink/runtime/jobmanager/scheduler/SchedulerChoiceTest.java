package org.apache.flink.runtime.jobmanager.scheduler;


import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.JobManagerOptions;
import org.apache.flink.runtime.blob.VoidBlobStore;
import org.apache.flink.runtime.jobmanager.JobManager;
import org.apache.flink.runtime.metrics.NoOpMetricRegistry;
import org.apache.flink.runtime.testingUtils.TestingUtils;
import org.apache.flink.util.TestLogger;
import org.junit.Test;
import scala.Tuple10;

import static org.junit.Assert.assertTrue;

public class SchedulerChoiceTest extends TestLogger {

	/**
	 * Tests that when the geo scheduling is enabled in config and the actor system passed the geoscheduler is instantiated
	 */
	@Test
	public void geoSchedulingChoiceWithActorTest() {
		Configuration conf = new Configuration();
		conf.setBoolean(JobManagerOptions.IS_GEO_SCHEDULING_ENABLED, true);

		Tuple10 t = JobManager.createJobManagerComponents(
			conf,
			TestingUtils.defaultExecutor(),
			TestingUtils.defaultExecutor(),
			new VoidBlobStore(),
			NoOpMetricRegistry.INSTANCE);

		Scheduler scheduler = (Scheduler) t._2();

		assertTrue(scheduler instanceof GeoScheduler);
	}

	/**
	 * Tests that when the geo scheduling is enabled in config and the actor system is not passed the standard scheduler is instantiated
	 * */
	@Test
	public void geoSchedulingChoiceNoActorTest() {
		Configuration conf = new Configuration();
		conf.setBoolean(JobManagerOptions.IS_GEO_SCHEDULING_ENABLED, true);

		Tuple10 t = JobManager.createJobManagerComponents(
			conf,
			TestingUtils.defaultExecutor(),
			TestingUtils.defaultExecutor(),
			new VoidBlobStore(),
			NoOpMetricRegistry.INSTANCE);

		Scheduler scheduler = (Scheduler) t._2();

		assertTrue(scheduler instanceof Scheduler);
	}

	/**
	 * Tests that when the geo scheduling is disabled in config the standard scheduler is instantiated
	 * */
	@Test
	public void standardSchedulingChoiceTest() {
		Configuration conf = new Configuration();
		conf.setBoolean(JobManagerOptions.IS_GEO_SCHEDULING_ENABLED, false);

		Tuple10 t = JobManager.createJobManagerComponents(
			conf,
			TestingUtils.defaultExecutor(),
			TestingUtils.defaultExecutor(),
			new VoidBlobStore(),
			NoOpMetricRegistry.INSTANCE);

		Scheduler scheduler = (Scheduler) t._2();

		assertTrue(scheduler instanceof Scheduler);
	}
}
