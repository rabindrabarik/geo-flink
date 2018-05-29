package org.apache.flink.runtime.jobmanager;


import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.JobManagerOptions;
import org.apache.flink.runtime.akka.AkkaUtils;
import org.apache.flink.runtime.blob.VoidBlobStore;
import org.apache.flink.runtime.jobmanager.scheduler.AbstractScheduler;
import org.apache.flink.runtime.jobmanager.scheduler.GeoScheduler;
import org.apache.flink.runtime.metrics.NoOpMetricRegistry;
import org.apache.flink.runtime.testingUtils.TestingUtils;
import org.apache.flink.util.TestLogger;
import org.junit.Test;
import scala.Tuple10;

import static org.junit.Assert.assertTrue;

public class SchedulerChoiceTest extends TestLogger {

	@Test
	public void geoSchedulingChoiceWithActor() {
		Configuration conf = new Configuration();
		conf.setBoolean(JobManagerOptions.IS_GEO_SCHEDULING_ENABLED, true);

		Tuple10 t = JobManager.createJobManagerComponents(
			conf,
			TestingUtils.defaultExecutor(),
			TestingUtils.defaultExecutor(),
			new VoidBlobStore(),
			NoOpMetricRegistry.INSTANCE,
			AkkaUtils.createLocalActorSystem(new Configuration()));

		AbstractScheduler scheduler = (AbstractScheduler) t._2();

		assertTrue(scheduler instanceof  GeoScheduler);
	}
}
