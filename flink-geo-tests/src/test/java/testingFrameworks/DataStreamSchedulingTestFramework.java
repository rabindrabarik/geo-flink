package testingFrameworks;

import org.apache.flink.runtime.jobmanager.scheduler.Scheduler;
import org.apache.flink.runtime.testingUtils.TestingUtils;
import org.apache.flink.util.TestLogger;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import spies.SpyableFlinkScheduler;
import spies.SpyableGeoScheduler;
import testOutputWriter.TestOutputWriter;

import java.util.Arrays;
import java.util.Collection;

/**
 * This tests logs the slot usages after scheduling a DataStream
 */
@RunWith(Parameterized.class)
@Ignore
public abstract class DataStreamSchedulingTestFramework extends TestLogger {

	private final static Logger log = LoggerFactory.getLogger(DataStreamSchedulingTestFramework.class);

	private final static TestOutputWriter writer = new TestOutputWriter();

	@Parameterized.Parameters(name = "scheduling via: {0}")
	public static Collection<Object[]> data() {
		return Arrays.asList(new Object[][]{
			{new SpyableGeoScheduler(TestingUtils.defaultExecutor())}, {new SpyableFlinkScheduler(TestingUtils.defaultExecutor())}
		});
	}

	@Parameterized.Parameter(0)
	public Scheduler scheduler;

	@Test
	public void test() throws Exception {
	}
}
