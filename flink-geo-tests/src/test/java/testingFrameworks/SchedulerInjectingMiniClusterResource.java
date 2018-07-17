package testingFrameworks;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.UnmodifiableConfiguration;
import org.apache.flink.runtime.jobmanager.scheduler.Scheduler;
import org.apache.flink.runtime.minicluster.LocalFlinkMiniCluster;
import org.apache.flink.test.util.MiniClusterResource;
import org.apache.flink.test.util.TestBaseUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import spies.SpyableFlinkScheduler;
import spies.SpyableGeoScheduler;
import spies.SpyableScheduler;

public class SchedulerInjectingMiniClusterResource extends MiniClusterResource {
	private final static Logger log = LoggerFactory.getLogger(DataStreamSchedulingTestFramework.class);

	private SpyableScheduler injectedScheduler;

	public SchedulerInjectingMiniClusterResource(MiniClusterResourceConfiguration miniClusterResourceConfiguration) {
		super(miniClusterResourceConfiguration);
	}

	public SchedulerInjectingMiniClusterResource(MiniClusterResourceConfiguration miniClusterResourceConfiguration, MiniClusterType miniClusterType) {
		super(miniClusterResourceConfiguration, miniClusterType);
	}

	public SchedulerInjectingMiniClusterResource(MiniClusterResourceConfiguration miniClusterResourceConfiguration, SpyableScheduler injectedScheduler, MiniClusterType miniClusterType) {
		super(miniClusterResourceConfiguration, miniClusterType);
		this.injectedScheduler = injectedScheduler;
	}

	public SchedulerInjectingMiniClusterResource(MiniClusterResourceConfiguration miniClusterResourceConfiguration, boolean enableClusterClient) {
		super(miniClusterResourceConfiguration, enableClusterClient);
	}

	@Override
	protected void startJobExecutorService(MiniClusterType miniClusterType) throws Exception {
		if (miniClusterType == MiniClusterType.LEGACY_SPY_FLINK) {
			if(!(injectedScheduler instanceof SpyableFlinkScheduler)) {
				throw new Exception("Uncompatible minicluster type " + miniClusterType + " and injected scheduler class " + injectedScheduler);
			}
			startLegacyMiniClusterWithSpyableScheduler(injectedScheduler);
		} else if (miniClusterType == MiniClusterType.LEGACY_SPY_GEO) {
			if(!(injectedScheduler instanceof SpyableGeoScheduler)) {
				throw new Exception("Uncompatible minicluster type " + miniClusterType + " and injected scheduler class " + injectedScheduler);
			}
			startLegacyMiniClusterWithSpyableScheduler(injectedScheduler);
		} else {
			if(injectedScheduler != null) {
				log.warn("An injected scheduler was specified but the minicluster type is not LEGACY_SPY_FLINK or LEGACY_SPY_GEO. The injected scheduler will be ignored");
			}
			super.startJobExecutorService(miniClusterType);
		}
	}

	private void startLegacyMiniClusterWithSpyableScheduler(SpyableScheduler spyableScheduler) throws Exception {
		final Configuration configuration = newLegacyMiniClusterConfiguration();

		TestBaseUtils.cleanStartClusterConfig(configuration);

		final LocalFlinkMiniCluster flinkMiniCluster = new LocalFlinkMiniCluster(configuration, (Scheduler) spyableScheduler, !enableClusterClient);

		flinkMiniCluster.start();

		jobExecutorService = flinkMiniCluster;

		if (enableClusterClient) {
			clusterClient = makeClusterClient(configuration, flinkMiniCluster);
		}

		restClusterClientConfig = new UnmodifiableConfiguration(newLegacyMiniClusterRestClientConfiguration(flinkMiniCluster));

		if (flinkMiniCluster.webMonitor().isDefined()) {
			webUIPort = flinkMiniCluster.webMonitor().get().getServerPort();
		}
	}


	public SpyableScheduler getInjectedScheduler() {
		return injectedScheduler;
	}
}
