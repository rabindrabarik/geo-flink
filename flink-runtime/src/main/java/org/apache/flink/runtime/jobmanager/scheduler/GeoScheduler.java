package org.apache.flink.runtime.jobmanager.scheduler;

import org.apache.flink.api.common.time.Time;
import org.apache.flink.runtime.clusterframework.types.GeoLocation;
import org.apache.flink.runtime.clusterframework.types.SlotProfile;
import org.apache.flink.runtime.instance.Instance;
import org.apache.flink.runtime.instance.SlotSharingGroupId;
import org.apache.flink.runtime.jobmaster.JobManagerGateway;
import org.apache.flink.runtime.jobmaster.LogicalSlot;
import org.apache.flink.runtime.jobmaster.SlotRequestId;
import org.apache.flink.runtime.messages.Acknowledge;
import org.apache.flink.runtime.rest.handler.legacy.metrics.MetricFetcher;
import org.apache.flink.runtime.taskmanager.GeoTaskManagerLocation;
import org.apache.flink.runtime.webmonitor.retriever.GatewayRetriever;
import org.apache.flink.runtime.webmonitor.retriever.MetricQueryServiceRetriever;

import javax.annotation.Nullable;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;

/**
 * This scheduler allows Flink to make better allocation decisions when the task managers are executed in geo-distributed data centers
 * */
public class GeoScheduler extends Scheduler {

	private Map<GeoLocation, Set<Instance>> allInstancesByGeoLocation = new HashMap<>();

	private MetricFetcher metricFetcher;

	/**
	 * Creates a new scheduler.
	 *
	 * @param executor the executor to run futures on
	 * @param jobManagerRetriever a the gateway retriever for the job manager, for metrics retrieval
	 */
	public GeoScheduler(Executor executor, GatewayRetriever<JobManagerGateway> jobManagerRetriever, MetricQueryServiceRetriever queryServiceRetriever) {
		super(executor);
		this.metricFetcher = new MetricFetcher<>(jobManagerRetriever, queryServiceRetriever, executor, Time.milliseconds(1000L), Time.milliseconds(500L));
	}

	@Override
	public void newInstanceAvailable(Instance instance) {
		super.newInstanceAvailable(instance);

		GeoLocation instanceLocation;

		if(instance.getTaskManagerLocation() instanceof GeoTaskManagerLocation) {
			instanceLocation = ((GeoTaskManagerLocation) instance.getTaskManagerLocation()).getGeoLocation();
		} else {
			instanceLocation = GeoLocation.UNKNOWN;
		}


		Set<Instance> instanceSet = allInstancesByGeoLocation.getOrDefault(instanceLocation, new HashSet<>());
		instanceSet.add(instance);
		allInstancesByGeoLocation.put(instanceLocation, instanceSet);
	}

	@Override
	public void instanceDied(Instance instance) {
		if(instance == null) {
			throw new NullPointerException();
		}

		GeoLocation instanceLocation;
		if(instance.getTaskManagerLocation() instanceof GeoTaskManagerLocation) {
			instanceLocation = ((GeoTaskManagerLocation) instance.getTaskManagerLocation()).getGeoLocation();
		} else {
			instanceLocation = GeoLocation.UNKNOWN;
		}

		Set<Instance> instanceSet = allInstancesByGeoLocation.get(instanceLocation);
		if (instanceSet != null) {
			instanceSet.remove(instance);
			if (instanceSet.isEmpty()) {
				allInstancesByGeoLocation.remove(instanceLocation);
			}
		}

		super.instanceDied(instance);
	}

	@Override
	public CompletableFuture<LogicalSlot> allocateSlot(SlotRequestId slotRequestId, ScheduledUnit task, boolean allowQueued, SlotProfile slotProfile, Time allocationTimeout) {
		metricFetcher.update();

		return super.allocateSlot(slotRequestId, task, allowQueued, slotProfile, allocationTimeout);
	}

	@Override
	public void shutdown() {
		allInstancesByGeoLocation.clear();
		super.shutdown();
	}
}
