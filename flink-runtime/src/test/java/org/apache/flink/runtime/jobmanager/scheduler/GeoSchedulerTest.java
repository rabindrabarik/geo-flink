package org.apache.flink.runtime.jobmanager.scheduler;

import org.apache.flink.runtime.clusterframework.types.GeoLocation;
import org.apache.flink.runtime.instance.Instance;
import org.apache.flink.runtime.testingUtils.TestingUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import static org.junit.Assert.*;

public class GeoSchedulerTest {

	GeoScheduler scheduler;

	@Before
	public void setUp() throws Exception {
		scheduler = new GeoScheduler(TestingUtils.defaultExecutor());
	}

	@After
	public void tearDown() throws Exception {
		scheduler.shutdown();
	}

	@Test
	public void newInstanceAvailable() {
		GeoLocation location = new GeoLocation("test-location");
		Instance i1 = SchedulerTestUtils.getRandomInstance(1, location);

		scheduler.newInstanceAvailable(i1);

		assertTrue(scheduler.getAllInstancesByGeoLocation().get(location).contains(i1));
	}

	@Test
	public void instanceDied() {
		GeoLocation location = new GeoLocation("test-location");
		Instance i1 = SchedulerTestUtils.getRandomInstance(1, location);

		scheduler.newInstanceAvailable(i1);

		assertTrue(scheduler.getAllInstancesByGeoLocation().get(location).contains(i1));

		scheduler.instanceDied(i1);

		assertTrue(! scheduler.getAllInstancesByGeoLocation().containsKey(location));
	}


	@Test
	public void availableSlotsAndInstancesByGeoLocation() {
		GeoLocation[] locations = new GeoLocation[5];
		Map<GeoLocation, Set<Instance>> instancesByLoc = new HashMap<>();
		Map<GeoLocation, Integer> slotsByLoc = new HashMap<>();

		for(int i = 0; i < locations.length; i ++) {
			locations[i] = new GeoLocation("test-location-" + i);

			Set<Instance> instances = new HashSet<>();

			for(int j = 0; j < 7; j ++) {
				Instance instance = SchedulerTestUtils.getRandomInstance(1, locations[i]);
				scheduler.newInstanceAvailable(instance);
				instances.add(instance);
				slotsByLoc.put(locations[i], slotsByLoc.getOrDefault(locations[i], 0) + 1);
			}

			instancesByLoc.put(locations[i], instances);
		}

		assertEquals(scheduler.getAllInstancesByGeoLocation(), instancesByLoc);
		assertEquals(scheduler.calculateAvailableSlotsByGeoLocation(), slotsByLoc);
	}
}
