import org.apache.commons.io.FileUtils;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.runtime.clusterframework.types.GeoLocation;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.test.examples.join.WindowJoinData;
import org.apache.flink.streaming.util.TestStreamEnvironment;
import org.apache.flink.test.util.MiniClusterResource;
import org.apache.flink.types.TwoKeysMap;
import org.apache.flink.types.TwoKeysMultiMap;
import org.junit.Before;
import org.junit.Test;
import spies.SpyableScheduler;
import testingFrameworks.DataStreamSchedulingTestFramework;

import java.io.File;
import java.util.HashMap;
import java.util.Map;

import static org.apache.flink.test.util.TestBaseUtils.checkLinesAgainstRegexp;

public class SimpleDataStreamSchedulingTest extends DataStreamSchedulingTestFramework {

	private Map<String, Integer> geoLocationSlotMap;

	public SimpleDataStreamSchedulingTest(SpyableScheduler scheduler, MiniClusterResource.MiniClusterType miniClusterType) {
		super(scheduler, miniClusterType);
	}

	public static GeoLocation center = new GeoLocation("center");
	public static GeoLocation edge1 = new GeoLocation("edge1");
	public static GeoLocation edge2 = new GeoLocation("edge2");
	public static GeoLocation edge3 = new GeoLocation("edge3");

	@Override
	public Map<String, Integer> getGeoLocationSlotMap() {
		if(geoLocationSlotMap != null) {
			return geoLocationSlotMap;
		} else {
			geoLocationSlotMap = new HashMap<>();
			geoLocationSlotMap.put("center", 10);
			geoLocationSlotMap.put("edge1", 4);
			geoLocationSlotMap.put("edge2", 4);
			geoLocationSlotMap.put("edge3", 4);
		}
		return geoLocationSlotMap;
	}

	@Override
	public TwoKeysMap<GeoLocation, GeoLocation, Double> getBandwidths() {
		TwoKeysMap <GeoLocation, GeoLocation, Double> bandwidths = new TwoKeysMultiMap<>();
		bandwidths.put(center, edge1, 1d);
		bandwidths.put(center, edge2, 1d);
		bandwidths.put(center, edge3, 1d);

		bandwidths.put(edge1, center, 1d);
		bandwidths.put(edge1, edge2, 1d);
		bandwidths.put(edge1, edge3, 1d);

		bandwidths.put(edge2, center, 1d);
		bandwidths.put(edge2, edge1, 1d);
		bandwidths.put(edge2, edge3, 1d);

		bandwidths.put(edge3, center, 1d);
		bandwidths.put(edge3, edge2, 1d);
		bandwidths.put(edge3, edge1, 1d);

		return bandwidths;
	}

	@Before
	public void setup() {
		jobName = "windowJoin";
		instanceSetName = "1_center_20_slots_3_edge_4_slot";
	}

	@Test
	@SuppressWarnings("Duplicates")
	public void test() throws Exception {
		final String resultPath = File.createTempFile("result-path", "dir").toURI().toString();

		final class Parser implements MapFunction<String, Tuple2<String, Integer>> {

			@Override
			public Tuple2<String, Integer> map(String value) throws Exception {
				String[] fields = value.split(",");
				return new Tuple2<>(fields[1], Integer.parseInt(fields[2]));
			}
		}

		try {
			final TestStreamEnvironment env = (TestStreamEnvironment) getEnvironment();
			env.setStreamTimeCharacteristic(TimeCharacteristic.IngestionTime);

			DataStream<Tuple2<String, Integer>> grades = env
				.fromElements(WindowJoinData.GRADES_INPUT.split("\n"))
				.setGeoLocationKey("edge1")
				.map(new Parser(),1);

			DataStream<Tuple2<String, Integer>> salaries = env
				.fromElements(WindowJoinData.SALARIES_INPUT.split("\n"))
				.setSourceSize(2)
				.setGeoLocationKey("edge2")
				.map(new Parser(), 1);

			org.apache.flink.streaming.examples.join.WindowJoin
				.runWindowJoin(grades, salaries, 100)
				.writeAsText(resultPath, FileSystem.WriteMode.OVERWRITE)
				.setGeoLocationKey("edge3");

			env.getModelParameters().setExecutionSpeedWeight(0.8);
			env.getModelParameters().setNetworkCostWeight(0.2);

			env.execute();

			// since the two sides of the join might have different speed
			// the exact output can not be checked just whether it is well-formed
			// checks that the result lines look like e.g. (bob, 2, 2015)
			checkLinesAgainstRegexp(resultPath, "^\\([a-z]+,(\\d),(\\d)+\\)");
		} finally {
			try {
				FileUtils.deleteDirectory(new File(resultPath));
			} catch (Throwable ignored) {
			}
		}
	}
}
