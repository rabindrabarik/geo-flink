import org.apache.commons.io.FileUtils;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.collector.selector.SelectivityAwareOutputSelector;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SplitStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.test.examples.join.WindowJoinData;
import org.apache.flink.test.util.MiniClusterResource;
import org.junit.Before;
import org.junit.Test;
import spies.SpyableScheduler;
import testingFrameworks.DataStreamSchedulingTestFramework;

import java.io.File;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;

public class SelectionDataStreamSchedulingTest extends DataStreamSchedulingTestFramework {

	private Map<String, Integer> geoLocationSlotMap;

	public SelectionDataStreamSchedulingTest(SpyableScheduler scheduler, MiniClusterResource.MiniClusterType miniClusterType) {
		super(scheduler, miniClusterType);
	}

	@Override
	public Map<String, Integer> getGeoLocationSlotMap() {
		if (geoLocationSlotMap != null) {
			return geoLocationSlotMap;
		} else {
			geoLocationSlotMap = new HashMap<>();
			geoLocationSlotMap.put("center", 10);
			geoLocationSlotMap.put("edge1", 4);
			geoLocationSlotMap.put("edge2", 4);
		}
		return geoLocationSlotMap;
	}

	@Before
	public void setup() {
		jobName = "windowJoin";
		instanceSetName = "1_center_20_slots_2_edge_1_slot";
	}

	@Test
	@SuppressWarnings("Duplicates")
	public void test() throws Exception {
		final String resultPath1 = File.createTempFile("result-path", "dir").toURI().toString();
		final String resultPath2 = File.createTempFile("result-path", "dir").toURI().toString();

		final class Parser implements MapFunction<String, Tuple2<String, Integer>> {

			@Override
			public Tuple2<String, Integer> map(String value) throws Exception {
				String[] fields = value.split(",");
				return new Tuple2<>(fields[1], Integer.parseInt(fields[2]));
			}
		}

		try {
			final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
			env.setStreamTimeCharacteristic(TimeCharacteristic.IngestionTime);

			DataStream<Tuple2<String, Integer>> grades = env
				.fromElements(WindowJoinData.GRADES_INPUT.split("\n"))
				.setSourceSize(2)
				.map(new Parser(), 0.1);

			SplitStream<Tuple2<String, Integer>> splitted = grades.split(new SelectivityAwareOutputSelector<Tuple2<String, Integer>>() {
				private Random random = new Random();

				@Override
				public Map<String, Double> selectivities() {
					Map<String, Double> selectivities = new HashMap<>();
					selectivities.put("select-name-1", 0.3);
					selectivities.put("select-name-2", 0.7);
					return selectivities;
				}

				@Override
				public Iterable<String> select(Tuple2<String, Integer> value) {
					double r = random.nextDouble();
					if (r < 0.3) {
						return Collections.singleton("select-name-1");
					} else {
						return Collections.singleton("select-name-2");
					}
				}
			});

			splitted.select("select-name-1")
				.writeAsText(resultPath1, FileSystem.WriteMode.OVERWRITE);

			splitted.select("select-name-2")
				.writeAsText(resultPath2, FileSystem.WriteMode.OVERWRITE);


			env.execute();
		} finally {
			try {
				FileUtils.deleteDirectory(new File(resultPath1));
				FileUtils.deleteDirectory(new File(resultPath2));
			} catch (Throwable ignored) {
			}
		}
	}
}
