import data.WindowJoinData;
import org.apache.commons.io.FileUtils;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.util.TestStreamEnvironment;
import org.apache.flink.test.util.MiniClusterResource;
import org.junit.Before;
import org.junit.Test;
import spies.SpyableScheduler;
import testingFrameworks.DataStreamSchedulingTestFramework;
import writableTypes.CentralAndEdgeGeoLocationAndBandwidths;
import writableTypes.TestGeoLocationAndBandwidths;

import java.io.File;

import static org.apache.flink.test.util.TestBaseUtils.checkLinesAgainstRegexp;

public class SimpleDataStreamSchedulingTest extends DataStreamSchedulingTestFramework {

	private static TestGeoLocationAndBandwidths geoLocationAndBandwidths = new CentralAndEdgeGeoLocationAndBandwidths(
		3,
		4,
		10,
		2d,
		1d);

	@Override
	public TestGeoLocationAndBandwidths getTestGeoLocationAndBandwidths() {
		return geoLocationAndBandwidths;
	}

	@Before
	public void setup() {
		jobName = "windowJoin";
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

			env.getModelParameters().setExecutionSpeedWeight(0.5);
			env.getModelParameters().setNetworkCostWeight(0.5);

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
