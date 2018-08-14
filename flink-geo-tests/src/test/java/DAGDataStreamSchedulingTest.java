import org.apache.commons.io.FileUtils;
import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.functions.windowing.AllWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.streaming.util.TestStreamEnvironment;
import org.apache.flink.test.util.MiniClusterResource;
import org.apache.flink.util.Collector;
import org.junit.Before;
import org.junit.Test;
import spies.SpyableScheduler;
import testingFrameworks.DataStreamSchedulingTestFramework;
import writableTypes.CentralAndEdgeGeoLocationAndBandwidths;
import writableTypes.TestGeoLocationAndBandwidths;

import java.io.File;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Random;
import java.util.concurrent.TimeUnit;

public class DAGDataStreamSchedulingTest extends DataStreamSchedulingTestFramework {

	private static TestGeoLocationAndBandwidths geoLocationAndBandwidths = new CentralAndEdgeGeoLocationAndBandwidths(
		6,
		1,
		1,
		1d,
		10d);


	@Override
	public TestGeoLocationAndBandwidths getTestGeoLocationAndBandwidths() {
		return geoLocationAndBandwidths;
	}
	@Before
	public void setup() {
		jobName = "windowJoin";
	}

	private Collection<Tuple2<String, Integer>> makeInputCollection() {
		Random r = new Random();
		Collection<Tuple2<String, Integer>> vals = new ArrayList<>();
		for(int i = 0; i < 2; i ++) {
			vals.add(new Tuple2<>(JobID.generate().toString(), r.nextInt(10)));
		}
		return vals;
	}

	@Test
	@SuppressWarnings("Duplicates")
	public void test() throws Exception {
		final String resultPath = File.createTempFile("result-path", "dir").toURI().toString();

		try {
			TestStreamEnvironment env = (TestStreamEnvironment) getEnvironment();
			env.setStreamTimeCharacteristic(TimeCharacteristic.IngestionTime);

			DataStream<Tuple2<String, Integer>> input1 = env.fromCollection(makeInputCollection(), TypeInformation.of(new TypeHint<Tuple2<String, Integer>>(){}))
				.setGeoLocationKey("edge1");


			DataStream<Tuple2<String, Integer>> input2 = env.fromCollection(makeInputCollection(), TypeInformation.of(new TypeHint<Tuple2<String, Integer>>(){}))
				.setGeoLocationKey("edge2");

			DataStream<Tuple3<String, String, Integer>> join1and2 = input1.join(input2)
				.where(new KeySelector<Tuple2<String,Integer>, Object>() {
					@Override
					public Object getKey(Tuple2<String, Integer> value) throws Exception {
						return value.f1;
					}
				})
				.equalTo(new KeySelector<Tuple2<String,Integer>, Object>() {
					@Override
					public Object getKey(Tuple2<String, Integer> value) throws Exception {
						return value.f1;
					}
				})
				.window(TumblingEventTimeWindows.of(Time.of(5, TimeUnit.SECONDS)))
      			.apply(new JoinFunction<Tuple2<String, Integer>, Tuple2<String, Integer>, Tuple3<String, String, Integer>>() {
					@Override
					public Tuple3<String, String, Integer> join(Tuple2<String, Integer> first, Tuple2<String, Integer> second) throws Exception {
						return new Tuple3<>(first.f0, second.f0, second.f1);
					}
				});

			DataStream<Tuple2<String, Integer>> input3 = env.fromCollection(makeInputCollection(), TypeInformation.of(new TypeHint<Tuple2<String, Integer>>(){}));

			DataStream<Tuple3<String, String, Integer>> join1and2and3 = join1and2.join(input3)
				.where(new KeySelector<Tuple3<String, String,Integer>, Object>() {
					@Override
					public Object getKey(Tuple3<String, String, Integer> value) throws Exception {
						return value.f2;
					}
				})
				.equalTo(new KeySelector<Tuple2<String,Integer>, Object>() {
					@Override
					public Object getKey(Tuple2<String, Integer> value) throws Exception {
						return value.f1;
					}
				})
				.window(TumblingEventTimeWindows.of(Time.of(5, TimeUnit.SECONDS)))
				.apply(new JoinFunction<Tuple3<String, String, Integer>, Tuple2<String, Integer>, Tuple3<String, String, Integer>>() {
					@Override
					public Tuple3<String, String, Integer> join(Tuple3<String, String, Integer> first, Tuple2<String, Integer> second) throws Exception {
						return new Tuple3<>(first.f0, second.f0, second.f1);
					}
				});

			join1and2and3.timeWindowAll(Time.of(5, TimeUnit.SECONDS), Time.of(2, TimeUnit.SECONDS))
				.apply(new AllWindowFunction<Tuple3<String,String,Integer>, Object, TimeWindow>() {
					@Override
					public void apply(TimeWindow window, Iterable<Tuple3<String, String, Integer>> values, Collector<Object> out) throws Exception {
							Tuple3[] largest = new Tuple3[10];
							int filled = 0;
							for(Tuple3 tuple : values) {
								if(filled < 10) {
									largest[filled] = tuple;
									filled ++;
								} else {
									for(int i = 0; i < filled; i ++) {
										if((Integer) largest[i].f2 < (Integer) tuple.f2) {
											largest[i] = tuple;
										}
									}
								}
							}
							for(Tuple3 tuple : largest) {
								out.collect(tuple);
							}
					}
				})
				.writeAsText(resultPath, FileSystem.WriteMode.OVERWRITE);

			env.getModelParameters().setExecutionSpeedWeight(0.5);
			env.getModelParameters().setNetworkCostWeight(0.5);


			env.execute();
		} finally {
			try {
				FileUtils.deleteDirectory(new File(resultPath));
			} catch (Throwable ignored) {
			}
		}
	}
}
