import org.apache.commons.io.FileUtils;
import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.runtime.clusterframework.types.GeoLocation;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.functions.windowing.AllWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.streaming.util.TestStreamEnvironment;
import org.apache.flink.test.util.MiniClusterResource;
import org.apache.flink.types.TwoKeysMap;
import org.apache.flink.types.TwoKeysMultiMap;
import org.apache.flink.util.Collector;
import org.junit.Before;
import org.junit.Test;
import spies.SpyableScheduler;
import testingFrameworks.DataStreamSchedulingTestFramework;

import java.io.File;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.TimeUnit;

public class DAGDataStreamSchedulingTest extends DataStreamSchedulingTestFramework {

	private Map<String, Integer> geoLocationSlotMap;

	public DAGDataStreamSchedulingTest(SpyableScheduler scheduler, MiniClusterResource.MiniClusterType miniClusterType) {
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
		bandwidths.put(center, edge1, 2d);
		bandwidths.put(center, edge2, 2d);
		bandwidths.put(center, edge3, 2d);

		bandwidths.put(edge1, center, 2d);
		bandwidths.put(edge1, edge2, 1d);
		bandwidths.put(edge1, edge3, 1d);

		bandwidths.put(edge2, center, 2d);
		bandwidths.put(edge2, edge1, 1d);
		bandwidths.put(edge2, edge3, 1d);

		bandwidths.put(edge3, center, 2d);
		bandwidths.put(edge3, edge2, 1d);
		bandwidths.put(edge3, edge1, 1d);

		return bandwidths;
	}

	@Before
	public void setup() {
		jobName = "windowJoin";
		instanceSetName = instanceSetNameFromGeoLocationSlotMap(getGeoLocationSlotMap());
	}

	private Collection<Tuple2<String, Integer>> makeInputCollection() {
		Random r = new Random();
		Collection<Tuple2<String, Integer>> vals = new ArrayList<>();
		for(int i = 0; i < 50; i ++) {
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
