package writableTypes;

import org.apache.flink.runtime.clusterframework.types.GeoLocation;
import org.apache.flink.types.TwoKeysMap;

import java.util.Map;

public abstract class TestGeoLocationAndBandwidths extends WritableType {
	public abstract Map<GeoLocation ,Integer> getGeoLocationSlotMap();

	public abstract TwoKeysMap<GeoLocation, GeoLocation, Double> getBandwidths();
}
