package writableTypes;

import org.apache.flink.runtime.clusterframework.types.GeoLocation;
import org.apache.flink.types.TwoKeysMap;
import org.apache.flink.types.TwoKeysMultiMap;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

public class DistributedDeploymentsGeoLocationAndBandwidths extends TestGeoLocationAndBandwidths {
	private Map<GeoLocation, Integer> geoLocationSlotMap = new HashMap<>();
	private TwoKeysMap<GeoLocation, GeoLocation, Double> bandwidths = new TwoKeysMultiMap<>();

	public DistributedDeploymentsGeoLocationAndBandwidths(int numberOfClouds, int cloudsSlots, double interCloudBandwidth) {
		this.params = new Object[3];
		params[0] = numberOfClouds;
		params[1] = cloudsSlots;
		params[2] = interCloudBandwidth;

		Set<GeoLocation> clouds = new HashSet<>();

		for(int i = 0; i < numberOfClouds; i ++) {
			GeoLocation edge = new GeoLocation("location" + i);
			clouds.add(edge);
			geoLocationSlotMap.put(edge, cloudsSlots);
		}

		for (GeoLocation from : clouds) {
			for (GeoLocation to : clouds) {
				if(!from.equals(to)) {
					bandwidths.put(from, to, interCloudBandwidth);
				}
			}
		}

	}

	@Override
	public Map<GeoLocation, Integer> getGeoLocationSlotMap() {
		return geoLocationSlotMap;
	}

	@Override
	public TwoKeysMap<GeoLocation, GeoLocation, Double> getBandwidths() {
		return bandwidths;
	}
}
