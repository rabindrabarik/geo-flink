package writableTypes;

import org.apache.flink.runtime.clusterframework.types.GeoLocation;
import org.apache.flink.types.TwoKeysMap;
import org.apache.flink.types.TwoKeysMultiMap;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

public class CentralAndEdgeGeoLocationAndBandwidths extends TestGeoLocationAndBandwidths {
	private Map<GeoLocation, Integer> geoLocationSlotMap = new HashMap<>();
	private TwoKeysMap<GeoLocation, GeoLocation, Double> bandwidths = new TwoKeysMultiMap<>();

	public CentralAndEdgeGeoLocationAndBandwidths(int numberOfEdgeClouds, int edgeCloudsSlots, int centralCloudSlots, double centralEdgeBandwidth, double edgeEdgeBandwidth) {
		this.params = new Object[5];
		params[0] = numberOfEdgeClouds;
		params[1] = edgeCloudsSlots;
		params[2] = centralCloudSlots;
		params[3] = centralEdgeBandwidth;
		params[4] = edgeEdgeBandwidth;

		GeoLocation centralCloud = new GeoLocation("central");

		geoLocationSlotMap.put(centralCloud, centralCloudSlots);

		Set<GeoLocation> edgeClouds = new HashSet<>();

		for(int i = 0; i < numberOfEdgeClouds; i ++) {
			GeoLocation edge = new GeoLocation("edge" + i);
			edgeClouds.add(edge);
			geoLocationSlotMap.put(edge, edgeCloudsSlots);
			bandwidths.put(edge, centralCloud, centralEdgeBandwidth);
			bandwidths.put(centralCloud, edge, centralEdgeBandwidth);
		}

		for (GeoLocation from : edgeClouds) {
			for (GeoLocation to : edgeClouds) {
				if(!from.equals(to)) {
					bandwidths.put(from, to, edgeEdgeBandwidth);
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
