package org.apache.flink.runtime.taskmanager;

import org.apache.flink.runtime.clusterframework.types.GeoLocation;
import org.apache.flink.runtime.clusterframework.types.ResourceID;

import java.net.InetAddress;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * This class extends {@link TaskManagerLocation} with a location field, indicating in which of the geo-distributed data centers a task manger is executed
 */
public class GeoTaskManagerLocation extends TaskManagerLocation {

	private GeoLocation location;

	/**
	 * Constructs a new instance connection info object. The constructor will attempt to retrieve the instance's
	 * host name and domain name through the operating system's lookup mechanisms.
	 *
	 * @param resourceID
	 * @param inetAddress the network address the instance's task manager binds its sockets to
	 * @param dataPort
	 * @param location    the geo-distributed locations of a task manager
	 */
	public GeoTaskManagerLocation(ResourceID resourceID, InetAddress inetAddress, int dataPort, GeoLocation location) {
		super(resourceID, inetAddress, dataPort);
		this.location = checkNotNull(location);
	}

	public GeoLocation getGeoLocation() {
		return location;
	}
}
