/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.runtime.jobmanager.scheduler;

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.runtime.clusterframework.types.ResourceID;
import org.apache.flink.runtime.clusterframework.types.SlotProfile;
import org.apache.flink.runtime.concurrent.FutureUtils;
import org.apache.flink.runtime.executiongraph.ExecutionVertex;
import org.apache.flink.runtime.instance.Instance;
import org.apache.flink.runtime.instance.InstanceDiedException;
import org.apache.flink.runtime.instance.InstanceListener;
import org.apache.flink.runtime.instance.SharedSlot;
import org.apache.flink.runtime.instance.SimpleSlot;
import org.apache.flink.runtime.instance.SlotSharingGroupAssignment;
import org.apache.flink.runtime.instance.SlotSharingGroupId;
import org.apache.flink.runtime.jobgraph.JobVertexID;
import org.apache.flink.runtime.jobmaster.LogicalSlot;
import org.apache.flink.runtime.jobmaster.SlotRequestId;
import org.apache.flink.runtime.jobmaster.slotpool.SlotProvider;
import org.apache.flink.runtime.messages.Acknowledge;
import org.apache.flink.runtime.taskmanager.TaskManagerLocation;
import org.apache.flink.util.ExceptionUtils;
import org.apache.flink.util.FlinkException;
import org.apache.flink.util.Preconditions;

import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

/**
 * The scheduler is responsible for distributing the ready-to-run tasks among instances and slots.
 * 
 * <p>The scheduler supports two scheduling modes:</p>
 * <ul>
 *     <li>Immediate scheduling: A request for a task slot immediately returns a task slot, if one is
 *         available, or throws a {@link NoResourceAvailableException}.</li>
 *     <li>Queued Scheduling: A request for a task slot is queued and returns a future that will be
 *         fulfilled as soon as a slot becomes available.</li>
 * </ul>
 */
public class Scheduler extends AbstractScheduler {

	/**
	 * Creates a new scheduler.
	 */
	public Scheduler(Executor executor) {
		super(executor);
	}

	// ------------------------------------------------------------------------
	//  Scheduling
	// ------------------------------------------------------------------------

	@Override
	public CompletableFuture<LogicalSlot> allocateSlot(
		SlotRequestId slotRequestId,
		ScheduledUnit task,
		boolean allowQueued,
		SlotProfile slotProfile,
		Time allocationTimeout) {

		try {
			final Object ret = scheduleTask(task, allowQueued, slotProfile.getPreferredLocations());

			if (ret instanceof SimpleSlot) {
				return CompletableFuture.completedFuture((SimpleSlot) ret);
			}
			else if (ret instanceof CompletableFuture) {
				@SuppressWarnings("unchecked")
				CompletableFuture<LogicalSlot> typed = (CompletableFuture<LogicalSlot>) ret;
				return FutureUtils.orTimeout(typed, allocationTimeout.toMilliseconds(), TimeUnit.MILLISECONDS);
			}
			else {
				// this should never happen, simply guard this case with an exception
				throw new RuntimeException();
			}
		} catch (NoResourceAvailableException e) {
			return FutureUtils.completedExceptionally(e);
		}
	}

	@Override
	public CompletableFuture<Acknowledge> cancelSlotRequest(SlotRequestId slotRequestId, @Nullable SlotSharingGroupId slotSharingGroupId, Throwable cause) {
		return CompletableFuture.completedFuture(Acknowledge.get());
	}

	/**
	 * Returns either a {@link SimpleSlot}, or a {@link CompletableFuture}.
	 */
	private Object scheduleTask(ScheduledUnit task, boolean queueIfNoResource, Iterable<TaskManagerLocation> preferredLocations) throws NoResourceAvailableException {
		if (task == null) {
			throw new NullPointerException();
		}
		if (LOG.isDebugEnabled()) {
			LOG.debug("Scheduling task " + task);
		}

		final ExecutionVertex vertex = task.getTaskToExecute().getVertex();

		final boolean forceExternalLocation = false &&
			preferredLocations != null && preferredLocations.iterator().hasNext();

		synchronized (globalLock) {

			SlotSharingGroup sharingUnit = vertex.getJobVertex().getSlotSharingGroup();

			if (sharingUnit != null) {

				// 1)  === If the task has a slot sharing group, schedule with shared slots ===

				if (queueIfNoResource) {
					throw new IllegalArgumentException(
						"A task with a vertex sharing group was scheduled in a queued fashion.");
				}

				final SlotSharingGroupAssignment assignment = sharingUnit.getTaskAssignment();
				final CoLocationConstraint constraint = task.getCoLocationConstraint();

				// sanity check that we do not use an externally forced location and a co-location constraint together
				if (constraint != null && forceExternalLocation) {
					throw new IllegalArgumentException("The scheduling cannot be constrained simultaneously by a "
						+ "co-location constraint and an external location constraint.");
				}

				// get a slot from the group, if the group has one for us (and can fulfill the constraint)
				final SimpleSlot slotFromGroup;
				if (constraint == null) {
					slotFromGroup = assignment.getSlotForTask(vertex.getJobvertexId(), preferredLocations);
				}
				else {
					slotFromGroup = assignment.getSlotForTask(constraint, preferredLocations);
				}

				SimpleSlot newSlot = null;
				SimpleSlot toUse = null;

				// the following needs to make sure any allocated slot is released in case of an error
				try {

					// check whether the slot from the group is already what we want.
					// any slot that is local, or where the assignment was unconstrained is good!
					if (slotFromGroup != null && slotFromGroup.getLocality() != Locality.NON_LOCAL) {

						// if this is the first slot for the co-location constraint, we lock
						// the location, because we are quite happy with the slot
						if (constraint != null && !constraint.isAssigned()) {
							constraint.lockLocation();
						}

						updateLocalityCounters(slotFromGroup, vertex);
						return slotFromGroup;
					}

					// the group did not have a local slot for us. see if we can one (or a better one)

					// our location preference is either determined by the location constraint, or by the
					// vertex's preferred locations
					final Iterable<TaskManagerLocation> locations;
					final boolean localOnly;
					if (constraint != null && constraint.isAssigned()) {
						locations = Collections.singleton(constraint.getLocation());
						localOnly = true;
					}
					else {
						locations = preferredLocations;
						localOnly = forceExternalLocation;
					}

					newSlot = getNewSlotForSharingGroup(vertex, locations, assignment, constraint, localOnly);

					if (newSlot == null) {
						if (slotFromGroup == null) {
							// both null, which means there is nothing available at all

							if (constraint != null && constraint.isAssigned()) {
								// nothing is available on the node where the co-location constraint forces us to
								throw new NoResourceAvailableException("Could not allocate a slot on instance " +
									constraint.getLocation() + ", as required by the co-location constraint.");
							}
							else if (forceExternalLocation) {
								// could not satisfy the external location constraint
								String hosts = getHostnamesFromInstances(preferredLocations);
								throw new NoResourceAvailableException("Could not schedule task " + vertex
									+ " to any of the required hosts: " + hosts);
							}
							else {
								// simply nothing is available
								throw new NoResourceAvailableException(task, getNumberOfAvailableInstances(),
									getTotalNumberOfSlots(), getNumberOfAvailableSlots());
							}
						}
						else {
							// got a non-local from the group, and no new one, so we use the non-local
							// slot from the sharing group
							toUse = slotFromGroup;
						}
					}
					else if (slotFromGroup == null || !slotFromGroup.isAlive() || newSlot.getLocality() == Locality.LOCAL) {
						// if there is no slot from the group, or the new slot is local,
						// then we use the new slot
						if (slotFromGroup != null) {
							slotFromGroup.releaseSlot(null);
						}
						toUse = newSlot;
					}
					else {
						// both are available and usable. neither is local. in that case, we may
						// as well use the slot from the sharing group, to minimize the number of
						// instances that the job occupies
						newSlot.releaseSlot(null);
						toUse = slotFromGroup;
					}

					// if this is the first slot for the co-location constraint, we lock
					// the location, because we are going to use that slot
					if (constraint != null && !constraint.isAssigned()) {
						constraint.lockLocation();
					}

					updateLocalityCounters(toUse, vertex);
				}
				catch (NoResourceAvailableException e) {
					throw e;
				}
				catch (Throwable t) {
					if (slotFromGroup != null) {
						slotFromGroup.releaseSlot(t);
					}
					if (newSlot != null) {
						newSlot.releaseSlot(t);
					}

					ExceptionUtils.rethrow(t, "An error occurred while allocating a slot in a sharing group");
				}

				return toUse;
			}
			else {

				// 2) === schedule without hints and sharing ===

				SimpleSlot slot = getFreeSlotForTask(vertex, preferredLocations, forceExternalLocation);
				if (slot != null) {
					updateLocalityCounters(slot, vertex);
					return slot;
				}
				else {
					// no resource available now, so queue the request
					if (queueIfNoResource) {
						CompletableFuture<LogicalSlot> future = new CompletableFuture<>();
						this.taskQueue.add(new QueuedTask(task, future));
						return future;
					}
					else if (forceExternalLocation) {
						String hosts = getHostnamesFromInstances(preferredLocations);
						throw new NoResourceAvailableException("Could not schedule task " + vertex
							+ " to any of the required hosts: " + hosts);
					}
					else {
						throw new NoResourceAvailableException(getNumberOfAvailableInstances(),
							getTotalNumberOfSlots(), getNumberOfAvailableSlots());
					}
				}
			}
		}
	}
}
