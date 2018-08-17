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

package org.apache.flink.configuration;

import org.apache.flink.annotation.PublicEvolving;

import static org.apache.flink.configuration.ConfigOptions.key;

/**
 * Configuration options for the Optimisation model.
 */
@PublicEvolving
public class OptimisationModelOptions {

	public static final ConfigOption<Double> NETWORK_COST =
		key("optimisation-model.network-cost-weight")
			.defaultValue(0.5d)
			.withDescription("Weight of network cost. execution_speed_weight = 1 - network_cost_weight");

	public static final ConfigOption<Boolean> GEO_ENABLE_SLOT_SHARING =
		key("optimisation-model.is-slot-sharing-enabled")
			.defaultValue(false)
			.withDescription("Enable slot sharing when geo scheduling");

	// ---------------------------------------------------------------------------------------------

	private OptimisationModelOptions() {
		throw new IllegalAccessError();
	}
}
