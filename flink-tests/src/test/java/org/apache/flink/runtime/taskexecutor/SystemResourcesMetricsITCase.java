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

package org.apache.flink.runtime.taskexecutor;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.metrics.Gauge;
import org.apache.flink.metrics.MetricConfig;
import org.apache.flink.metrics.reporter.AbstractReporter;
import org.apache.flink.test.util.MiniClusterResource;
import org.apache.flink.test.util.MiniClusterResource.MiniClusterResourceConfiguration;

import org.junit.ClassRule;
import org.junit.Test;

import java.util.Collection;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import static org.apache.flink.configuration.MetricOptions.REPORTERS_LIST;
import static org.apache.flink.configuration.TaskManagerOptions.ADDITIONAL_LOGGING;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

/**
 * Integration tests for proper initialization of the system resource metrics in the {@link TaskManagerRunner}.
 */
public class SystemResourcesMetricsITCase {

	@ClassRule
	public static final MiniClusterResource MINI_CLUSTER_RESOURCE = new MiniClusterResource(
		new MiniClusterResourceConfiguration(getConfiguration(), 1, 1));

	private static Configuration getConfiguration() {
		Configuration configuration = new Configuration();
		configuration.setBoolean(ADDITIONAL_LOGGING, true);
		configuration.setString(REPORTERS_LIST, "test_reporter");
		configuration.setString("metrics.reporter.test_reporter.class", TestReporter.class.getName());
		return configuration;
	}

	@Test
	public void startTaskManagerAndCheckForRegisteredSystemMetrics() throws Exception {
		assertEquals(1, TestReporter.OPENED_REPORTERS.size());
		TestReporter reporter = TestReporter.OPENED_REPORTERS.iterator().next();

		String[] expectedGauges = new String[] {
			"System.CPU.Idle",
			"System.CPU.Sys",
			"System.CPU.User",
			"System.CPU.IOWait",
			"System.CPU.Irq",
			"System.CPU.SoftIrq",
			"System.CPU.Nice",
			"System.Memory.Available",
			"System.Memory.Total",
			"System.Swap.Used",
			"System.Swap.Total",
			"ReceiveRate",
			"SendRate"
		};

		Collection<String> gaugeNames = reporter.getGauges().values();

		for (String expectedGauge : expectedGauges) {
			boolean found = false;
			for (String gaugeName : gaugeNames) {
				if (gaugeName.endsWith(expectedGauge)) {
					found = true;
				}
			}
			if (!found) {
				fail(String.format("Failed to find gauge [%s] in registered gauges [%s]", expectedGauge, gaugeNames));
			}
		}
	}

	/**
	 * Test metric reporter that exposes registered metrics.
	 */
	public static final class TestReporter extends AbstractReporter {
		public static final Set<TestReporter> OPENED_REPORTERS = ConcurrentHashMap.newKeySet();

		@Override
		public String filterCharacters(String input) {
			return input;
		}

		@Override
		public void open(MetricConfig config) {
			OPENED_REPORTERS.add(this);
		}

		@Override
		public void close() {
			OPENED_REPORTERS.remove(this);
		}

		public Map<Gauge<?>, String> getGauges() {
			return gauges;
		}
	}
}
