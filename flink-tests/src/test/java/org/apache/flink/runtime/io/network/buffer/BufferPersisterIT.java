/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.runtime.io.network.buffer;

import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.api.common.accumulators.LongCounter;
import org.apache.flink.configuration.CheckpointingOptions;
import org.apache.flink.configuration.ConfigConstants;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.MemorySize;
import org.apache.flink.configuration.TaskManagerOptions;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.LocalStreamEnvironment;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;
import org.apache.flink.util.TestLogger;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.rules.Timeout;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;
import javax.management.AttributeNotFoundException;
import javax.management.InstanceNotFoundException;
import javax.management.MBeanException;
import javax.management.MBeanServer;
import javax.management.MalformedObjectNameException;
import javax.management.ObjectName;
import javax.management.ReflectionException;

import java.io.File;
import java.lang.management.ManagementFactory;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * Tests that a buffer persister indeed spills the required data onto disk.
 */
public class BufferPersisterIT extends TestLogger {

	private static final Logger LOG = LoggerFactory.getLogger(BufferPersisterIT.class);

	@Rule
	public TemporaryFolder temporaryFolder = new TemporaryFolder();

	private static final MBeanServer mBeanServer = ManagementFactory.getPlatformMBeanServer();

	//	@Rule
	public final Timeout timeout = Timeout.builder()
		.withTimeout(30, TimeUnit.SECONDS)
		.withLookingForStuckThread(true)
		.build();

	@Test
	public void testSimplePersist() throws Exception {
		final File persistDir = temporaryFolder.newFolder();
		StreamExecutionEnvironment env = createEnv(persistDir, 1);

		createDAG(env, 100);
		final JobExecutionResult executionResult = env.execute();

		long writtenBytes = executionResult.getAccumulatorResult("writtenBytes");
		long persistedBytes = executionResult.getAccumulatorResult("persistedBytes");
		long numCheckpoints = getMetricValue("*numberOfCompletedCheckpoints*:*");
		assertTrue(persistedBytes > 0);
		assertTrue(writtenBytes >= persistedBytes);
		assertEquals(writtenBytes, persistedBytes, writtenBytes / numCheckpoints);
	}

	@Test
	public void testParallelPersist() throws Exception {
		final File persistDir = temporaryFolder.newFolder();
		StreamExecutionEnvironment env = createEnv(persistDir, 2);

		createDAG(env, 100);
		final JobExecutionResult executionResult = env.execute();

		long writtenBytes = executionResult.getAccumulatorResult("writtenBytes");
		long persistedBytes = executionResult.getAccumulatorResult("persistedBytes");
		long numCheckpoints = getMetricValue("*numberOfCompletedCheckpoints*:*");
		assertTrue(persistedBytes > 0);
		assertTrue(writtenBytes >= persistedBytes);
		assertEquals(writtenBytes, persistedBytes, writtenBytes / numCheckpoints);
	}

	@Test
	public void testMoreParallelPersist() throws Exception {
		final File persistDir = temporaryFolder.newFolder();
		StreamExecutionEnvironment env = createEnv(persistDir, 4);

		createDAG(env, 100);
		final JobExecutionResult executionResult = env.execute();

		long writtenBytes = executionResult.getAccumulatorResult("writtenBytes");
		long persistedBytes = executionResult.getAccumulatorResult("persistedBytes");
		long numCheckpoints = getMetricValue("*numberOfCompletedCheckpoints*:*");
		assertTrue(persistedBytes > 0);
		assertTrue(writtenBytes >= persistedBytes);
		assertEquals(writtenBytes, persistedBytes, writtenBytes / numCheckpoints);
	}

	@Test
	public void testMassivelyParallelPersist() throws Exception {
		final File persistDir = temporaryFolder.newFolder();
		StreamExecutionEnvironment env = createEnv(persistDir, 20);

		createDAG(env, 100);
		final JobExecutionResult executionResult = env.execute();

		long writtenBytes = executionResult.getAccumulatorResult("writtenBytes");
		long persistedBytes = executionResult.getAccumulatorResult("persistedBytes");
		long numCheckpoints = getMetricValue("*numberOfCompletedCheckpoints*:*");
		assertTrue(persistedBytes > 0);
		assertTrue(writtenBytes >= persistedBytes);
		assertEquals(writtenBytes, persistedBytes, writtenBytes / numCheckpoints);
	}

	@Nonnull
	private static LocalStreamEnvironment createEnv(final File persistDir, final int parallelism) {
		Configuration conf = new Configuration();
		conf.setInteger(TaskManagerOptions.NUM_TASK_SLOTS, 3);
		conf.setFloat(TaskManagerOptions.NETWORK_MEMORY_FRACTION, .8f);
		conf.set(TaskManagerOptions.NETWORK_MEMORY_MIN, MemorySize.ofMebiBytes(1000 / parallelism));
		conf.setInteger(ConfigConstants.LOCAL_NUMBER_TASK_MANAGER, parallelism);
		conf.setString(CheckpointingOptions.PERSIST_LOCATION_CONFIG, persistDir.toURI().toString());
		conf.setString("metrics.reporter.jmx.factory.class", "org.apache.flink.metrics.jmx.JMXReporterFactory");
		final LocalStreamEnvironment env = StreamExecutionEnvironment.createLocalEnvironment(parallelism, conf);
		env.enableCheckpointing(100);
		return env;
	}

	private void createDAG(
			final StreamExecutionEnvironment env,
			final long minCheckpoints) {
		final SingleOutputStreamOperator<Integer> source = env.addSource(new IntegerSource(minCheckpoints));
		final SingleOutputStreamOperator<Integer> transform = source.shuffle().map(i -> 2 * i);
		transform.shuffle().addSink(new CountingSink<>());
	}

	private static long getMetricCount(String type) {
		return getMetric(type, "Count");
	}

	private static long getMetric(String type, String count) {
		try {
			Set<ObjectName> offsetMetrics = mBeanServer.queryNames(new ObjectName(type), null);
			long sum = 0L;
			for (ObjectName name : offsetMetrics) {
				sum += (long) mBeanServer.getAttribute(name, count);
			}
			return sum;
		} catch (MalformedObjectNameException | MBeanException | ReflectionException | InstanceNotFoundException | AttributeNotFoundException e) {
			e.printStackTrace();
			return 0;
		}
	}

	private static long getMetricValue(String type) {
		return getMetric(type, "Value");
	}

	private static class IntegerSource extends RichParallelSourceFunction<Integer> {

		private final long minCheckpoints;
		private volatile boolean running = true;

		public IntegerSource(final long minCheckpoints) {
			this.minCheckpoints = minCheckpoints;
		}

		private LongCounter persistedBytes = new LongCounter();
		private LongCounter writtenBytes = new LongCounter();

		@Override
		public void open(Configuration parameters) throws Exception {
			super.open(parameters);
			getRuntimeContext().addAccumulator("persistedBytes", persistedBytes);
			getRuntimeContext().addAccumulator("writtenBytes", writtenBytes);
		}

		@Override
		public void close() throws Exception {
			super.close();
			persistedBytes.add(getMetricCount("*persistedBytes*:*,subtask_index=" + getRuntimeContext().getIndexOfThisSubtask()));
			writtenBytes.add(getMetricCount("*writtenBytes*:*,subtask_index=" + getRuntimeContext().getIndexOfThisSubtask()));
		}

		@Override
		public void run(SourceContext<Integer> ctx) throws Exception {
			int counter = 0;
			while (running) {
				ctx.collect(counter++);

				if (getMetricValue("*numberOfCompletedCheckpoints*:*") >= minCheckpoints) {
					cancel();
				}
			}

			// wait for all instances to finish, such that checkpoints are still processed
			Thread.sleep(1000);
		}

		@Override
		public void cancel() {
			running = false;
		}
	}

	private static class CountingSink<T> extends RichSinkFunction<T> {
		private LongCounter counter = new LongCounter();

		@Override
		public void open(Configuration parameters) throws Exception {
			super.open(parameters);
			getRuntimeContext().addAccumulator("outputs", counter);
		}

		@Override
		public void invoke(T value, Context context) throws Exception {
			counter.add(1);
			Thread.sleep(1);
		}
	}
}
