/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.flink.test.checkpointing;

import org.apache.flink.api.common.JobID;
import org.apache.flink.client.program.MiniClusterClient;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.runtime.jobmaster.JobResult;
import org.apache.flink.runtime.minicluster.MiniCluster;
import org.apache.flink.runtime.minicluster.MiniClusterConfiguration;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.checkpoint.ListCheckpointed;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.streaming.api.functions.source.ParallelSourceFunction;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;
import org.apache.flink.test.util.IdentityMapFunction;
import org.apache.flink.util.TestLogger;

import org.junit.Test;

import java.util.Collections;
import java.util.List;
import java.util.concurrent.CompletableFuture;

import static org.junit.Assert.assertFalse;

/**
 * Integration test for performing the unaligned checkpoint.
 */
public class UnalignedCheckpointITCase extends TestLogger {

	private static final JobID TEST_JOB_ID = new JobID();
	private static final int NUM_TASK_MANAGERS = 2;
	private static final int NUM_SLOTS_PER_TASK_MANAGER = 2;

	@Test
	public void testUnalignedCheckpoint() throws Exception {
		final Configuration configuration = new Configuration();
		final MiniClusterConfiguration miniClusterConfiguration = new MiniClusterConfiguration.Builder()
			.setConfiguration(configuration)
			.setNumTaskManagers(NUM_TASK_MANAGERS)
			.setNumSlotsPerTaskManager(NUM_SLOTS_PER_TASK_MANAGER)
			.build();

		final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		env.setParallelism(1);
		env.enableCheckpointing(1000, CheckpointingMode.UNALIGNED);

		env.addSource(new FiniteIntegerSource(10000))
			.slotSharingGroup("sourceGroup")
			.map(new IdentityMapFunction<>())
			.slotSharingGroup("mapGroup")
			.addSink(new DiscardingSink())
			.slotSharingGroup("sinkGroup");

		try (final MiniCluster miniCluster = new MiniCluster(miniClusterConfiguration)) {
			miniCluster.start();

			MiniClusterClient miniClusterClient = new MiniClusterClient(configuration, miniCluster);
			final JobGraph jobGraph = env.getStreamGraph().getJobGraph(TEST_JOB_ID);
			miniClusterClient.submitJob(jobGraph).get();

			// wait for the submission to succeed
			CompletableFuture<JobResult> resultFuture = miniClusterClient.requestJobResult(TEST_JOB_ID);
			assertFalse(resultFuture.get().getSerializedThrowable().isPresent());
		}
	}

	/**
	 * The source for emitting the limit number of integer records.
	 */
	private static final class FiniteIntegerSource extends RichSourceFunction<Integer>
		implements ParallelSourceFunction<Integer>, ListCheckpointed<Integer> {

		private static final long serialVersionUID = 1L;

		private final long numElements;

		private int counter;

		private volatile boolean running = true;

		FiniteIntegerSource(long numElements) {
			this.numElements = numElements;
		}

		@Override
		public void run(SourceContext<Integer> ctx) throws Exception {
			while (running && counter < numElements) {
				synchronized (ctx.getCheckpointLock()) {
					ctx.collect(counter++);
					Thread.sleep(1);
				}
			}
		}

		@Override
		public List<Integer> snapshotState(long checkpointId, long timestamp) {
			return Collections.singletonList(counter);
		}

		@Override
		public void restoreState(List<Integer> state) {
		}

		@Override
		public void cancel() {
			running = false;
		}
	}

	/**
	 * The sink for discarding the records directly.
	 */
	private static final class DiscardingSink implements SinkFunction<Integer> {

		@Override
		public void invoke(Integer value, Context context) throws Exception {
			Thread.sleep(1);
		}
	}
}
