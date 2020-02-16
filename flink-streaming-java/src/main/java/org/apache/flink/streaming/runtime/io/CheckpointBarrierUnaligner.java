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

package org.apache.flink.streaming.runtime.io;

import org.apache.flink.annotation.Internal;
import org.apache.flink.runtime.checkpoint.CheckpointMetaData;
import org.apache.flink.runtime.checkpoint.CheckpointOptions;
import org.apache.flink.runtime.io.network.api.CancelCheckpointMarker;
import org.apache.flink.runtime.io.network.api.CheckpointBarrier;
import org.apache.flink.runtime.jobgraph.tasks.AbstractInvokable;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

/**
 * {@link CheckpointBarrierUnaligner} is used for triggering checkpoint while reading the first barrier
 * and keeping track of the number of received barriers and consumed barriers.
 */
@Internal
public class CheckpointBarrierUnaligner extends CheckpointBarrierHandler {

	private static final Logger LOG = LoggerFactory.getLogger(CheckpointBarrierUnaligner.class);

	private final String taskName;

	/**
	 * The checkpoint id to guarantee that we would trigger only one checkpoint when reading the same barrier from
	 * different channels.
	 */
	private long currentCheckpointId = -1L;

	CheckpointBarrierUnaligner(
			int totalNumberOfInputChannels,
			String taskName,
			@Nullable AbstractInvokable toNotifyOnCheckpoint) {
		super(toNotifyOnCheckpoint);

		this.taskName = taskName;
	}

	@Override
	public void releaseBlocksAndResetBarriers() {
	}

	/**
	 * For unaligned checkpoint, it never blocks processing from the task aspect.
	 *
	 * <p>For PoC, we do not consider the possibility that the unaligned checkpoint would
	 * not perform due to the max configured unaligned checkpoint size.
	 */
	@Override
	public boolean isBlocked(int channelIndex) {
		return false;
	}

	/**
	 * We still need to trigger checkpoint while reading the first barrier from one channel, because this might happen
	 * earlier than the previous async trigger via mailbox by netty thread. And the {@link AbstractInvokable} has the
	 * deduplication logic to guarantee trigger checkpoint only once finally.
	 *
	 * <p>Note this is also suitable for the trigger case of local input channel.
	 */
	@Override
	public synchronized boolean processBarrier(CheckpointBarrier receivedBarrier, int channelIndex, long bufferedBytes) throws Exception {
		final long barrierId = receivedBarrier.getId();

		if (readBarrier(channelIndex, barrierId)) {
			notifyCheckpoint(receivedBarrier, bufferedBytes, 0);
		}
		return false;
	}

	@Override
	public boolean processCancellationBarrier(CancelCheckpointMarker cancelBarrier) {
		return false;
	}

	@Override
	public boolean processEndOfPartition() {
		return false;
	}

	@Override
	public long getLatestCheckpointId() {
		return currentCheckpointId;
	}

	@Override
	public long getAlignmentDurationNanos() {
		return 0;
	}

	@Override
	public String toString() {
		return String.format("%s: last checkpoint: %d", taskName, currentCheckpointId);
	}

	@Override
	public void checkpointSizeLimitExceeded(long maxBufferedBytes) {
	}

	@Override
	public synchronized void notifyBarrierReceived(CheckpointBarrier barrier, int channelIndex) {
		boolean isFirstReceivedBarrier = onBarrier(channelIndex, barrier.getId());

		if (isFirstReceivedBarrier) {
			triggerCheckpoint(barrier);
		}
	}

	/**
	 * Note that we make the assumption that there is only one checkpoint under going at a time. That means one channel
	 * would not receive a bigger checkpoint id than other channels during alignment.
	 */
	private boolean onBarrier(int channelIndex, long barrierId) {
		if (LOG.isDebugEnabled()) {
			LOG.debug("{}: Received barrier from channel {}.", taskName, channelIndex);
		}

		return isNewCheckpoint(barrierId);
	}

	private boolean readBarrier(int channelIndex, long barrierId) {
		if (LOG.isDebugEnabled()) {
			LOG.debug("{}: Read barrier from channel {}.", taskName, channelIndex);
		}

		return isNewCheckpoint(barrierId);
	}

	private boolean isNewCheckpoint(long barrierId) {
		boolean newCheckpoint = barrierId > currentCheckpointId;
		if (newCheckpoint) {
			currentCheckpointId = barrierId;
		}
		return newCheckpoint;
	}

	private void triggerCheckpoint(CheckpointBarrier barrier) {
		if (toNotifyOnCheckpoint != null) {
			toNotifyOnCheckpoint.triggerCheckpointAsync(
				new CheckpointMetaData(barrier.getId(), barrier.getTimestamp()),
				CheckpointOptions.forCheckpointWithDefaultLocation(),
				false);
		}
	}
}
