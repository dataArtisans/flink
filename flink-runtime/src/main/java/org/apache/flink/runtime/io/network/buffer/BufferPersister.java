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

import org.apache.flink.annotation.Internal;
import org.apache.flink.core.fs.Path;
import org.apache.flink.core.fs.RecoverableFsDataOutputStream;
import org.apache.flink.core.fs.RecoverableWriter;
import org.apache.flink.core.memory.MemorySegment;
import org.apache.flink.core.memory.MemorySegmentFactory;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.io.IOException;
import java.util.ArrayDeque;
import java.util.Queue;
import java.util.concurrent.CompletableFuture;

import static org.apache.flink.util.Preconditions.checkState;

/**
 * The {@link BufferPersister} takes the buffers and events from a data stream and persists them
 * asynchronously using {@link RecoverableFsDataOutputStream}.
 */
@Internal
public class BufferPersister implements AutoCloseable {

	private final Channel[] channels;
	private final Writer writer;

	public BufferPersister(
			RecoverableWriter recoverableWriter,
			Path recoverableWriterBasePath,
			int numberOfChannels) throws IOException {
		writer = new Writer(recoverableWriter, recoverableWriterBasePath);
		channels = new Channel[numberOfChannels];
		for (int i = 0; i < channels.length; i++) {
			channels[i] = new Channel();
		}
		writer.start();
	}

	public void add(BufferConsumer bufferConsumer, int channelId) {
		// do not spill the currently being added bufferConsumer, as it's empty
		spill(channels[channelId]);
		channels[channelId].pendingBuffers.add(bufferConsumer);
	}

	public CompletableFuture<?> persist() throws IOException {
		flushAll();
		return writer.persist();
	}

	@Override
	public void close() throws IOException, InterruptedException {
		writer.close();

		releaseMemory();
	}

	private void releaseMemory() {
		for (Channel channel : channels) {
			while (!channel.pendingBuffers.isEmpty()) {
				channel.pendingBuffers.poll().close();
			}
		}
	}

	public long getPendingBytes() {
		throw new UnsupportedOperationException("We should implement some metrics");
	}

	public void flushAll() {
		for (Channel channel : channels) {
			spill(channel);
		}
	}

	public void flush(int channelId) {
		spill(channels[channelId]);
	}

	/**
	 * @return true if moved enqueued some data
	 */
	private boolean spill(Channel channel) {
		boolean writtenSomething = false;
		while (!channel.pendingBuffers.isEmpty()) {
			BufferConsumer bufferConsumer = channel.pendingBuffers.peek();
			Buffer buffer = bufferConsumer.build();
			if (buffer.readableBytes() > 0) {
				writer.add(buffer);
				writtenSomething = true;
			}
			if (bufferConsumer.isFinished()) {
				bufferConsumer.close();
				channel.pendingBuffers.pop();
			}
			else {
				break;
			}
		}
		return writtenSomething;
	}

	private static class Channel {
		ArrayDeque<BufferConsumer> pendingBuffers = new ArrayDeque<>();
	}

	private static class Writer extends Thread implements AutoCloseable {
		private static final Logger LOG = LoggerFactory.getLogger(Writer.class);
		private static final Buffer PERSIST_MARKER = new NetworkBuffer(MemorySegmentFactory.allocateUnpooledSegment(42), memorySegment -> {});

		private volatile boolean running = true;

		private final Queue<Buffer> handover = new ArrayDeque<>();
		private final RecoverableWriter recoverableWriter;
		private final Path recoverableWriterBasePath;

		@Nullable
		private Throwable asyncException;
		private int partId;
		private CompletableFuture<?> persistFuture = CompletableFuture.completedFuture(null);
		private RecoverableFsDataOutputStream currentOutputStream;

		public Writer(RecoverableWriter recoverableWriter, Path recoverableWriterBasePath) throws IOException {
			this.recoverableWriter = recoverableWriter;
			this.recoverableWriterBasePath = recoverableWriterBasePath;

			openNewOutputStream();
		}

		public synchronized void add(Buffer buffer) {
			checkErroneousUnsafe();
			boolean wasEmpty = handover.isEmpty();
			handover.add(buffer);
			if (wasEmpty) {
				notify();
			}
		}

		public synchronized CompletableFuture<?> persist() throws IOException {
			checkErroneousUnsafe();
			checkState(persistFuture.isDone(), "TODO support multiple pending persist requests (multiple ongoing checkpoints?)");
			if (persistFuture.isDone()) {
				persistFuture = new CompletableFuture<>();
			}
			add(PERSIST_MARKER);

			return persistFuture;
		}

		public synchronized void checkErroneous() {
			checkErroneousUnsafe();
		}

		@Override
		public void run() {
			try {
				while (running) {
					write(get());
				}
			}
			catch (Throwable t) {
				synchronized (this) {
					if (running) {
						asyncException = t;
					}
					if (!persistFuture.isDone()) {
						persistFuture.completeExceptionally(t);
					}
				}
				LOG.error("unhandled exception in the Writer", t);
			}
		}

		private void write(Buffer buffer) throws IOException {
			try {
				int offset = buffer.getMemorySegmentOffset();
				MemorySegment segment = buffer.getMemorySegment();
				int numBytes = buffer.getSize();

				currentOutputStream.write(segment.getArray(), offset, numBytes);
			}
			finally {
				buffer.recycleBuffer();
			}
		}

		private synchronized Buffer get() throws InterruptedException, IOException {
			while (handover.isEmpty()) {
				wait();
			}
			Buffer buffer = handover.poll();
			if (buffer == PERSIST_MARKER) {
				currentOutputStream.closeForCommit().commit();
				openNewOutputStream();
				persistFuture.complete(null);
				return get();
			}

			return buffer;
		}

		@Override
		public void close() throws InterruptedException, IOException {
			try {
				checkErroneous();
				running = false;
				interrupt();
				join();
			}
			finally {
				currentOutputStream.close();
			}
		}

		private void openNewOutputStream() throws IOException {
			currentOutputStream = recoverableWriter.open(assemblePartFilePath());
		}

		private Path assemblePartFilePath() {
			return new Path(recoverableWriterBasePath, "part-file." + partId++);
		}

		private void checkErroneousUnsafe() {
			if (asyncException != null) {
				throw new RuntimeException(asyncException);
			}
		}
	}
}
