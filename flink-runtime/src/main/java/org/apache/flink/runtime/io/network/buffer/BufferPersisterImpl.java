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
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.core.fs.Path;
import org.apache.flink.core.fs.RecoverableFsDataOutputStream;
import org.apache.flink.core.fs.RecoverableWriter;
import org.apache.flink.core.memory.MemorySegment;
import org.apache.flink.core.memory.MemorySegmentFactory;
import org.apache.flink.metrics.Counter;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.io.IOException;
import java.util.ArrayDeque;
import java.util.Collection;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.CompletableFuture;

import static org.apache.flink.util.Preconditions.checkState;

/**
 * The {@link BufferPersisterImpl} takes the buffers and events from a data stream and persists them
 * asynchronously using {@link RecoverableFsDataOutputStream}.
 */
@Internal
public class BufferPersisterImpl implements BufferPersister {

	private final Channel[] channels;
	private final Writer writer;
	private final Thread writerThread;

	public BufferPersisterImpl(
			RecoverableWriter recoverableWriter,
			Path recoverableWriterBasePath,
			int numberOfChannels,
			Counter writtenBytes,
			Counter persistedBytes) throws IOException {
		writer = new Writer(recoverableWriter, recoverableWriterBasePath, writtenBytes, persistedBytes);
		channels = new Channel[numberOfChannels];
		for (int i = 0; i < channels.length; i++) {
			channels[i] = new Channel();
		}
		writerThread = new Thread(writer, "BufferPersisterWriter");
		writerThread.start();
	}

	@Override
	public synchronized void add(BufferConsumer bufferConsumer, int channelId) {
		// do not spill the currently being added bufferConsumer, as it's empty
		spill(channels[channelId]);
		channels[channelId].pendingBuffers.add(bufferConsumer.copy());
	}

	@Override
	public synchronized CompletableFuture<?> persist() {
		flushAll();
		return writer.persist();
	}

	@Override
	public synchronized void close() throws IOException, InterruptedException {
		writer.close();
		writerThread.interrupt();
		writerThread.join();

		releaseMemory();
	}

	private synchronized void releaseMemory() {
		for (Channel channel : channels) {
			while (!channel.pendingBuffers.isEmpty()) {
				channel.pendingBuffers.poll().close();
			}
		}
	}

	@Override
	public long getPendingBytes() {
		throw new UnsupportedOperationException("We should implement some metrics");
	}

	@Override
	public synchronized void flushAll() {
		for (Channel channel : channels) {
			spill(channel);
		}
	}

	@Override
	public synchronized void flush(int channelId) {
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
			} else {
				buffer.recycleBuffer();
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

	private static class Writer implements Runnable, AutoCloseable {
		private static final Logger LOG = LoggerFactory.getLogger(Writer.class);
		private final FileSystem fs;
		private final Counter writtenBytes;
		private final Counter persistedBytes;

		private volatile boolean running = true;

		private final Queue<Buffer> handover = new ArrayDeque<>();
		private final RecoverableWriter recoverableWriter;
		private final Path recoverableWriterBasePath;

		@Nullable
		private Throwable asyncException;
		private int partId;
		private Collection<PersistentMarkingBuffer> markingBuffers = new ArrayDeque<PersistentMarkingBuffer>();
		private RecoverableFsDataOutputStream currentOutputStream;
		private byte[] readBuffer = new byte[0];

		public Writer(
				RecoverableWriter recoverableWriter,
				Path recoverableWriterBasePath,
				Counter writtenBytes,
				Counter persistedBytes) throws IOException {
			this.recoverableWriter = recoverableWriter;
			this.recoverableWriterBasePath = recoverableWriterBasePath;
			fs = FileSystem.get(recoverableWriterBasePath.toUri());
			this.writtenBytes = writtenBytes;
			this.persistedBytes = persistedBytes;

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

		public synchronized CompletableFuture<?> persist() {
			checkErroneousUnsafe();
			final PersistentMarkingBuffer markingBuffer = new PersistentMarkingBuffer();
			markingBuffers.add(markingBuffer);
			add(markingBuffer);
			markingBuffer.getPersistFuture().whenComplete((result, e) -> markingBuffers.remove(markingBuffer));

			return markingBuffer.getPersistFuture();
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
					for (final PersistentMarkingBuffer markingBuffer : markingBuffers) {
						markingBuffer.getPersistFuture().completeExceptionally(t);
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

				if (readBuffer.length < numBytes) {
					readBuffer = new byte[numBytes];
				}
				segment.get(offset, readBuffer, 0, numBytes);
				currentOutputStream.write(readBuffer, 0, numBytes);
				writtenBytes.inc(numBytes);
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
			if (buffer instanceof PersistentMarkingBuffer) {
				final long pos = currentOutputStream.getPos();
				currentOutputStream.closeForCommit().commit();
				persistedBytes.inc(pos);
				final Path previousPart = assemblePartFilePath(partId - 2);
				// cleanup old part, this could be done asynchronously
				if (fs.exists(previousPart)) {
					fs.delete(previousPart, true);
				}
				openNewOutputStream();
				((PersistentMarkingBuffer) buffer).getPersistFuture().complete(null);
				return get();
			}

			return buffer;
		}

		@Override
		public void close() throws InterruptedException, IOException {
			try {
				running = false;
				checkErroneous();
			}
			finally {
				currentOutputStream.close();
			}
		}

		private void openNewOutputStream() throws IOException {
			currentOutputStream = recoverableWriter.open(assemblePartFilePath(partId++));
		}

		private Path assemblePartFilePath(int partId) {
			return new Path(recoverableWriterBasePath, "part-file." + partId);
		}

		private void checkErroneousUnsafe() {
			if (asyncException != null) {
				throw new RuntimeException(asyncException);
			}
		}

		private static class PersistentMarkingBuffer extends NetworkBuffer {
			private CompletableFuture<?> persistFuture = new CompletableFuture<>();

			public PersistentMarkingBuffer() {
				super(
					MemorySegmentFactory.allocateUnpooledSegment(42),
					memorySegment -> {});
			}

			public CompletableFuture<?> getPersistFuture() {
				return persistFuture;
			}
		}
	}
}
