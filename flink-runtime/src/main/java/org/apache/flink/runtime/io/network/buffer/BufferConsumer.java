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

package org.apache.flink.runtime.io.network.buffer;

import org.apache.flink.core.memory.MemorySegment;

import javax.annotation.concurrent.NotThreadSafe;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * Not thread safe class for producing {@link Buffer}. It reads data written by {@link BufferBuilder}.
 * Although it is not thread safe and can be used only by one single thread, this thread can be different then the
 * thread using/writing to {@link BufferBuilder}. Pattern here is simple: one thread writes data to
 * {@link BufferBuilder} and there can be a different thread reading from it using {@link BufferConsumer}.
 */
@NotThreadSafe
public class BufferConsumer implements AutoCloseable {
	private final Buffer buffer;

	private final BufferBuilder.PositionMarker currentWriterPosition;

	private final int initialWriterPosition;

	private int lastWriterPosition = 0;

	private int currentReaderPosition = 0;

	public static BufferConsumer consumerFor(BufferBuilder bufferBuilder) {
		return new BufferConsumer(
			bufferBuilder.getMemorySegment(),
			bufferBuilder.getRecycler(),
			bufferBuilder.getPositionMarker(),
			bufferBuilder.getPosition());
	}

	public BufferConsumer(
			MemorySegment memorySegment,
			BufferRecycler recycler,
			BufferBuilder.PositionMarker currentWriterPosition,
			int initialWriterPosition) {

		this.buffer = new NetworkBuffer(checkNotNull(memorySegment), checkNotNull(recycler), true);
		this.currentWriterPosition = checkNotNull(currentWriterPosition);
		this.lastWriterPosition = initialWriterPosition;
		this.initialWriterPosition = initialWriterPosition;
	}

	public boolean isFinished() {
		return lastWriterPosition == limit();
	}

	public Buffer build() {
		lastWriterPosition = currentWriterPosition.get();
		Buffer slice = buffer.readOnlySlice(currentReaderPosition, lastWriterPosition - currentReaderPosition);
		currentReaderPosition = lastWriterPosition;
		return slice.retainBuffer();
	}

	@Override
	public void close() {
		if (!buffer.isRecycled()) {
			buffer.recycleBuffer();
		}
	}

	public int getWrittenBytes() {
		return lastWriterPosition - initialWriterPosition;
	}

	private int limit() {
		return buffer.getMaxCapacity();
	}
}
