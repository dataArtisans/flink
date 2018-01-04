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
import javax.annotation.concurrent.ThreadSafe;

import java.nio.ByteBuffer;

import static org.apache.flink.util.Preconditions.checkNotNull;
import static org.apache.flink.util.Preconditions.checkState;

/**
 * Not thread safe class for filling in the content of the {@link MemorySegment}. Once writing to the builder
 * is complete, {@link Buffer} instance can be built and shared across multiple threads.
 */
@NotThreadSafe
public class BufferBuilder {
	private final MemorySegment memorySegment;

	private final BufferRecycler recycler;

	private final SettablePositionMarker positionMarker = new SettablePositionMarker();

	private int position = 0; // cache for positionMarker value, it allows us to avoid reading from positionMarker

	private boolean built = false;

	public BufferBuilder(MemorySegment memorySegment, BufferRecycler recycler) {
		this.memorySegment = checkNotNull(memorySegment);
		this.recycler = checkNotNull(recycler);
	}

	/**
	 * @return number of copied bytes
	 */
	public int append(ByteBuffer source) {
		checkState(!built);

		int needed = source.remaining();
		int available = limit() - position;
		int toCopy = Math.min(needed, available);

		memorySegment.put(position, source, toCopy);
		position += toCopy;
		positionMarker.set(position);
		return toCopy;
	}

	public boolean isFull() {
		checkState(position <= limit());
		return position == limit();
	}

	public boolean isEmpty() {
		return position == 0;
	}

	public int getPosition() {
		return position;
	}

	MemorySegment getMemorySegment() {
		return memorySegment;
	}

	BufferRecycler getRecycler() {
		return recycler;
	}

	PositionMarker getPositionMarker() {
		return positionMarker;
	}

	private int limit() {
		return memorySegment.size();
	}

	@ThreadSafe
	interface PositionMarker {
		int get();
	}

	private static class SettablePositionMarker implements PositionMarker {
		private volatile int position = 0;

		@Override
		public int get() {
			return position;
		}

		public void set(int newPosition) {
			position = newPosition;
		}
	}
}
