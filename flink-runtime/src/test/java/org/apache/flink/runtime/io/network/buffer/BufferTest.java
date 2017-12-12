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
import org.apache.flink.core.memory.MemorySegmentFactory;
import org.apache.flink.runtime.io.network.netty.NettyBufferPool;

import org.apache.flink.shaded.netty4.io.netty.buffer.ByteBuf;

import org.junit.Assert;
import org.junit.Test;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;

/**
 * Tests for the {@link Buffer} class.
 */
public class BufferTest extends AbstractByteBufTest {

	/**
	 * Upper limit for the max size that is sufficient for all the tests.
	 */
	static final int MAX_CAPACITY_UPPER_BOUND = 64 * 1024 * 1024;

	static final NettyBufferPool NETTY_BUFFER_POOL = new NettyBufferPool(1);

	@Override
	protected ByteBuf newBuffer(int length, int maxCapacity) {
		final MemorySegment segment =
			MemorySegmentFactory
				.allocateUnpooledSegment(Math.min(maxCapacity, MAX_CAPACITY_UPPER_BOUND));

		NetworkBuffer buffer = new NetworkBuffer(segment, FreeingBufferRecycler.INSTANCE);
		buffer.capacity(length);
		buffer.setAllocator(NETTY_BUFFER_POOL);

		assertSame(ByteOrder.BIG_ENDIAN, buffer.order());
		assertEquals(0, buffer.readerIndex());
		assertEquals(0, buffer.writerIndex());
		return buffer;
	}

	@Test
	public void testSetGetSize() {
		final MemorySegment segment = MemorySegmentFactory.allocateUnpooledSegment(1024);

		NetworkBuffer buffer = new NetworkBuffer(segment, FreeingBufferRecycler.INSTANCE);
		Assert.assertEquals(segment.size(), buffer.getSize());
		Assert.assertEquals(segment.size(), buffer.maxCapacity());
		// writer index should not reflect size
		assertEquals(0, buffer.writerIndex());
		assertEquals(0, buffer.readerIndex());

		buffer = new NetworkBuffer(segment, FreeingBufferRecycler.INSTANCE, true, 10);
		Assert.assertEquals(segment.size(), buffer.getSize());
		Assert.assertEquals(segment.size(), buffer.maxCapacity());
		// writer index should not reflect size
		assertEquals(10, buffer.writerIndex());
		assertEquals(0, buffer.readerIndex());
	}

	@Test
	public void testgetNioBufferReadableThreadSafe() {
		final MemorySegment segment = MemorySegmentFactory.allocateUnpooledSegment(1024);

		NetworkBuffer buffer = new NetworkBuffer(segment, FreeingBufferRecycler.INSTANCE);

		ByteBuffer buf1 = buffer.getNioBufferReadable();
		ByteBuffer buf2 = buffer.getNioBufferReadable();

		assertNotNull(buf1);
		assertNotNull(buf2);

		assertTrue("Repeated call to getNioBuffer() returns the same nio buffer", buf1 != buf2);
	}

	@Test
	public void testgetNioBufferThreadSafe() {
		final MemorySegment segment = MemorySegmentFactory.allocateUnpooledSegment(1024);

		NetworkBuffer buffer = new NetworkBuffer(segment, FreeingBufferRecycler.INSTANCE);

		ByteBuffer buf1 = buffer.getNioBuffer(0, 10);
		ByteBuffer buf2 = buffer.getNioBuffer(0, 10);

		assertNotNull(buf1);
		assertNotNull(buf2);

		assertTrue("Repeated call to getNioBuffer(int, int) returns the same nio buffer", buf1 != buf2);
	}
}
