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

import org.apache.flink.core.memory.MemorySegmentFactory;

import org.junit.Test;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.IntBuffer;
import java.util.ArrayList;

import static org.apache.flink.runtime.io.network.buffer.BufferBuilderTestUtils.buildSingleBuffer;
import static org.apache.flink.runtime.io.network.buffer.BufferConsumer.consumerFor;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/**
 * Tests for {@link BufferBuilder}.
 */
public class BufferBuilderAndConsumerTest {
	private static final int BUFFER_SIZE = 10 * Integer.BYTES;

	@Test
	public void referenceCounting() {
		BufferBuilder bufferBuilder = createBufferBuilder();
		BufferConsumer bufferConsumer = consumerFor(bufferBuilder);

		assertEquals(3 * Integer.BYTES, bufferBuilder.append(toByteBuffer(1, 2, 3)));

		Buffer buffer = bufferConsumer.build();
		assertFalse(buffer.isRecycled());
		buffer.recycleBuffer();
		assertFalse(buffer.isRecycled());
		bufferConsumer.close();
		assertTrue(buffer.isRecycled());
	}

	@Test
	public void append() {
		BufferBuilder bufferBuilder = createBufferBuilder();
		BufferConsumer bufferConsumer = consumerFor(bufferBuilder);

		int[] intsToWrite = new int[] {0, 1, 2, 3, 42};
		ByteBuffer bytesToWrite = toByteBuffer(intsToWrite);

		assertEquals(bytesToWrite.limit(), bufferBuilder.append(bytesToWrite));

		assertEquals(bytesToWrite.limit(), bytesToWrite.position());
		assertFalse(bufferBuilder.isFull());
		assertContent(bufferConsumer, intsToWrite);
	}

	@Test
	public void multipleAppends() {
		BufferBuilder bufferBuilder = createBufferBuilder();
		BufferConsumer bufferConsumer = consumerFor(bufferBuilder);

		bufferBuilder.append(toByteBuffer(0, 1));
		bufferBuilder.append(toByteBuffer(2));
		bufferBuilder.append(toByteBuffer(3, 42));

		assertContent(bufferConsumer, 0, 1, 2, 3, 42);
	}

	@Test
	public void appendOverSize() {
		BufferBuilder bufferBuilder = createBufferBuilder();
		BufferConsumer bufferConsumer = consumerFor(bufferBuilder);
		ByteBuffer bytesToWrite = toByteBuffer(0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 42);

		assertEquals(BUFFER_SIZE, bufferBuilder.append(bytesToWrite));

		assertTrue(bufferBuilder.isFull());
		assertContent(bufferConsumer, 0, 1, 2, 3, 4, 5, 6, 7, 8, 9);

		bufferBuilder = createBufferBuilder();
		bufferConsumer = consumerFor(bufferBuilder);
		assertEquals(Integer.BYTES, bufferBuilder.append(bytesToWrite));

		assertFalse(bufferBuilder.isFull());
		assertContent(bufferConsumer, 42);
	}

	@Test
	public void buildEmptyBuffer() {
		Buffer buffer = buildSingleBuffer(createBufferBuilder());
		assertEquals(0, buffer.getSize());
		assertContent(buffer);
	}

	@Test
	public void buildingBufferMultipleTimes() {
		BufferBuilder bufferBuilder = createBufferBuilder();
		try (BufferConsumer bufferConsumer = consumerFor(bufferBuilder)) {
			bufferBuilder.append(toByteBuffer(0, 1));
			bufferBuilder.append(toByteBuffer(2));

			assertContent(bufferConsumer, 0, 1, 2);

			bufferBuilder.append(toByteBuffer(3, 42));
			bufferBuilder.append(toByteBuffer(44));

			assertContent(bufferConsumer, 3, 42, 44);

			ArrayList<Integer> originalValues = new ArrayList<>();
			while (!bufferBuilder.isFull()) {
				bufferBuilder.append(toByteBuffer(1337));
				originalValues.add(1337);
			}

			assertContent(bufferConsumer, originalValues.stream().mapToInt(i->i).toArray());
			assertTrue(bufferConsumer.isFinished());
		}
	}

	private static ByteBuffer toByteBuffer(int... data) {
		ByteBuffer byteBuffer = ByteBuffer.allocate(data.length * Integer.BYTES);
		byteBuffer.asIntBuffer().put(data);
		return byteBuffer;
	}

	private static void assertContent(BufferConsumer actualConsumer, int... expected) {
		assertFalse(actualConsumer.isFinished());
		Buffer buffer = actualConsumer.build();
		assertFalse(buffer.isRecycled());
		assertContent(buffer, expected);
		assertEquals(expected.length * Integer.BYTES, buffer.getSize());
		buffer.recycleBuffer();
	}

	private static void assertContent(Buffer actualBuffer, int... expected) {
		IntBuffer actualIntBuffer = actualBuffer.getNioBufferReadable().order(ByteOrder.BIG_ENDIAN).asIntBuffer();
		int[] actual = new int[expected.length];
		actualIntBuffer.get(actual);
		for (int i = 0; i < expected.length; i++) {
			assertEquals(expected[i], actual[i]);
		}

		assertEquals(FreeingBufferRecycler.INSTANCE, actualBuffer.getRecycler());
	}

	private static BufferBuilder createBufferBuilder() {
		return new BufferBuilder(MemorySegmentFactory.allocateUnpooledSegment(BUFFER_SIZE), FreeingBufferRecycler.INSTANCE);
	}
}
