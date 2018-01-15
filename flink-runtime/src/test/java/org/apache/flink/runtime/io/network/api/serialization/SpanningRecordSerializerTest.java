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

package org.apache.flink.runtime.io.network.api.serialization;

import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataOutputView;
import org.apache.flink.core.memory.MemorySegment;
import org.apache.flink.testutils.serialization.types.SerializationTestType;
import org.apache.flink.testutils.serialization.types.SerializationTestTypeFactory;
import org.apache.flink.testutils.serialization.types.Util;

import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.util.Random;

import static org.apache.flink.runtime.io.network.buffer.BufferBuilderTestUtils.createBufferBuilder;

public class SpanningRecordSerializerTest {

	@Test
	public void testHasData() throws IOException {
		final int SEGMENT_SIZE = 16;

		final SpanningRecordSerializer<SerializationTestType> serializer = new SpanningRecordSerializer<SerializationTestType>();
		final SerializationTestType randomIntRecord = Util.randomRecord(SerializationTestTypeFactory.INT);

		Assert.assertFalse(serializer.hasData());

		serializer.addRecord(randomIntRecord);
		Assert.assertTrue(serializer.hasData());

		serializer.setNextBufferBuilder(createBufferBuilder(SEGMENT_SIZE));
		Assert.assertTrue(serializer.hasData());

		serializer.clear();
		Assert.assertFalse(serializer.hasData());

		serializer.setNextBufferBuilder(createBufferBuilder(SEGMENT_SIZE));

		serializer.addRecord(randomIntRecord);
		Assert.assertTrue(serializer.hasData());

		serializer.addRecord(randomIntRecord);
		Assert.assertTrue(serializer.hasData());
	}

	@Test
	public void testEmptyRecords() throws IOException {
		final int SEGMENT_SIZE = 11;

		final SpanningRecordSerializer<SerializationTestType> serializer = new SpanningRecordSerializer<SerializationTestType>();

		try {
			Assert.assertEquals(
				RecordSerializer.SerializationResult.FULL_RECORD,
				serializer.setNextBufferBuilder(createBufferBuilder(SEGMENT_SIZE)));
		} catch (IOException e) {
			e.printStackTrace();
		}

		SerializationTestType emptyRecord = new SerializationTestType() {
			@Override
			public SerializationTestType getRandom(Random rnd) {
				throw new UnsupportedOperationException();
			}

			@Override
			public int length() {
				throw new UnsupportedOperationException();
			}

			@Override
			public void write(DataOutputView out) {}

			@Override
			public void read(DataInputView in) {}

			@Override
			public int hashCode() {
				throw new UnsupportedOperationException();
			}

			@Override
			public boolean equals(Object obj) {
				throw new UnsupportedOperationException();
			}
		};

		RecordSerializer.SerializationResult result = serializer.addRecord(emptyRecord);
		Assert.assertEquals(RecordSerializer.SerializationResult.FULL_RECORD, result);

		result = serializer.addRecord(emptyRecord);
		Assert.assertEquals(RecordSerializer.SerializationResult.FULL_RECORD, result);

		result = serializer.addRecord(emptyRecord);
		Assert.assertEquals(RecordSerializer.SerializationResult.PARTIAL_RECORD_MEMORY_SEGMENT_FULL, result);

		result = serializer.setNextBufferBuilder(createBufferBuilder(SEGMENT_SIZE));
		Assert.assertEquals(RecordSerializer.SerializationResult.FULL_RECORD, result);
	}

	@Test
	public void testIntRecordsSpanningMultipleSegments() throws Exception {
		final int SEGMENT_SIZE = 1;
		final int NUM_VALUES = 10;

		test(Util.randomRecords(NUM_VALUES, SerializationTestTypeFactory.INT), SEGMENT_SIZE);
	}

	@Test
	public void testIntRecordsWithAlignedSegments() throws Exception {
		final int SEGMENT_SIZE = 64;
		final int NUM_VALUES = 64;

		test(Util.randomRecords(NUM_VALUES, SerializationTestTypeFactory.INT), SEGMENT_SIZE);
	}

	@Test
	public void testIntRecordsWithUnalignedSegments() throws Exception {
		final int SEGMENT_SIZE = 31;
		final int NUM_VALUES = 248; // least common multiple => last record should align

		test(Util.randomRecords(NUM_VALUES, SerializationTestTypeFactory.INT), SEGMENT_SIZE);
	}

	@Test
	public void testRandomRecords() throws Exception {
		final int SEGMENT_SIZE = 127;
		final int NUM_VALUES = 100000;

		test(Util.randomRecords(NUM_VALUES), SEGMENT_SIZE);
	}

	// -----------------------------------------------------------------------------------------------------------------

	/**
	 * Iterates over the provided records and tests whether the {@link SpanningRecordSerializer} returns the expected
	 * {@link RecordSerializer.SerializationResult} values.
	 * <p>
	 * Only a single {@link MemorySegment} will be allocated.
	 *
	 * @param records records to test
	 * @param segmentSize size for the {@link MemorySegment}
	 */
	private void test(Util.MockRecords records, int segmentSize) throws Exception {
		final int SERIALIZATION_OVERHEAD = 4; // length encoding

		final SpanningRecordSerializer<SerializationTestType> serializer = new SpanningRecordSerializer<SerializationTestType>();

		// -------------------------------------------------------------------------------------------------------------

		serializer.setNextBufferBuilder(createBufferBuilder(segmentSize));

		int numBytes = 0;
		for (SerializationTestType record : records) {
			RecordSerializer.SerializationResult result = serializer.addRecord(record);
			numBytes += record.length() + SERIALIZATION_OVERHEAD;

			if (numBytes < segmentSize) {
				Assert.assertEquals(RecordSerializer.SerializationResult.FULL_RECORD, result);
			} else if (numBytes == segmentSize) {
				Assert.assertEquals(RecordSerializer.SerializationResult.FULL_RECORD_MEMORY_SEGMENT_FULL, result);
				serializer.setNextBufferBuilder(createBufferBuilder(segmentSize));
				numBytes = 0;
			} else {
				Assert.assertEquals(RecordSerializer.SerializationResult.PARTIAL_RECORD_MEMORY_SEGMENT_FULL, result);

				while (result.isFullBuffer()) {
					numBytes -= segmentSize;

					result = serializer.setNextBufferBuilder(createBufferBuilder(segmentSize));
				}
			}
		}
	}
}
