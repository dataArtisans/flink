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

import org.apache.flink.runtime.io.network.serialization.types.LargeObjectType;
import org.apache.flink.testutils.serialization.types.IntType;
import org.apache.flink.testutils.serialization.types.SerializationTestType;

import org.junit.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;

public class LargeRecordsTest {
	private final static int NUM_RECORDS = 99;
	private final static int SEGMENT_SIZE = 32 * 1024;

	@Test
	public void testHandleMixedLargeRecords() throws Exception {
		RecordDeserializer<SerializationTestType> deserializer = new AdaptiveSpanningRecordDeserializer<>();
		testHandleMixedLargeRecords(deserializer);
	}

	@Test
	public void testHandleMixedLargeRecordsSpillingAdaptiveSerializer() throws Exception {
		RecordDeserializer<SerializationTestType> deserializer = new SpillingAdaptiveSpanningRecordDeserializer<>(
				new String[]{System.getProperty("java.io.tmpdir")});
		testHandleMixedLargeRecords(deserializer);
	}

	private void testHandleMixedLargeRecords(RecordDeserializer<SerializationTestType> deserializer) throws Exception {
		List<SerializationTestType> originalRecords = new ArrayList<>((NUM_RECORDS + 1) / 2);
		LargeObjectType genLarge = new LargeObjectType();
		Random rnd = new Random();

		for (int i = 0; i < NUM_RECORDS; i++) {
			if (i % 2 == 0) {
				originalRecords.add(new IntType(42));
			} else {
				originalRecords.add(genLarge.getRandom(rnd));
			}
		}

		SpanningRecordSerializationTest.test(originalRecords, SEGMENT_SIZE, new SpanningRecordSerializer<>(), deserializer);
	}
}
