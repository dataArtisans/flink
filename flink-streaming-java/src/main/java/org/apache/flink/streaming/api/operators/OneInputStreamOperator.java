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

package org.apache.flink.streaming.api.operators;

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.runtime.streamrecord.LatencyMarker;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecordBatch;

/**
 * Interface for stream operators with one input. Use
 * {@link org.apache.flink.streaming.api.operators.AbstractStreamOperator} as a base class if
 * you want to implement a custom operator.
 *
 * @param <IN> The input type of the operator
 * @param <OUT> The output type of the operator
 */
@PublicEvolving
public interface OneInputStreamOperator<IN, OUT> extends StreamOperator<OUT> {

	/**
	 * Processes one element that arrived at this operator.
	 * This method is guaranteed to not be called concurrently with other methods of the operator.
	 */
	void processElement(StreamRecord<IN> element) throws Exception;

	default void processBatch(StreamRecordBatch<IN> batch) throws Exception {
		throw new UnsupportedOperationException("processBatch is not supported yet in [" + this + "]");
	}

	default void processBatch2(StreamRecordBatch<IN> batch) throws Exception {
		for (int i = 0; i < batch.getNumberOfElements(); i++) {
			StreamRecord<IN> element = batch.get(i);
			setKeyContextElement1(element);
			processElement(element);
		}
	}

	/**
	 * Processes a {@link Watermark}.
	 * This method is guaranteed to not be called concurrently with other methods of the operator.
	 *
	 * @see org.apache.flink.streaming.api.watermark.Watermark
	 */
	void processWatermark(Watermark mark) throws Exception;

	void processLatencyMarker(LatencyMarker latencyMarker) throws Exception;
}
