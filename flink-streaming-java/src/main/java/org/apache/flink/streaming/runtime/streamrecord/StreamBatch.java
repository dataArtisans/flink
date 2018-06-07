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

package org.apache.flink.streaming.runtime.streamrecord;

import org.apache.flink.annotation.Internal;

import java.lang.reflect.Array;

import static org.apache.flink.util.Preconditions.checkState;

/**
 * ???
 */
@Internal
public final class StreamBatch<T> {
	private T[] batch;
	private final int maxBatchSize;
	private int index;

	public StreamBatch(int maxBatchSize) {
		checkState(maxBatchSize > 0);
		this.maxBatchSize = maxBatchSize;
//		batch = new ArrayList<>(maxBatchSize);
	}

	public boolean isFull() {
		return index >= maxBatchSize;
	}

	public void add(T element) {
//		batch.add(element);
		if (batch == null) {
			Class<?> aClass = element.getClass();
			batch = (T[]) Array.newInstance(aClass, maxBatchSize);
		}
		batch[index++] = element;
	}

//	public T[] getRecords() {
//		return batch;
//	}

	public void clear() {
//		batch.clear();
		for (int i = 0; i < index; i++) {
			batch[i] = null;
		}
		index = 0;
	}

	public int getNumberOfElements() {
		return index;
	}

	public T get(int i) {
		return batch[i];
	}
}
