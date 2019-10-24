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

import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.core.fs.Path;
import org.apache.flink.core.fs.RecoverableWriter;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.util.Arrays;

import static org.apache.flink.runtime.io.network.buffer.BufferBuilderTestUtils.createFilledFinishedBufferConsumer;
import static org.junit.Assert.assertArrayEquals;

/**
 * Tests for {@link BufferPersister}.
 */
public class BufferPersisterTest {
	private static final int BUFFER_SIZE = 32 * 1024;

	@Rule
	public TemporaryFolder temporaryFolder = new TemporaryFolder();

	@Test
	public void testSimplePersist() throws Exception {
		Path writerPath = new Path(temporaryFolder.newFolder().getPath());

		RecoverableWriter fileSystemWriter = FileSystem.get(writerPath.toUri()).createRecoverableWriter();

		try (BufferPersister bufferPersister = new BufferPersister(fileSystemWriter, writerPath, 2)) {

			bufferPersister.add(createFilledFinishedBufferConsumer(BUFFER_SIZE), 0);
			bufferPersister.add(createFilledFinishedBufferConsumer(BUFFER_SIZE), 0);
			bufferPersister.add(createFilledFinishedBufferConsumer(BUFFER_SIZE), 1);
			bufferPersister.add(createFilledFinishedBufferConsumer(BUFFER_SIZE), 0);

			BufferBuilder unfinishedBufferBuilder1 = BufferBuilderTestUtils.createBufferBuilder(BUFFER_SIZE);
			BufferConsumer unfinishedBufferConsumer1 = unfinishedBufferBuilder1.createBufferConsumer();
			BufferBuilderTestUtils.fillBufferBuilder(unfinishedBufferBuilder1, BUFFER_SIZE);

			bufferPersister.add(unfinishedBufferConsumer1, 0);

			BufferBuilder unfinishedBufferBuilder2 = BufferBuilderTestUtils.createBufferBuilder(BUFFER_SIZE);
			BufferConsumer unfinishedBufferConsumer2 = unfinishedBufferBuilder2.createBufferConsumer();
			BufferBuilderTestUtils.fillBufferBuilder(unfinishedBufferBuilder2, BUFFER_SIZE / 2);

			bufferPersister.add(unfinishedBufferConsumer2, 1);

			bufferPersister.persist().get();

			assertFileSizes(temporaryFolder.getRoot(), BUFFER_SIZE * 5 + BUFFER_SIZE / 2);

			unfinishedBufferBuilder1.finish();
			unfinishedBufferBuilder2.finish();

			bufferPersister.add(createFilledFinishedBufferConsumer(BUFFER_SIZE), 0);
			bufferPersister.add(createFilledFinishedBufferConsumer(BUFFER_SIZE), 1);

			bufferPersister.persist().get();

			assertFileSizes(temporaryFolder.getRoot(), BUFFER_SIZE * 5 + BUFFER_SIZE / 2, BUFFER_SIZE * 2);
		}
	}

	private void assertFileSizes(File baseDirectory, long... expectedFileSizes) throws IOException {
		long[] actualFilesSizes = Files.walk(baseDirectory.toPath())
			.map(java.nio.file.Path::toFile)
			.filter(file -> !file.isDirectory())
			.mapToLong(File::length)
			.filter(size -> size > 0)
			.sorted()
			.toArray();

		Arrays.sort(expectedFileSizes);
		assertArrayEquals(expectedFileSizes, actualFilesSizes);
	}
}
