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

import org.apache.flink.shaded.netty4.io.netty.buffer.ByteBuf;

import org.junit.Test;

import java.io.IOException;
import java.nio.ReadOnlyBufferException;
import java.util.Random;

import static org.junit.Assert.assertEquals;

/**
 * Tests for {@link ReadOnlySlicedNetworkBuffer}.
 */
public class ReadOnlySlicedBufferTest extends BufferTest {
	private final Random random = new Random();

	@Override
	protected ByteBuf newBuffer(int length, int maxCapacity) {
		int offset = length == 0 ? 0 : random.nextInt(length);

		NetworkBuffer buffer = (NetworkBuffer) super.newBuffer(length * 2, maxCapacity);
		ReadOnlySlicedNetworkBuffer readOnlySlice = buffer.readOnlySlice(offset, length);

		assertEquals(0, readOnlySlice.readerIndex());
		assertEquals(length, readOnlySlice.writerIndex());

		return readOnlySlice;
	}

	@Test(expected = ReadOnlyBufferException.class)
	@Override
	public void comparableInterfaceNotViolated() {
		super.comparableInterfaceNotViolated();
	}

	@Test(expected = ReadOnlyBufferException.class)
	@Override
	public void writerIndexBoundaryCheck4() {
		super.writerIndexBoundaryCheck4();
	}

	@Test(expected = ReadOnlyBufferException.class)
	@Override
	public void getByteArrayBoundaryCheck3() {
		super.getByteArrayBoundaryCheck3();
	}

	@Test(expected = ReadOnlyBufferException.class)
	@Override
	public void getByteArrayBoundaryCheck4() {
		super.getByteArrayBoundaryCheck4();
	}

	@Test(expected = ReadOnlyBufferException.class)
	@Override
	public void getByteBufferState() {
		super.getByteBufferState();
	}

	@Test(expected = ReadOnlyBufferException.class)
	@Override
	public void getDirectByteBufferState() {
		super.getDirectByteBufferState();
	}

	@Test(expected = ReadOnlyBufferException.class)
	@Override
	public void testRandomByteAccess() {
		super.testRandomByteAccess();
	}

	@Test(expected = ReadOnlyBufferException.class)
	@Override
	public void testRandomUnsignedByteAccess() {
		super.testRandomUnsignedByteAccess();
	}

	@Test(expected = ReadOnlyBufferException.class)
	@Override
	public void testRandomShortAccess() {
		super.testRandomShortAccess();
	}

	@Test(expected = ReadOnlyBufferException.class)
	@Override
	public void testShortConsistentWithByteBuffer() {
		super.testShortConsistentWithByteBuffer();
	}

	@Test(expected = ReadOnlyBufferException.class)
	@Override
	public void testRandomUnsignedShortAccess() {
		super.testRandomUnsignedShortAccess();
	}

	@Test(expected = ReadOnlyBufferException.class)
	@Override
	public void testRandomMediumAccess() {
		super.testRandomMediumAccess();
	}

	@Test(expected = ReadOnlyBufferException.class)
	@Override
	public void testRandomUnsignedMediumAccess() {
		super.testRandomUnsignedMediumAccess();
	}

	@Test(expected = ReadOnlyBufferException.class)
	@Override
	public void testMediumConsistentWithByteBuffer() {
		super.testMediumConsistentWithByteBuffer();
	}

	@Test(expected = ReadOnlyBufferException.class)
	@Override
	public void testRandomIntAccess() {
		super.testRandomIntAccess();
	}

	@Test(expected = ReadOnlyBufferException.class)
	@Override
	public void testIntConsistentWithByteBuffer() {
		super.testIntConsistentWithByteBuffer();
	}

	@Test(expected = ReadOnlyBufferException.class)
	@Override
	public void testRandomUnsignedIntAccess() {
		super.testRandomUnsignedIntAccess();
	}

	@Test(expected = ReadOnlyBufferException.class)
	@Override
	public void testRandomLongAccess() {
		super.testRandomLongAccess();
	}

	@Test(expected = ReadOnlyBufferException.class)
	@Override
	public void testSetZero() {
		super.testSetZero();
	}

	@Test(expected = AssertionError.class)
	@Override
	public void testSequentialByteAccess() {
		super.testSequentialByteAccess();
	}

	@Test(expected = AssertionError.class)
	@Override
	public void testSequentialUnsignedByteAccess() {
		super.testSequentialUnsignedByteAccess();
	}

	@Test(expected = AssertionError.class)
	@Override
	public void testSequentialShortAccess() {
		super.testSequentialShortAccess();
	}

	@Test(expected = AssertionError.class)
	@Override
	public void testSequentialUnsignedShortAccess() {
		super.testSequentialUnsignedShortAccess();
	}

	@Test(expected = AssertionError.class)
	@Override
	public void testSequentialMediumAccess() {
		super.testSequentialMediumAccess();
	}

	@Test(expected = AssertionError.class)
	@Override
	public void testSequentialUnsignedMediumAccess() {
		super.testSequentialUnsignedMediumAccess();
	}

	@Test(expected = AssertionError.class)
	@Override
	public void testSequentialIntAccess() {
		super.testSequentialIntAccess();
	}

	@Test(expected = AssertionError.class)
	@Override
	public void testSequentialUnsignedIntAccess() {
		super.testSequentialUnsignedIntAccess();
	}

	@Test(expected = AssertionError.class)
	@Override
	public void testSequentialLongAccess() {
		super.testSequentialLongAccess();
	}

	@Test(expected = ReadOnlyBufferException.class)
	@Override
	public void testByteArrayTransfer() {
		super.testByteArrayTransfer();
	}

	@Test(expected = ReadOnlyBufferException.class)
	@Override
	public void testRandomByteArrayTransfer1() {
		super.testRandomByteArrayTransfer1();
	}

	@Test(expected = ReadOnlyBufferException.class)
	@Override
	public void testRandomByteArrayTransfer2() {
		super.testRandomByteArrayTransfer2();
	}

	@Test(expected = ReadOnlyBufferException.class)
	@Override
	public void testRandomHeapBufferTransfer1() {
		super.testRandomHeapBufferTransfer1();
	}

	@Test(expected = ReadOnlyBufferException.class)
	@Override
	public void testRandomHeapBufferTransfer2() {
		super.testRandomHeapBufferTransfer2();
	}

	@Test(expected = ReadOnlyBufferException.class)
	@Override
	public void testRandomDirectBufferTransfer() {
		super.testRandomDirectBufferTransfer();
	}

	@Test(expected = ReadOnlyBufferException.class)
	@Override
	public void testRandomByteBufferTransfer() {
		super.testRandomByteBufferTransfer();
	}

	@Test(expected = ReadOnlyBufferException.class)
	@Override
	public void testSequentialByteArrayTransfer1() {
		super.testSequentialByteArrayTransfer1();
	}

	@Test(expected = ReadOnlyBufferException.class)
	@Override
	public void testSequentialByteArrayTransfer2() {
		super.testSequentialByteArrayTransfer2();
	}

	@Test(expected = ReadOnlyBufferException.class)
	@Override
	public void testSequentialHeapBufferTransfer1() {
		super.testSequentialHeapBufferTransfer1();
	}

	@Test(expected = ReadOnlyBufferException.class)
	@Override
	public void testSequentialHeapBufferTransfer2() {
		super.testSequentialHeapBufferTransfer2();
	}

	@Test(expected = ReadOnlyBufferException.class)
	@Override
	public void testSequentialDirectBufferTransfer1() {
		super.testSequentialDirectBufferTransfer1();
	}

	@Test(expected = ReadOnlyBufferException.class)
	@Override
	public void testSequentialDirectBufferTransfer2() {
		super.testSequentialDirectBufferTransfer2();
	}

	@Test(expected = ReadOnlyBufferException.class)
	@Override
	public void testSequentialByteBufferBackedHeapBufferTransfer1() {
		super.testSequentialByteBufferBackedHeapBufferTransfer1();
	}

	@Test(expected = ReadOnlyBufferException.class)
	@Override
	public void testSequentialByteBufferBackedHeapBufferTransfer2() {
		super.testSequentialByteBufferBackedHeapBufferTransfer2();
	}

	@Test(expected = ReadOnlyBufferException.class)
	@Override
	public void testSequentialByteBufferTransfer() {
		super.testSequentialByteBufferTransfer();
	}

	@Test(expected = ReadOnlyBufferException.class)
	@Override
	public void testSequentialCopiedBufferTransfer1() {
		super.testSequentialCopiedBufferTransfer1();
	}

	@Test(expected = ReadOnlyBufferException.class)
	@Override
	public void testSequentialSlice1() {
		super.testSequentialSlice1();
	}

	@Test(expected = ReadOnlyBufferException.class)
	@Override
	public void testWriteZero() {
		super.testWriteZero();
	}

	@Test(expected = ReadOnlyBufferException.class)
	@Override
	public void testDiscardReadBytes() {
		super.testDiscardReadBytes();
	}

	@Test(expected = ReadOnlyBufferException.class)
	@Override
	public void testDiscardReadBytes2() {
		super.testDiscardReadBytes2();
	}

	@Test(expected = ReadOnlyBufferException.class)
	@Override
	public void testStreamTransfer1() throws Exception {
		super.testStreamTransfer1();
	}

	@Test(expected = ReadOnlyBufferException.class)
	@Override
	public void testStreamTransfer2() throws Exception {
		super.testStreamTransfer2();
	}

	@Test(expected = ReadOnlyBufferException.class)
	@Override
	public void testCopy() {
		super.testCopy();
	}

	@Test(expected = ReadOnlyBufferException.class)
	@Override
	public void testDuplicate() {
		super.testDuplicate();
	}

	@Test(expected = ReadOnlyBufferException.class)
	@Override
	public void testEquals() {
		super.testEquals();
	}

	@Test(expected = ReadOnlyBufferException.class)
	@Override
	public void testCompareTo() {
		super.testCompareTo();
	}

	@Test(expected = ReadOnlyBufferException.class)
	@Override
	public void testCompareTo2() {
		super.testCompareTo2();
	}

	@Test(expected = ReadOnlyBufferException.class)
	@Override
	public void testToString() {
		super.testToString();
	}

	@Test(expected = ReadOnlyBufferException.class)
	@Override
	public void testIndexOf() {
		super.testIndexOf();
	}

	@Test(expected = ReadOnlyBufferException.class)
	@Override
	public void testNioBuffer1() {
		super.testNioBuffer1();
	}

	@Test(expected = ReadOnlyBufferException.class)
	@Override
	public void testToByteBuffer2() {
		super.testToByteBuffer2();
	}

	@Test(expected = ReadOnlyBufferException.class)
	@Override
	public void testHashCode() {
		super.testHashCode();
	}

	@Test(expected = ReadOnlyBufferException.class)
	@Override
	public void testDiscardAllReadBytes() {
		super.testDiscardAllReadBytes();
	}

	@Test(expected = ReadOnlyBufferException.class)
	@Override
	public void testForEachByte() {
		super.testForEachByte();
	}

	@Test(expected = ReadOnlyBufferException.class)
	@Override
	public void testForEachByteAbort() {
		super.testForEachByteAbort();
	}

	@Test(expected = ReadOnlyBufferException.class)
	@Override
	public void testForEachByteDesc() {
		super.testForEachByteDesc();
	}

	@Test(expected = ReadOnlyBufferException.class)
	@Override
	public void testInternalNioBuffer() {
		super.testInternalNioBuffer();
	}

	@Test(expected = ReadOnlyBufferException.class)
	@Override
	public void testDuplicateReadGatheringByteChannelMultipleThreads() throws Exception {
		super.testDuplicateReadGatheringByteChannelMultipleThreads();
	}

	@Test(expected = ReadOnlyBufferException.class)
	@Override
	public void testSliceReadGatheringByteChannelMultipleThreads() throws Exception {
		super.testSliceReadGatheringByteChannelMultipleThreads();
	}

	@Test(expected = ReadOnlyBufferException.class)
	@Override
	public void testDuplicateReadOutputStreamMultipleThreads() throws Exception {
		super.testDuplicateReadOutputStreamMultipleThreads();
	}

	@Test(expected = ReadOnlyBufferException.class)
	@Override
	public void testWriteZeroAfterRelease() throws IOException {
		super.testWriteZeroAfterRelease();
	}

	@Test(expected = ReadOnlyBufferException.class)
	@Override
	public void testSliceReadOutputStreamMultipleThreads() throws Exception {
		super.testSliceReadOutputStreamMultipleThreads();
	}

	@Test(expected = ReadOnlyBufferException.class)
	@Override
	public void testDuplicateBytesInArrayMultipleThreads() throws Exception {
		super.testDuplicateBytesInArrayMultipleThreads();
	}

	@Test(expected = ReadOnlyBufferException.class)
	@Override
	public void testSliceBytesInArrayMultipleThreads() throws Exception {
		super.testSliceBytesInArrayMultipleThreads();
	}

	@Test(expected = ReadOnlyBufferException.class)
	@Override
	public void readByteThrowsIndexOutOfBoundsException() {
		super.readByteThrowsIndexOutOfBoundsException();
	}

	@Test(expected = ReadOnlyBufferException.class)
	@Override
	public void testNioBufferExposeOnlyRegion() {
		super.testNioBufferExposeOnlyRegion();
	}

	@Test(expected = ReadOnlyBufferException.class)
	@Override
	public void ensureWritableWithOutForceDoesNotThrow() {
		super.ensureWritableWithOutForceDoesNotThrow();
	}

	@Test(expected = ReadOnlyBufferException.class)
	@Override
	public void testLittleEndianWithExpand() {
		super.testLittleEndianWithExpand();
	}

	@Test(expected = ReadOnlyBufferException.class)
	@Override
	public void testDiscardReadBytesAfterRelease() {
		super.testDiscardReadBytesAfterRelease();
	}

	@Test(expected = ReadOnlyBufferException.class)
	@Override
	public void testEnsureWritableAfterRelease() {
		super.testEnsureWritableAfterRelease();
	}

	@Test(expected = ReadOnlyBufferException.class)
	@Override
	public void testSetBytesAfterRelease3() {
		super.testSetBytesAfterRelease3();
	}

	@Test(expected = ReadOnlyBufferException.class)
	@Override
	public void testSetBytesAfterRelease4() {
		super.testSetBytesAfterRelease4();
	}

	@Test(expected = ReadOnlyBufferException.class)
	@Override
	public void testSetBytesAfterRelease5() {
		super.testSetBytesAfterRelease5();
	}

	@Test(expected = ReadOnlyBufferException.class)
	@Override
	public void testSetBytesAfterRelease6() {
		super.testSetBytesAfterRelease6();
	}

	@Test(expected = ReadOnlyBufferException.class)
	@Override
	public void testSetBytesAfterRelease7() throws IOException {
		super.testSetBytesAfterRelease7();
	}

	@Test(expected = ReadOnlyBufferException.class)
	@Override
	public void testSetBytesAfterRelease8() throws IOException {
		super.testSetBytesAfterRelease8();
	}

	@Test(expected = ReadOnlyBufferException.class)
	@Override
	public void testGetReadOnlyDirectDst() {
		super.testGetReadOnlyDirectDst();
	}

	@Test(expected = ReadOnlyBufferException.class)
	@Override
	public void testGetReadOnlyHeapDst() {
		super.testGetReadOnlyHeapDst();
	}

	@Test(expected = ReadOnlyBufferException.class)
	@Override
	public void testReadBytes() {
		super.testReadBytes();
	}

	@Test(expected = ReadOnlyBufferException.class)
	@Override
	public void testGetBytesByteBuffer() {
		super.testGetBytesByteBuffer();
	}

	@Test(expected = ReadOnlyBufferException.class)
	@Override
	public void testForEachByteDesc2() {
		super.testForEachByteDesc2();
	}

	@Test(expected = ReadOnlyBufferException.class)
	@Override
	public void testForEachByte2() {
		super.testForEachByte2();
	}

	@Test(expected = AssertionError.class)
	@Override
	public void testCapacityEnforceMaxCapacity() {
		super.testCapacityEnforceMaxCapacity();
	}

	@Test(expected = AssertionError.class)
	@Override
	public void testCapacityNegative() {
		super.testCapacityNegative();
	}

	@Test(expected = AssertionError.class)
	@Override
	public void testCapacityDecrease() {
		super.testCapacityDecrease();
	}

	@Test(expected = AssertionError.class)
	@Override
	public void testCapacityIncrease() {
		super.testCapacityIncrease();
	}
}
