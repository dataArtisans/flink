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

import org.apache.flink.shaded.netty4.io.netty.buffer.ByteBuf;
import org.apache.flink.shaded.netty4.io.netty.buffer.ByteBufAllocator;
import org.apache.flink.shaded.netty4.io.netty.buffer.SlicedByteBuf;

import java.io.InputStream;
import java.nio.ByteBuffer;
import java.nio.ReadOnlyBufferException;
import java.nio.channels.ScatteringByteChannel;

/**
 * Minimal best-effort read-only {@link SlicedByteBuf} implementation wrapping a
 * {@link NetworkBuffer}'s sub-region based on <tt>io.netty.buffer.SlicedByteBuf</tt> and
 * <tt>io.netty.buffer.ReadOnlyByteBuf</tt>.
 *
 * <p><strong>BEWARE:</strong> We do not guarantee to blcok every operation that is able to write
 * data but all returned data structures should be handled as if it was!.
 */
public final class ReadOnlySlicedNetworkBuffer extends SlicedByteBuf implements Buffer {

	/**
	 * Creates a buffer which shares the memory segment of the given buffer and exposed the given
	 * sub-region only.
	 *
	 * <p>Reader and writer indices as well as markers are not shared. Reference counters are
	 * shared but the slice is not {@link #retainBuffer() retained} automatically.
	 *
	 * @param buffer the buffer to derive from
	 * @param index the index to start from
	 * @param length the length of the slice
	 */
	ReadOnlySlicedNetworkBuffer(NetworkBuffer buffer, int index, int length) {
		super(buffer, index, length);
	}

	@Override
	public NetworkBuffer unwrap() {
		return (NetworkBuffer) super.unwrap();
	}

	@Override
	public boolean isBuffer() {
		return unwrap().isBuffer();
	}

	@Override
	public void tagAsEvent() {
		unwrap().tagAsEvent();
	}

	/**
	 * Returns the underlying memory segment.
	 *
	 * <p><strong>BEWARE:</strong> Although we cannot set the memory segment read-only it should be
	 * handled as if it was!.
	 *
	 * @return the memory segment backing this buffer
	 */
	@Override
	public MemorySegment getMemorySegment() {
		return unwrap().getMemorySegment();
	}

	@Override
	public BufferRecycler getRecycler() {
		return unwrap().getRecycler();
	}

	@Override
	public void recycleBuffer() {
		unwrap().recycleBuffer();
	}

	@Override
	public boolean isRecycled() {
		return unwrap().isRecycled();
	}

	@Override
	public ReadOnlySlicedNetworkBuffer retainBuffer() {
		unwrap().retainBuffer();
		return this;
	}

	@Override
	public int getSize() {
		return unwrap().getSize();
	}

	@Override
	public int getReaderIndex() {
		return readerIndex();
	}

	@Override
	public void setReaderIndex(int readerIndex) throws IndexOutOfBoundsException {
		readerIndex(readerIndex);
	}

	@Override
	public int getWriterIndex() {
		return writerIndex();
	}

	@Override
	public void setWriterIndex(int writerIndex) {
		writerIndex(writerIndex);
	}

	@Override
	public ByteBuffer getNioBufferReadable() {
		return nioBuffer();
	}

	@Override
	public ByteBuffer getNioBuffer(int index, int length) throws IndexOutOfBoundsException {
		return nioBuffer(index, length);
	}

	@Override
	public ByteBuffer nioBuffer(int index, int length) {
		return super.nioBuffer(index, length).asReadOnlyBuffer();
	}

	@Override
	public boolean isWritable() {
		return false;
	}

	@Override
	public boolean isWritable(int numBytes) {
		return false;
	}

	@Override
	public ByteBuf ensureWritable(int minWritableBytes) {
		throw new ReadOnlyBufferException();
	}

	@Override
	public void setAllocator(ByteBufAllocator allocator) {
		unwrap().setAllocator(allocator);
	}

	// ------------------------------------------------------------------------

	@Override
	public ByteBuf capacity(int newCapacity) {
		throw new ReadOnlyBufferException();
	}

	@Override
	public ByteBuf setBytes(int index, ByteBuf src, int srcIndex, int length) {
		throw new ReadOnlyBufferException();
	}

	@Override
	public ByteBuf setBytes(int index, byte[] src, int srcIndex, int length) {
		throw new ReadOnlyBufferException();
	}

	@Override
	public ByteBuf setBytes(int index, ByteBuffer src) {
		throw new ReadOnlyBufferException();
	}

	@Override
	public int setBytes(int index, InputStream in, int length) {
		throw new ReadOnlyBufferException();
	}

	@Override
	public int setBytes(int index, ScatteringByteChannel in, int length) {
		throw new ReadOnlyBufferException();
	}

	@Override
	protected void _setByte(int index, int value) {
		throw new ReadOnlyBufferException();
	}

	@Override
	protected void _setShort(int index, int value) {
		throw new ReadOnlyBufferException();
	}

	@Override
	protected void _setMedium(int index, int value) {
		throw new ReadOnlyBufferException();
	}

	@Override
	protected void _setInt(int index, int value) {
		throw new ReadOnlyBufferException();
	}

	@Override
	protected void _setLong(int index, long value) {
		throw new ReadOnlyBufferException();
	}

	@Override
	public ByteBuf discardReadBytes() {
		throw new ReadOnlyBufferException();
	}

}
