/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [https://neo4j.com]
 *
 * This file is part of Neo4j.
 *
 * Neo4j is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <https://www.gnu.org/licenses/>.
 */
package org.neo4j.bolt.negotiation.util;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.buffer.Unpooled;
import io.netty.util.ReferenceCounted;
import java.util.Objects;
import org.neo4j.util.Preconditions;

public final class BitMask implements ReferenceCounted {

    private final ByteBuf encoded;
    private final int length;

    private int readerIndex;
    private int writerIndex;

    private BitMask(ByteBuf encoded, int length) {
        Objects.requireNonNull(encoded, "encoded");
        Preconditions.requireNonNegative(length);

        this.encoded = encoded;
        this.length = length;
    }

    public BitMask(ByteBufAllocator alloc, int length) {
        this(allocateZeroedBuffer(alloc, length), length);
    }

    private static ByteBuf allocateZeroedBuffer(ByteBufAllocator alloc, int length) {
        var bufferLength = length / 8 + (length % 8 == 0 ? 0 : 1);
        return alloc.buffer(bufferLength).setZero(0, bufferLength);
    }

    public BitMask(byte[] encoded) {
        this(Unpooled.wrappedBuffer(encoded), encoded.length * 8);
    }

    public boolean get(int index) {
        if (index < 0 || index >= this.length) {
            throw new IllegalArgumentException(
                    "Expected mask index to be within bounds 0 < x < " + this.length + " but got " + index);
        }

        var arrayIndex = index / 8;
        var bitOffset = index % 8;

        return (this.encoded.getByte(arrayIndex) & (1 << bitOffset)) != 0;
    }

    public int readable() {
        return this.length - this.readerIndex;
    }

    public boolean isReadable() {
        return this.isReadable(1);
    }

    public boolean isReadable(int bits) {
        return this.readable() >= bits;
    }

    public boolean read() {
        return this.get(this.readerIndex++);
    }

    public int readN(int length) {
        if (length < 0 || length > 31) {
            throw new IllegalArgumentException("Expected mask index to be within bounds 0 < x <= 31 but got " + length);
        }

        if (length == 0) {
            return 0;
        }

        var accumulator = 0;
        for (var i = 0; i < length; ++i) {
            accumulator ^= (this.read() ? 1 : 0) << i;
        }

        return accumulator;
    }

    public void set(int index, boolean value) {
        if (index < 0 || index >= this.length) {
            throw new IllegalArgumentException(
                    "Expected mask index to be within bounds 0 < x < " + this.length + " but got " + index);
        }

        var arrayIndex = index / 8;
        var bitOffset = index % 8;

        var heap = this.encoded.getByte(arrayIndex);
        if (value) {
            heap |= 1 << bitOffset;
        } else {
            heap &= ~(1 << bitOffset);
        }
        this.encoded.setByte(arrayIndex, heap);
    }

    public int writable() {
        return this.length - this.writerIndex;
    }

    public boolean isWritable() {
        return this.isWritable(1);
    }

    public boolean isWritable(int length) {
        return this.writable() >= length;
    }

    public void write(boolean value) {
        this.set(this.writerIndex++, value);
    }

    public void writeN(int value, int length) {
        for (var i = 0; i < length; ++i) {
            this.write((value >> i & 1) != 0);
        }
    }

    public int length() {
        return this.length;
    }

    public ByteBuf asReadOnlyBuffer() {
        return this.encoded.asReadOnly();
    }

    @Override
    public int refCnt() {
        return this.encoded.refCnt();
    }

    @Override
    public BitMask retain() {
        this.encoded.retain();
        return this;
    }

    @Override
    public BitMask retain(int i) {
        this.encoded.retain(i);
        return this;
    }

    @Override
    public BitMask touch() {
        this.encoded.touch();
        return this;
    }

    @Override
    public BitMask touch(Object o) {
        this.encoded.touch(o);
        return this;
    }

    @Override
    public boolean release() {
        ByteBuf byteBuf = this.encoded;
        if (byteBuf == null) {
            return false;
        }
        return byteBuf.release();
    }

    @Override
    public boolean release(int i) {
        return this.encoded.release(i);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof BitMask booleans)) {
            return false;
        }
        return length == booleans.length && Objects.deepEquals(encoded, booleans.encoded);
    }

    @Override
    public int hashCode() {
        return Objects.hash(this.encoded.hashCode(), length);
    }

    @Override
    public String toString() {
        var builder = new StringBuilder();

        this.encoded.readerIndex(0);
        try {
            while (this.encoded.isReadable()) {
                var b = this.encoded.readByte();
                builder.append(String.format("%8s", Integer.toBinaryString(b)).replace(' ', '0'));
            }
        } finally {
            this.encoded.readerIndex(0);
        }

        return builder.reverse().toString();
    }
}
