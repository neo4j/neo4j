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
package org.neo4j.packstream.io;

import io.netty.buffer.ByteBuf;

public enum LengthPrefix {
    NONE(0, 0),
    NIBBLE(0, 15),
    UINT8(1, 255) {
        @Override
        public long readFrom(ByteBuf source) {
            return source.readUnsignedByte();
        }

        @Override
        public void writeTo(ByteBuf target, long length) {
            target.writeByte((int) length);
        }
    },
    UINT16(2, 65_535) {
        @Override
        public long readFrom(ByteBuf source) {
            return source.readUnsignedShort();
        }

        @Override
        public void writeTo(ByteBuf target, long length) {
            target.writeShort((int) length);
        }
    },
    UINT32(4, 4_294_967_295L) {
        @Override
        public long readFrom(ByteBuf source) {
            return source.readUnsignedInt();
        }

        @Override
        public void writeTo(ByteBuf target, long length) {
            target.writeInt((int) length);
        }
    };

    private final int encodedLength;
    private final long maxValue;

    LengthPrefix(int encodedLength, long maxValue) {
        this.encodedLength = encodedLength;
        this.maxValue = maxValue;
    }

    /**
     * Retrieves the total amount of bytes within a given prefix.
     *
     * @return a prefix.
     */
    public int getEncodedLength() {
        return this.encodedLength;
    }

    /**
     * Retrieves the largest value which may be represented by this particular length prefix.
     *
     * @return a maximum length.
     */
    public long getMaxValue() {
        return this.maxValue;
    }

    /**
     * Evaluates whether this prefix is capable of encoding a given number of elements.
     *
     * @param length a desired type length.
     * @return true if within bounds, false otherwise.
     */
    public boolean canEncode(long length) {
        return length <= this.maxValue;
    }

    /**
     * Retrieves a length prefix from a given source buffer.
     *
     * @param source a source buffer.
     * @return a length value.
     * @throws UnsupportedOperationException when this value represents a nibble or no prefix at all.
     * @throws IndexOutOfBoundsException     when insufficient data remains within the given buffer.
     */
    public long readFrom(ByteBuf source) {
        throw new UnsupportedOperationException();
    }

    /**
     * Writes a length prefix to a given target buffer.
     *
     * @param target a target buffer.
     * @param length a length value.
     * @throws UnsupportedOperationException when this value represents a nibble or no prefix at all.
     * @throws IndexOutOfBoundsException     when insufficient capacity remains within the given buffer.
     */
    public void writeTo(ByteBuf target, long length) {
        throw new UnsupportedOperationException();
    }
}
