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
package org.neo4j.bolt.testing.assertions;

import static org.neo4j.util.Preconditions.checkArgument;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufUtil;
import org.assertj.core.api.InstanceOfAssertFactory;

/**
 * Provides conditions which evaluate the state and contents of {@link ByteBuf} instances through Assert4j.
 */
public final class ByteBufAssertions extends ReferenceCountedAssertions<ByteBufAssertions, ByteBuf> {

    private ByteBufAssertions(ByteBuf byteBuf) {
        super(byteBuf, ByteBufAssertions.class);
    }

    public static ByteBufAssertions assertThat(ByteBuf value) {
        return new ByteBufAssertions(value);
    }

    public static InstanceOfAssertFactory<ByteBuf, ByteBufAssertions> byteBuf() {
        return new InstanceOfAssertFactory<>(ByteBuf.class, ByteBufAssertions::new);
    }

    public ByteBufAssertions isDirect() {
        this.isNotNull();

        if (!this.actual.isDirect()) {
            failWithMessage("Expected buffer to be backed by off-heap memory");
        }

        return this;
    }

    public ByteBufAssertions isHeap() {
        this.isNotNull();

        if (this.actual.isDirect()) {
            failWithMessage("Expected buffer to be backed by heap memory");
        }

        return this;
    }

    public ByteBufAssertions hasReadableBytes(int expected) {
        this.isNotNull();

        if (!this.actual.isReadable(expected)) {
            failWithActualExpectedAndMessage(
                    this.actual.readableBytes(),
                    expected,
                    "Expected at least <%d> readable bytes to be available but got <%d>",
                    expected,
                    this.actual.readableBytes());
        }

        return this;
    }

    public ByteBufAssertions hasNoRemainingReadableBytes() {
        this.isNotNull();

        if (this.actual.isReadable()) {
            failWithMessage("Expected no data to remain readable but got <%d> bytes", this.actual.readableBytes());
        }

        return this;
    }

    public ByteBufAssertions contains(ByteBuf expected) {
        checkArgument(expected.isReadable(), "expected must contain at least one readable byte");

        this.isNotNull();

        if (expected.readableBytes() == this.actual.readableBytes()) {
            if (!expected.equals(this.actual)) {
                failWithActualExpectedAndMessage(
                        actual,
                        expected,
                        "Expected payload\n<%s>\n but got \n<%s>",
                        ByteBufUtil.hexDump(expected),
                        ByteBufUtil.hexDump(this.actual));
            }

            return this;
        }

        var heap = expected.alloc().buffer(expected.capacity());

        try {
            this.actual.readBytes(heap);

            if (!expected.equals(this.actual)) {
                failWithActualExpectedAndMessage(
                        heap,
                        expected,
                        "Expected payload\n<%s>\n but got \n<%s>",
                        ByteBufUtil.hexDump(expected),
                        ByteBufUtil.hexDump(heap));
            }

            return this;
        } finally {
            heap.release();
        }
    }

    public ByteBufAssertions containsByte(byte expected) {
        this.isNotNull();

        var actual = this.actual.readByte();
        if (actual != expected) {
            failWithActualExpectedAndMessage(
                    actual, expected, "Expected byte <0x%02X> but got <0x%02X>", expected, actual);
        }

        return this;
    }

    public ByteBufAssertions containsByte(int expected) {
        return this.containsByte((byte) expected);
    }

    public ByteBufAssertions containsUnsignedByte(short expected) {
        this.isNotNull();

        var actual = this.actual.readUnsignedByte();
        if (actual != expected) {
            failWithActualExpectedAndMessage(
                    actual, expected, "Expected unsigned byte <0x%02X> but got <0x%02X>", expected, actual);
        }

        return this;
    }

    public ByteBufAssertions containsUnsignedByte(int expected) {
        return this.containsUnsignedByte((short) expected);
    }

    public ByteBufAssertions containsShort(short expected) {
        this.isNotNull();

        var actual = this.actual.readShort();
        if (actual != expected) {
            failWithActualExpectedAndMessage(
                    actual, expected, "Expected short <0x%04X> but got <0x%04X>", expected, actual);
        }

        return this;
    }

    public ByteBufAssertions containsShort(int expected) {
        return this.containsShort((short) expected);
    }

    public ByteBufAssertions containsUnsignedShort(int expected) {
        this.isNotNull();

        var actual = this.actual.readUnsignedShort();
        if (actual != expected) {
            failWithActualExpectedAndMessage(
                    actual, expected, "Expected unsigned short <0x%04X> but got <0x%04X>", expected, actual);
        }

        return this;
    }

    public ByteBufAssertions containsInt(int expected) {
        this.isNotNull();

        var actual = this.actual.readInt();
        if (actual != expected) {
            failWithActualExpectedAndMessage(
                    actual, expected, "Expected int <0x%08X> but got <0x%08X>", expected, actual);
        }

        return this;
    }

    public ByteBufAssertions containsUnsignedInt(long expected) {
        this.isNotNull();

        var actual = this.actual.readUnsignedInt();
        if (actual != expected) {
            failWithActualExpectedAndMessage(
                    actual, expected, "Expected unsigned int <0x%08X> but got <0x%08X>", expected, actual);
        }

        return this;
    }

    public ByteBufAssertions containsLong(long expected) {
        this.isNotNull();

        var actual = this.actual.readLong();
        if (actual != expected) {
            failWithActualExpectedAndMessage(
                    actual, expected, "Expected long <0x%016> but got <0x%016>", expected, actual);
        }

        return this;
    }
}
