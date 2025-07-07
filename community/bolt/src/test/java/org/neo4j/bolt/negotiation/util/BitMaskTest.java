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

import io.netty.buffer.ByteBufAllocator;
import io.netty.buffer.UnpooledByteBufAllocator;
import java.util.stream.IntStream;
import java.util.stream.Stream;
import org.junit.jupiter.api.DynamicTest;
import org.junit.jupiter.api.TestFactory;
import org.mockito.Mockito;
import org.neo4j.bolt.testing.annotation.StrictBufferExtension;
import org.neo4j.bolt.testing.assertions.BitMaskAssertions;
import org.neo4j.bolt.testing.channel.StrictBufferContext.RootStrictBufferContext;

@StrictBufferExtension
class BitMaskTest {

    @TestFactory
    Stream<DynamicTest> shouldAllocateSufficientBuffer() {
        return IntStream.range(0, 5)
                .map(i -> i * 7)
                .mapToObj(bits -> DynamicTest.dynamicTest(bits + " bits", () -> {
                    var expected = bits / 8 + (bits % 8 == 0 ? 0 : 1);
                    var allocator = Mockito.mock(ByteBufAllocator.class, Mockito.RETURNS_MOCKS);

                    var mask = new BitMask(allocator, bits);

                    BitMaskAssertions.assertThat(mask).hasLength(bits);

                    Mockito.verify(allocator).buffer(expected);
                }));
    }

    @TestFactory
    Stream<DynamicTest> shouldWrapByteArray(RootStrictBufferContext root) {
        return IntStream.range(0, 5)
                .mapToObj(bytes -> root.test(bytes + " bytes", (ctx) -> {
                    var expected = bytes * 8;
                    var buffer = new byte[bytes];

                    var mask = ctx.output(new BitMask(buffer));
                    BitMaskAssertions.assertThat(mask).hasLength(expected);
                }));
    }

    @TestFactory
    Stream<DynamicTest> shouldExposeBitPatterns(RootStrictBufferContext root) {
        return Stream.of(
                        0b01010101010101010101010101010101,
                        0b00001111000011110000111100001111,
                        0b11110000111100001111000011110000,
                        0b00110011001100110011001100110011,
                        0b11001100110011001100110011001100)
                .map(value -> root.test(String.format("%08X", value), (ctx) -> {
                    var encoded = new byte[] {
                        (byte) (value & 0xFF),
                        (byte) ((value >>> 8) & 0xFF),
                        (byte) ((value >>> 16) & 0xFF),
                        (byte) (value >>> 24)
                    };

                    var mask = ctx.output(new BitMask(encoded));

                    var nibble = value & 0xF;
                    var octet = (value & 0xFF0) >>> 4;
                    var bit0 = (value >>> 12) & 1;
                    var bit1 = (value >>> 13) & 1;
                    var remainder = value >>> 14;

                    BitMaskAssertions.assertThat(mask)
                            .hasBits(nibble, 4)
                            .hasBits(octet, 8)
                            .hasBit(bit0 == 1)
                            .hasBit(bit1 == 1)
                            .hasBits(remainder, 18);
                }));
    }

    @TestFactory
    Stream<DynamicTest> shouldIndicateReadableBits(RootStrictBufferContext root) {
        return IntStream.range(0, 5)
                .map(i -> i * 7)
                .mapToObj(bits -> root.test(bits + " bits", (ctx) -> {
                    var mask = ctx.output(new BitMask(UnpooledByteBufAllocator.DEFAULT, bits));

                    BitMaskAssertions.assertThat(mask).hasLength(bits).hasRemaining(bits);

                    for (var i = 0; i < bits; ++i) {
                        mask.read();

                        BitMaskAssertions.assertThat(mask).hasLength(bits).hasRemaining(bits - i - 1);
                    }
                }));
    }

    @TestFactory
    Stream<DynamicTest> shouldIndicateWritableBits(RootStrictBufferContext root) {
        return IntStream.range(0, 5)
                .map(i -> i * 7)
                .mapToObj(bits -> root.test(bits + " bits", (ctx) -> {
                    var mask = ctx.output(new BitMask(UnpooledByteBufAllocator.DEFAULT, bits));

                    BitMaskAssertions.assertThat(mask).hasLength(bits).isWritable(bits);

                    for (var i = 0; i < bits; ++i) {
                        mask.write(true);

                        BitMaskAssertions.assertThat(mask).hasLength(bits).isWritable(bits - i - 1);
                    }
                }));
    }
}
