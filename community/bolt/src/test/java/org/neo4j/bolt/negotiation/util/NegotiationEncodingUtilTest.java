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

import static org.assertj.core.api.Assertions.assertThat;

import io.netty.buffer.UnpooledByteBufAllocator;
import org.junit.jupiter.api.Test;
import org.neo4j.bolt.testing.annotation.StrictBufferExtension;
import org.neo4j.bolt.testing.assertions.BitMaskAssertions;
import org.neo4j.bolt.testing.assertions.ByteBufAssertions;
import org.neo4j.bolt.testing.channel.StrictBufferContext;

@StrictBufferExtension
class NegotiationEncodingUtilTest {

    @Test
    void shouldWriteBitMask(StrictBufferContext ctx) {
        var mask = ctx.output(new BitMask(UnpooledByteBufAllocator.DEFAULT, 24));

        var s = true;
        for (var i = 0; i < mask.length(); ++i) {
            mask.write(s);
            s = !s;
        }

        var actual = ctx.outputBuffer();
        NegotiationEncodingUtil.writeBitMask(actual, mask);

        ByteBufAssertions.assertThat(actual)
                .hasReadableBytes(4)
                .containsByte(0b11010101)
                .containsByte(0b10101010)
                .containsByte(0b11010101)
                .containsByte(0b00000010)
                .hasNoRemainingReadableBytes();
    }

    @Test
    void shouldIndicateFullyReadableBitMasks(StrictBufferContext ctx) {
        var buf = ctx.outputBuffer()
                .writeByte(0x80)
                .writeByte(0xFF)
                .writeByte(0x81)
                .writeByte(0x0F);

        assertThat(NegotiationEncodingUtil.isBitMaskReadable(buf, 32)).isTrue();
    }

    @Test
    void shouldIndicateTruncatedBitMasks(StrictBufferContext ctx) {
        var buf = ctx.outputBuffer().writeByte(0x80).writeByte(0xFF).writeByte(0x81);

        assertThat(NegotiationEncodingUtil.isBitMaskReadable(buf, 32)).isFalse();
    }

    @Test
    void shouldIndicateTruncatedBitMaskWhenLimitedIsExceeded(StrictBufferContext ctx) {
        var buf = ctx.outputBuffer()
                .writeByte(0x80)
                .writeByte(0x80)
                .writeByte(0x80)
                .writeByte(0x80)
                .writeByte(0x01);

        assertThat(NegotiationEncodingUtil.isBitMaskReadable(buf, 4)).isFalse();
    }

    @Test
    void shouldReadBitMask(StrictBufferContext ctx) {
        var buffer = ctx.outputBuffer()
                .writeByte(0b11010101)
                .writeByte(0b10101010)
                .writeByte(0b11010101)
                .writeByte(0b00000010);

        var actual = ctx.output(NegotiationEncodingUtil.readBitMask(buffer));

        BitMaskAssertions.assertThat(actual)
                .hasAtLeastRemaining(24)
                .hasBits(0b01010101, 8)
                .hasBits(0b01010101, 8)
                .hasBits(0b01010101, 8)
                .hasAtMostRemaining(5); // network padding
    }
}
