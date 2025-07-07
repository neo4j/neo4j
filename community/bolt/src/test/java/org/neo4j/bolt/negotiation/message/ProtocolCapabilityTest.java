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
package org.neo4j.bolt.negotiation.message;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.data.Index.atIndex;
import static org.neo4j.bolt.testing.assertions.BitMaskAssertions.assertThat;

import io.netty.buffer.UnpooledByteBufAllocator;
import java.util.EnumSet;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;
import org.neo4j.bolt.negotiation.util.BitMask;
import org.neo4j.bolt.testing.annotation.StrictBufferExtension;
import org.neo4j.bolt.testing.channel.StrictBufferContext;

@StrictBufferExtension
class ProtocolCapabilityTest {

    @Test
    void shouldThrowWhenNegativeNetworkIdsAreGiven() {
        Assertions.assertThatExceptionOfType(IllegalArgumentException.class)
                .isThrownBy(() -> ProtocolCapability.byNetworkIndex(-1))
                .withMessage("Expected positive int value, got -1")
                .withNoCause();
    }

    @Test
    void shouldThrowWhenZeroNetworkIdIsGiven() {
        Assertions.assertThatExceptionOfType(IllegalArgumentException.class)
                .isThrownBy(() -> ProtocolCapability.byNetworkIndex(0))
                .withMessage("Expected positive int value, got 0")
                .withNoCause();
    }

    @Test
    void shouldIgnoreUnknownNetworkIds() {
        assertThat(ProtocolCapability.byNetworkIndex(5)).isEmpty();
    }

    @Test
    void shouldReturnKnownNetworkIds() {
        assertThat(ProtocolCapability.byNetworkIndex(1)).isNotEmpty().contains(ProtocolCapability.FABRIC);
    }

    @Test
    void shouldConvertToBitMask(StrictBufferContext ctx) {
        var values = EnumSet.of(ProtocolCapability.FABRIC);
        var mask = ctx.output(ProtocolCapability.toBitMask(UnpooledByteBufAllocator.DEFAULT, values));

        assertThat(mask).hasAtLeastRemaining(1).hasBit(true, atIndex(0));
    }

    @Test
    void shouldConvertFromEmptyBitMask(StrictBufferContext ctx) {
        var mask = ctx.output(new BitMask(UnpooledByteBufAllocator.DEFAULT, 8));
        for (var i = 0; i < 8; ++i) {
            mask.write(false);
        }

        var actual = ProtocolCapability.fromBitMask(mask);

        assertThat(actual).isEmpty();
    }

    @Test
    void shouldConvertFromFullBitMask(StrictBufferContext ctx) {
        var mask = ctx.output(new BitMask(UnpooledByteBufAllocator.DEFAULT, 8));
        for (var i = 0; i < 8; ++i) {
            mask.write(true);
        }

        var actual = ProtocolCapability.fromBitMask(mask);

        assertThat(actual).hasSize(1).contains(ProtocolCapability.FABRIC);
    }
}
