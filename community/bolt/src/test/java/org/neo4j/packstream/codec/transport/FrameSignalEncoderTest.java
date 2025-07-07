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
package org.neo4j.packstream.codec.transport;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

import io.netty.buffer.ByteBuf;
import java.util.function.Predicate;
import java.util.stream.Stream;
import org.junit.jupiter.api.DynamicTest;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestFactory;
import org.neo4j.bolt.testing.annotation.StrictBufferExtension;
import org.neo4j.bolt.testing.assertions.ByteBufAssertions;
import org.neo4j.bolt.testing.channel.StrictBufferContext;
import org.neo4j.bolt.testing.channel.StrictBufferContext.RootStrictBufferContext;
import org.neo4j.packstream.signal.FrameSignal;

@StrictBufferExtension
class FrameSignalEncoderTest {

    @TestFactory
    Stream<DynamicTest> shouldEncodeSignals(RootStrictBufferContext root) {
        return Stream.of(FrameSignal.values())
                .map(signal -> root.test(signal.name(), (ctx) -> {
                    var channel = ctx.channel(new FrameSignalEncoder());

                    channel.writeOutbound(signal);

                    var buf = ctx.output(channel.<ByteBuf>readOutbound());

                    ByteBufAssertions.assertThat(buf)
                            .containsUnsignedShort(0x00)
                            .hasNoRemainingReadableBytes()
                            .hasReferences(1);
                }));
    }

    @Test
    void shouldIgnoreSignalsWhenInsideOfMessage(StrictBufferContext ctx) {
        var channel = ctx.channel(new FrameSignalEncoder());

        // handler passes buffer outbound as-is - mark as output since we expect it to remain at one
        // ref count at the end of the test
        channel.writeOutbound(ctx.outputBuffer(1).writeByte(0x42));

        var signal = ctx.output(channel.<ByteBuf>readOutbound());

        ByteBufAssertions.assertThat(signal)
                .containsByte(0x42)
                .hasNoRemainingReadableBytes()
                .hasReferences(1);

        channel.writeOutbound(FrameSignal.NOOP);

        signal = ctx.output(channel.readOutbound());
        assertFalse(signal.isReadable());

        channel.writeOutbound(FrameSignal.MESSAGE_END);

        signal = ctx.output(channel.readOutbound());

        ByteBufAssertions.assertThat(signal)
                .containsShort(0x00)
                .hasNoRemainingReadableBytes()
                .hasReferences(1);

        channel.writeOutbound(FrameSignal.NOOP);

        signal = ctx.output(channel.readOutbound());

        ByteBufAssertions.assertThat(signal)
                .containsShort(0x00)
                .hasNoRemainingReadableBytes()
                .hasReferences(1);
    }

    @Test
    void shouldFilterSignals(StrictBufferContext ctx) {
        @SuppressWarnings("unchecked")
        var predicate = (Predicate<FrameSignal>) mock(Predicate.class);

        when(predicate.test(FrameSignal.NOOP)).thenReturn(true);

        var channel = ctx.channel(new FrameSignalEncoder(predicate));

        channel.writeOutbound(FrameSignal.NOOP);

        var signal = ctx.output(channel.<ByteBuf>readOutbound());

        ByteBufAssertions.assertThat(signal).hasNoRemainingReadableBytes();

        channel.writeOutbound(FrameSignal.MESSAGE_END);

        signal = ctx.output(channel.readOutbound());
        ByteBufAssertions.assertThat(signal).hasReadableBytes(2).hasReferences(1);

        verify(predicate).test(FrameSignal.NOOP);
        verify(predicate).test(FrameSignal.MESSAGE_END);
        verifyNoMoreInteractions(predicate);
    }
}
