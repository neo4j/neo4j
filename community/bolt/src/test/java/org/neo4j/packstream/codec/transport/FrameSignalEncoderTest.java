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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.DynamicTest.dynamicTest;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.embedded.EmbeddedChannel;
import java.util.function.Predicate;
import java.util.stream.Stream;
import org.junit.jupiter.api.DynamicTest;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestFactory;
import org.neo4j.packstream.signal.FrameSignal;

class FrameSignalEncoderTest {

    @TestFactory
    Stream<DynamicTest> shouldEncodeSignals() {
        return Stream.of(FrameSignal.values())
                .map(signal -> dynamicTest(signal.name(), () -> {
                    var channel = new EmbeddedChannel(new FrameSignalEncoder());

                    channel.writeOutbound(signal);

                    ByteBuf buf = channel.readOutbound();

                    assertNotNull(buf);
                    assertEquals(0x00, buf.readUnsignedShort());
                    assertFalse(buf.isReadable());
                }));
    }

    @Test
    void shouldIgnoreSignalsWhenInsideOfMessage() {
        var channel = new EmbeddedChannel(new FrameSignalEncoder());

        channel.writeOutbound(Unpooled.buffer(1).writeByte(0x42));

        ByteBuf payload = channel.readOutbound();

        assertNotNull(payload);
        assertEquals(0x42, payload.readByte());
        assertFalse(payload.isReadable());

        channel.writeOutbound(FrameSignal.NOOP);

        ByteBuf signal = channel.readOutbound();

        assertFalse(signal.isReadable());

        channel.writeOutbound(FrameSignal.MESSAGE_END);

        signal = channel.readOutbound();

        assertEquals(0x00, signal.readShort());
        assertFalse(signal.isReadable());

        channel.writeOutbound(FrameSignal.NOOP);

        signal = channel.readOutbound();

        assertEquals(0x00, signal.readShort());
        assertFalse(signal.isReadable());
    }

    @Test
    void shouldFilterSignals() {
        @SuppressWarnings("unchecked")
        var predicate = (Predicate<FrameSignal>) mock(Predicate.class);

        when(predicate.test(FrameSignal.NOOP)).thenReturn(true);

        var channel = new EmbeddedChannel(new FrameSignalEncoder(predicate));

        channel.writeOutbound(FrameSignal.NOOP);

        ByteBuf signal = channel.readOutbound();

        assertFalse(signal.isReadable());

        channel.writeOutbound(FrameSignal.MESSAGE_END);

        signal = channel.readOutbound();

        assertTrue(signal.isReadable(2));

        verify(predicate).test(FrameSignal.NOOP);
        verify(predicate).test(FrameSignal.MESSAGE_END);
        verifyNoMoreInteractions(predicate);
    }
}
