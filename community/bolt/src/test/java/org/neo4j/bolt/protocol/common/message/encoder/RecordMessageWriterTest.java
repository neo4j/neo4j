/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
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
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.bolt.protocol.common.message.encoder;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.DynamicTest.dynamicTest;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.RETURNS_MOCKS;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.channel.Channel;
import io.netty.channel.ChannelPromise;
import java.io.IOException;
import java.util.stream.Stream;
import org.junit.jupiter.api.DynamicTest;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestFactory;
import org.mockito.ArgumentCaptor;
import org.neo4j.bolt.protocol.common.message.result.ResponseHandler;
import org.neo4j.bolt.protocol.common.signal.MessageSignal;
import org.neo4j.packstream.io.PackstreamBuf;
import org.neo4j.packstream.io.Type;
import org.neo4j.packstream.io.value.PackstreamValues;
import org.neo4j.values.AnyValue;
import org.neo4j.values.storable.Values;

class RecordMessageWriterTest {

    private static final short RECORD_TAG = 0x71;

    @Test
    void shouldBeginRecord() throws IOException {
        var channel = mock(Channel.class);
        var parent = mock(ResponseHandler.class);

        var captor = ArgumentCaptor.forClass(ByteBuf.class);

        when(channel.alloc()).thenReturn(ByteBufAllocator.DEFAULT);
        when(channel.write(captor.capture())).thenReturn(mock(ChannelPromise.class));

        var writer = new RecordMessageWriter(channel, parent);

        writer.beginRecord(42);

        verify(channel).alloc();
        verify(channel).write(any(ByteBuf.class));
        verifyNoMoreInteractions(channel);

        var buf = PackstreamBuf.wrap(captor.getValue());

        var header = buf.readStructHeader();
        var listHeader = buf.readLengthPrefixMarker(Type.LIST, -1);

        assertThat(header.tag()).isEqualTo(RECORD_TAG);
        assertThat(header.length()).isEqualTo(1);

        assertThat(listHeader).isEqualTo(42);
    }

    @TestFactory
    Stream<DynamicTest> shouldConsumeField() {
        return Stream.<AnyValue>of(
                        Values.booleanValue(true),
                        Values.byteValue(Byte.MAX_VALUE),
                        Values.shortValue((short) (Byte.MAX_VALUE + 1)),
                        Values.intValue(Short.MAX_VALUE + 1),
                        Values.longValue(Integer.MAX_VALUE + 1L),
                        Values.stringValue("foo"))
                .map(expected -> dynamicTest(expected.getTypeName(), () -> {
                    var channel = mock(Channel.class);
                    var parent = mock(ResponseHandler.class);

                    var captor = ArgumentCaptor.forClass(ByteBuf.class);

                    when(channel.alloc()).thenReturn(ByteBufAllocator.DEFAULT);
                    when(channel.write(captor.capture())).thenReturn(mock(ChannelPromise.class));

                    var writer = new RecordMessageWriter(channel, parent);

                    writer.consumeField(expected);

                    verify(channel).alloc();
                    verify(channel).write(any(ByteBuf.class));
                    verifyNoMoreInteractions(channel);

                    var buf = PackstreamBuf.wrap(captor.getValue());
                    var actual = PackstreamValues.readValue(buf);

                    assertThat(actual).isEqualTo(expected);
                }));
    }

    @Test
    void shouldEndMessageOnRecordEnd() throws IOException {
        var channel = mock(Channel.class, RETURNS_MOCKS);
        var parent = mock(ResponseHandler.class);

        var writer = new RecordMessageWriter(channel, parent);

        writer.endRecord();

        verify(channel).write(MessageSignal.END);
    }

    @Test
    void shouldResetOnError() throws IOException {
        var channel = mock(Channel.class, RETURNS_MOCKS);
        var parent = mock(ResponseHandler.class);

        var writer = new RecordMessageWriter(channel, parent);

        writer.onError();

        verify(channel).write(MessageSignal.RESET);
    }

    @Test
    void shouldInvokeParentWhenMetadataIsPassed() {
        var channel = mock(Channel.class);
        var parent = mock(ResponseHandler.class);
        var child = new RecordMessageWriter(channel, parent);

        child.addMetadata("the_answer", Values.longValue(42));
        child.addMetadata("foo", Values.stringValue("bar"));

        verifyNoInteractions(channel);

        verify(parent).onMetadata("the_answer", Values.longValue(42));
        verify(parent).onMetadata("foo", Values.stringValue("bar"));
        verifyNoMoreInteractions(parent);
    }
}
