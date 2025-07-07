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
package org.neo4j.bolt.protocol.common.fsm.response;

import io.netty.buffer.ByteBuf;
import io.netty.util.ReferenceCounted;
import java.util.function.Consumer;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;
import org.neo4j.bolt.protocol.io.pipeline.WriterContext;
import org.neo4j.bolt.testing.annotation.StrictBufferExtension;
import org.neo4j.bolt.testing.channel.StrictBufferContext;
import org.neo4j.bolt.testing.mock.ConnectionMockFactory;
import org.neo4j.packstream.signal.FrameSignal;
import org.neo4j.packstream.testing.PackstreamBufAssertions;
import org.neo4j.values.storable.BooleanValue;

@StrictBufferExtension
class NetworkRecordHandlerTest {

    @Test
    void shouldWriteStructHeaderOnBegin(StrictBufferContext ctx) {
        var ch = ctx.channel();

        var connection = ConnectionMockFactory.newFactory().attachTo(ch);

        try (var handler = new NetworkRecordHandler(connection, 2, 512, 0)) {
            handler.onBegin();

            // ensure that begin does not immediately flush as the record has yet to be completed
            Assertions.assertThat(ctx.tracked(ch.<ReferenceCounted>readOutbound()))
                    .isNull();

            Mockito.verify(connection).allocator();
            Mockito.verify(connection).writerContext(Mockito.notNull());
            Mockito.verifyNoMoreInteractions(connection);

            // abnormal completion - handlers do not validate call order thus permitting partial
            // validation
            handler.onCompleted();

            var buffer = ctx.output(ch.<ByteBuf>readOutbound());

            Assertions.assertThat(buffer)
                    .isNotNull()
                    .asInstanceOf(PackstreamBufAssertions.wrap())
                    .containsStruct(0x71, 1)
                    .containsListHeader(2)
                    .asBuffer()
                    .hasNoRemainingReadableBytes();

            var signal = ch.<FrameSignal>readOutbound();

            Assertions.assertThat(signal).isNotNull().isEqualTo(FrameSignal.MESSAGE_END);
        }
    }

    @Test
    void shouldWriteFields(StrictBufferContext ctx) {
        var ch = ctx.channel();

        var connection = ConnectionMockFactory.newFactory().attachTo(ch);

        var writer = Mockito.mock(WriterContext.class);

        Mockito.doReturn(writer).when(connection).writerContext(Mockito.any());

        try (var handler = new NetworkRecordHandler(connection, 2, 512, 0)) {
            handler.onBegin();

            // ensure that begin does not immediately flush as the record has yet to be completed
            Assertions.assertThat(ctx.tracked(ch.<ReferenceCounted>readOutbound()))
                    .isNull();

            Mockito.verify(connection).allocator();
            Mockito.verify(connection).writerContext(Mockito.notNull());
            Mockito.verifyNoMoreInteractions(connection);

            handler.onField(BooleanValue.TRUE);

            Mockito.verify(writer).writeValue(BooleanValue.TRUE);
        }
    }

    void verifyFlushesPendingMessages(StrictBufferContext ctx, Consumer<NetworkRecordHandler> closeFunction) {
        var ch = ctx.channel();

        var connection = ConnectionMockFactory.newFactory().attachTo(ch);

        try (var handler = new NetworkRecordHandler(connection, 4, 512, 8192)) {
            for (var i = 0; i < 2; ++i) {
                handler.onBegin();
                handler.onCompleted();
            }

            // dangling record - should not end up in result
            handler.onBegin();

            Mockito.verify(connection, Mockito.never()).write(Mockito.any());
            Mockito.verify(connection, Mockito.never()).writeAndFlush(Mockito.any());

            closeFunction.accept(handler);

            // implicit flush as all close functions expect a follow-up flush call
            ch.flush();

            for (var i = 0; i < 2; ++i) {
                var buffer = ctx.output(ch.<ByteBuf>readOutbound());
                Assertions.assertThat(buffer)
                        .isNotNull()
                        .asInstanceOf(PackstreamBufAssertions.wrap())
                        .containsStruct(0x71, 1)
                        .containsListHeader(4)
                        .asBuffer()
                        .hasNoRemainingReadableBytes();

                var signal = ch.<FrameSignal>readOutbound();

                Assertions.assertThat(signal).isEqualTo(FrameSignal.MESSAGE_END);
            }

            // dangling record should not be written
            var buffer = ctx.tracked(ch.<ByteBuf>readOutbound());

            Assertions.assertThat(buffer).isNull();
        }
    }

    @Test
    void shouldFlushPendingRecordsOnClose(StrictBufferContext ctx) {
        this.verifyFlushesPendingMessages(ctx, NetworkRecordHandler::close);
    }

    @Test
    void shouldFlushPendingRecordsOnFailure(StrictBufferContext ctx) {
        this.verifyFlushesPendingMessages(ctx, NetworkRecordHandler::onFailure);
    }
}
