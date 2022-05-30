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
package org.neo4j.bolt.protocol.common.handler;

import static org.assertj.core.api.Assertions.assertThat;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.embedded.EmbeddedChannel;
import org.junit.jupiter.api.Test;
import org.neo4j.bolt.protocol.common.error.AccumulatorResetException;
import org.neo4j.bolt.protocol.common.signal.MessageSignal;
import org.neo4j.bolt.testing.assertions.ByteBufAssertions;
import org.neo4j.packstream.io.PackstreamBuf;

class RecordResponseAccumulatorTest {

    @Test
    void shouldAccumulatePayloads() {
        var buf1 = Unpooled.buffer(3).writeByte(1).writeByte(2).writeByte(3);
        var buf2 = Unpooled.buffer(3).writeByte(11).writeByte(12).writeByte(13);

        var channel = new EmbeddedChannel(new RecordResponseAccumulator());

        var promise1 = channel.write(buf1);
        assertThat(promise1.isDone()).isFalse();
        assertThat(channel.<ByteBuf>readOutbound()).isNull();

        var promise2 = channel.write(buf2);
        assertThat(promise2.isDone()).isFalse();
        assertThat(channel.<ByteBuf>readOutbound()).isNull();

        channel.flush();

        assertThat(promise1.isDone()).isFalse();
        assertThat(promise2.isDone()).isFalse();

        channel.writeAndFlush(MessageSignal.END);

        var chunk = channel.<ByteBuf>readOutbound();

        ByteBufAssertions.assertThat(chunk)
                .hasReadableBytes(6)
                .containsByte(1)
                .containsByte(2)
                .containsByte(3)
                .containsByte(11)
                .containsByte(12)
                .containsByte(13)
                .hasNoRemainingReadableBytes();

        assertThat(promise1.isDone()).isTrue();
        assertThat(promise1.isSuccess()).isTrue();

        assertThat(promise2.isDone()).isTrue();
        assertThat(promise2.isSuccess()).isTrue();
    }

    @Test
    void shouldDiscardPayloads() {
        var buf1 = Unpooled.buffer(3);
        var buf2 = Unpooled.buffer(3);

        PackstreamBuf.wrap(buf1).writeInt(1).writeInt(2).writeInt(3);
        PackstreamBuf.wrap(buf2).writeInt(11).writeInt(12).writeInt(13);

        var channel = new EmbeddedChannel(new RecordResponseAccumulator());

        var promise1 = channel.write(buf1);
        assertThat(promise1.isDone()).isFalse();
        assertThat(channel.<ByteBuf>readOutbound()).isNull();

        var promise2 = channel.write(buf2);
        assertThat(promise2.isDone()).isFalse();
        assertThat(channel.<ByteBuf>readOutbound()).isNull();

        channel.flush();

        assertThat(promise1.isDone()).isFalse();
        assertThat(promise2.isDone()).isFalse();

        channel.writeAndFlush(MessageSignal.RESET);

        var chunk = channel.<ByteBuf>readOutbound();

        assertThat(chunk).isNull();

        assertThat(promise1.isDone()).isTrue();
        assertThat(promise1.isSuccess()).isFalse();
        assertThat(promise1.cause()).isInstanceOf(AccumulatorResetException.class);

        assertThat(promise2.isDone()).isTrue();
        assertThat(promise2.isSuccess()).isFalse();
        assertThat(promise2.cause()).isInstanceOf(AccumulatorResetException.class);
    }

    @Test
    void shouldIgnoreUnknownPayloadType() {
        var channel = new EmbeddedChannel(new RecordResponseAccumulator());

        channel.writeOutbound(42);

        assertThat(channel.<Integer>readOutbound()).isEqualTo(42);
    }
}
