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

import static org.junit.jupiter.api.Assertions.assertNull;

import io.netty.buffer.ByteBuf;
import io.netty.handler.codec.DecoderException;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.DynamicTest;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestFactory;
import org.neo4j.bolt.testing.annotation.StrictBufferExtension;
import org.neo4j.bolt.testing.assertions.ByteBufAssertions;
import org.neo4j.bolt.testing.channel.StrictBufferContext;
import org.neo4j.bolt.testing.channel.StrictBufferContext.RootStrictBufferContext;
import org.neo4j.logging.NullLogProvider;
import org.neo4j.packstream.error.reader.LimitExceededException;
import org.neo4j.packstream.io.PackstreamBuf;

@StrictBufferExtension
class ChunkFrameDecoderTest {

    private ChunkFrameDecoder createDecoder() {
        return new ChunkFrameDecoder(128, NullLogProvider.getInstance());
    }

    /**
     * Evaluates whether the implementation correctly decodes chunks which are entirely self enclosed
     * and ready to be read off the wire.
     */
    @TestFactory
    List<DynamicTest> shouldDecodeSelfEnclosedChunks(RootStrictBufferContext root) {
        return IntStream.range(1, 128)
                .mapToObj(size -> {
                    var payload = root.scopedBuffer();
                    for (var i = 0; i < size; ++i) {
                        payload.writeByte(i % 128);
                    }
                    return payload;
                })
                .map(expected -> root.test(expected.readableBytes() + " bytes", (ctx) -> {
                    var channel = ctx.channel(this.createDecoder());

                    // allocate as output buffer since this handler is expected to be chained
                    // resulting in a reusable buffer at the end of the run
                    var encoded = ctx.outputBuffer()
                            .writeShort(expected.readableBytes())
                            .writeBytes(expected.duplicate())
                            .writeShort(0x0000);

                    channel.writeInbound(encoded);
                    channel.checkException();

                    var actual = ctx.output(channel.<PackstreamBuf>readInbound().getTarget());

                    ByteBufAssertions.assertThat(actual).contains(expected).hasNoRemainingReadableBytes();
                }))
                .collect(Collectors.toList());
    }

    /**
     * Evaluates whether the implementation correctly decodes fragmented messages which are
     * immediately available to be read off the wire.
     */
    @TestFactory
    List<DynamicTest> shouldDecodeFragmentedMessages(RootStrictBufferContext root) {
        return IntStream.range(4, 64)
                .map(i -> i * 2)
                .<ByteBuf>mapToObj(
                        size -> { // IntelliJ does not like this syntax
                            var payload = root.scopedBuffer(size);
                            for (var i = 0; i < size; ++i) {
                                payload.writeByte(i % 128);
                            }
                            return payload;
                        })
                .map(expected -> root.test(expected.readableBytes() + " bytes", (ctx) -> {
                    var channel = ctx.channel(this.createDecoder());
                    var fragmentedSize = expected.readableBytes() / 2;

                    var encoded = ctx.scopedBuffer()
                            .writeShort(fragmentedSize)
                            .writeBytes(expected.slice(0, fragmentedSize))
                            .writeShort(fragmentedSize)
                            .writeBytes(expected.slice(fragmentedSize, fragmentedSize))
                            .writeShort(0x0000);

                    channel.writeInbound(encoded);
                    channel.checkException();

                    var actual = ctx.output(channel.<PackstreamBuf>readInbound().getTarget());

                    ByteBufAssertions.assertThat(actual).contains(expected).hasNoRemainingReadableBytes();

                    // ensure the input buffer still has two references as a result of being sliced
                    // up
                    ByteBufAssertions.assertThat(encoded).hasReferences(2);
                }))
                .collect(Collectors.toList());
    }

    /**
     * Evaluates whether the implementation correctly decodes fragmented messages which are only
     * partially available during the first decoder invocation.
     */
    @TestFactory
    List<DynamicTest> shouldDecodeDelayedFragmentedMessages(RootStrictBufferContext root) {
        return IntStream.range(4, 64)
                .map(i -> i * 2)
                .<ByteBuf>mapToObj(
                        size -> { // IntelliJ has issues understanding this ...
                            var payload = root.scopedBuffer(size);
                            for (var i = 0; i < size; ++i) {
                                payload.writeByte(i % 128);
                            }
                            return payload;
                        })
                .map(expected -> root.test(expected.readableBytes() + " bytes", (ctx) -> {
                    var channel = ctx.channel(this.createDecoder());
                    var fragmentedSize = expected.readableBytes() / 2;

                    var encoded1 = ctx.tracked(channel.alloc().buffer(), 2)
                            .writeShort(fragmentedSize)
                            .writeBytes(expected.slice(0, fragmentedSize));
                    var encoded2 = ctx.buffer()
                            .writeShort(fragmentedSize)
                            .writeBytes(expected.slice(fragmentedSize, fragmentedSize));
                    var encoded3 = ctx.buffer().writeShort(0x0000);

                    channel.writeInbound(encoded1);
                    channel.checkException();

                    ByteBuf actual = ctx.output(channel.readInbound());

                    ByteBufAssertions.assertThat(actual).isNull();

                    channel.writeInbound(encoded2);
                    channel.checkException();

                    actual = ctx.tracked(channel.readInbound());

                    ByteBufAssertions.assertThat(actual).isNull();

                    channel.writeInbound(encoded3);
                    channel.checkException();

                    actual = ctx.output(channel.<PackstreamBuf>readInbound().getTarget());

                    ByteBufAssertions.assertThat(actual).contains(expected).hasNoRemainingReadableBytes();

                    ByteBufAssertions.assertThat(expected).hasReferences(1);

                    ByteBufAssertions.assertThat(actual).hasReferences(1);
                }))
                .collect(Collectors.toList());
    }

    /**
     * Evaluates whether the implementation ignores empty standalone chunks (e.g. keep-alive chunks).
     */
    @Test
    void shouldIgnoreEmptyStandaloneChunks(StrictBufferContext ctx) {
        var channel = ctx.channel(this.createDecoder());

        var payload = ctx.buffer(256);
        for (var i = 0; i < 128; ++i) {
            payload.writeShort(0x0000);
        }

        channel.writeInbound(payload);
        channel.checkException();

        var actual = ctx.output(channel.readInbound());
        assertNull(actual);
    }

    /**
     * Evaluates whether the implementation ignores additional empty chunks between messages (e.g.
     * keep-alive chunks).
     */
    @Test
    void shouldIgnoreEmptyDelimitingChunks(StrictBufferContext ctx) {
        var channel = ctx.channel(this.createDecoder());

        var payload = ctx.scopedBuffer(8)
                .writeShort(0x08)
                .writeBytes(new byte[] {0x00, 0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07})
                .writeShort(0x0000)
                .writeShort(0x0000)
                .writeShort(0x08)
                .writeBytes(new byte[] {0x00, 0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07})
                .writeShort(0x0000)
                .writeShort(0x0000);

        channel.writeInbound(payload);
        channel.checkException();

        var firstMessage = ctx.output(channel.<PackstreamBuf>readInbound().getTarget());
        var secondMessage = ctx.output(channel.<PackstreamBuf>readInbound().getTarget());
        var nullMessage = ctx.output(channel.<PackstreamBuf>readInbound());

        ByteBufAssertions.assertThat(firstMessage).hasReadableBytes(8);
        ByteBufAssertions.assertThat(secondMessage).hasReadableBytes(8);
        ByteBufAssertions.assertThat(nullMessage).isNull();

        // we're using a scoped buffer for simplicity - ensure that the buffer retains exactly two
        // references (one for each message)
        ByteBufAssertions.assertThat(payload).hasReferences(2);
    }

    @Test
    void shouldFailWithLimitExceededWhenLargePayloadIsGiven(StrictBufferContext ctx) {
        var channel = ctx.channel(this.createDecoder());

        var payload = ctx.buffer().writeShort(64).writeBytes(new byte[64]).writeShort(65);

        Assertions.assertThatExceptionOfType(DecoderException.class)
                .isThrownBy(() -> channel.writeInbound(payload))
                .withRootCauseInstanceOf(LimitExceededException.class);
    }
}
