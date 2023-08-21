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
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.DynamicTest.dynamicTest;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.embedded.EmbeddedChannel;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DynamicTest;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestFactory;
import org.neo4j.logging.NullLogProvider;
import org.neo4j.packstream.io.PackstreamBuf;

class ChunkFrameDecoderTest {

    private EmbeddedChannel channel;

    @BeforeEach
    void prepareChannel() {
        this.channel = new EmbeddedChannel(new ChunkFrameDecoder(128, NullLogProvider.getInstance()));
    }

    /**
     * Evaluates whether the implementation correctly decodes chunks which are entirely self enclosed and ready to be read off the wire.
     */
    @TestFactory
    List<DynamicTest> shouldDecodeSelfEnclosedChunks() {
        return IntStream.range(1, 128)
                .mapToObj(size -> {
                    var payload = Unpooled.buffer(size);
                    for (var i = 0; i < size; ++i) {
                        payload.writeByte(i % 128);
                    }
                    return payload;
                })
                .map(expected -> dynamicTest(expected.readableBytes() + " bytes", () -> {
                    var encoded = Unpooled.buffer()
                            .writeShort(expected.readableBytes())
                            .writeBytes(expected.slice())
                            .writeShort(0x0000);

                    this.channel.writeInbound(encoded);
                    this.channel.checkException();

                    var actual = this.channel.<PackstreamBuf>readInbound().getTarget();

                    assertNotNull(actual);
                    assertEquals(expected, actual);

                    assertEquals(1, actual.refCnt());
                    expected.release();
                    actual.release();
                }))
                .collect(Collectors.toList());
    }

    /**
     * Evaluates whether the implementation correctly decodes fragmented messages which are immediately available to be read off the wire.
     */
    @TestFactory
    List<DynamicTest> shouldDecodeFragmentedMessages() {
        return IntStream.range(4, 64)
                .map(i -> i * 2)
                .mapToObj(size -> {
                    var payload = Unpooled.buffer(size);
                    for (var i = 0; i < size; ++i) {
                        payload.writeByte(i % 128);
                    }
                    return payload;
                })
                .map(expected -> dynamicTest(expected.readableBytes() + " bytes", () -> {
                    var fragmentedSize = expected.readableBytes() / 2;

                    var encoded = Unpooled.buffer()
                            .writeShort(fragmentedSize)
                            .writeBytes(expected.slice(0, fragmentedSize))
                            .writeShort(fragmentedSize)
                            .writeBytes(expected.slice(fragmentedSize, fragmentedSize))
                            .writeShort(0x0000);

                    this.channel.writeInbound(encoded);
                    this.channel.checkException();

                    var actual = this.channel.<PackstreamBuf>readInbound().getTarget();

                    assertNotNull(actual);
                    assertEquals(expected, actual);
                }))
                .collect(Collectors.toList());
    }

    /**
     * Evaluates whether the implementation correctly decodes fragmented messages which are only partially available during the first decoder invocation.
     */
    @TestFactory
    List<DynamicTest> shouldDecodeDelayedFragmentedMessages() {
        return IntStream.range(4, 64)
                .map(i -> i * 2)
                .mapToObj(size -> {
                    var payload = Unpooled.buffer(size);
                    for (var i = 0; i < size; ++i) {
                        payload.writeByte(i % 128);
                    }
                    return payload;
                })
                .map(expected -> dynamicTest(expected.readableBytes() + " bytes", () -> {
                    var fragmentedSize = expected.readableBytes() / 2;

                    var encoded1 =
                            Unpooled.buffer().writeShort(fragmentedSize).writeBytes(expected.slice(0, fragmentedSize));
                    var encoded2 = Unpooled.buffer()
                            .writeShort(fragmentedSize)
                            .writeBytes(expected.slice(fragmentedSize, fragmentedSize));
                    var encoded3 = Unpooled.buffer().writeShort(0x0000);

                    this.channel.writeInbound(encoded1);
                    this.channel.checkException();

                    ByteBuf actualIncomplete = this.channel.readInbound();

                    assertNull(actualIncomplete);

                    this.channel.writeInbound(encoded2);
                    this.channel.checkException();

                    actualIncomplete = this.channel.readInbound();

                    assertNull(actualIncomplete);

                    this.channel.writeInbound(encoded3);
                    this.channel.checkException();

                    var actualComplete =
                            this.channel.<PackstreamBuf>readInbound().getTarget();

                    assertNotNull(actualComplete);
                    assertEquals(expected, actualComplete);
                }))
                .collect(Collectors.toList());
    }

    /**
     * Evaluates whether the implementation ignores empty standalone chunks (e.g. keep-alive chunks).
     */
    @Test
    void shouldIgnoreEmptyStandaloneChunks() {
        var payload = Unpooled.buffer(256);
        for (var i = 0; i < 128; ++i) {
            payload.writeShort(0x0000);
        }

        this.channel.writeInbound(payload);
        this.channel.checkException();

        var actual = this.channel.readInbound();
        assertNull(actual);
    }

    /**
     * Evaluates whether the implementation ignores additional empty chunks between messages (e.g. keep-alive chunks).
     */
    @Test
    void shouldIgnoreEmptyDelimitingChunks() {
        var payload = Unpooled.buffer(8)
                .writeShort(0x08)
                .writeBytes(new byte[] {0x00, 0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07})
                .writeShort(0x0000)
                .writeShort(0x0000)
                .writeShort(0x08)
                .writeBytes(new byte[] {0x00, 0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07})
                .writeShort(0x0000)
                .writeShort(0x0000);

        this.channel.writeInbound(payload);
        this.channel.checkException();

        var firstMessage = this.channel.<PackstreamBuf>readInbound().getTarget();
        var secondMessage = this.channel.<PackstreamBuf>readInbound().getTarget();
        var nullMessage = this.channel.<PackstreamBuf>readInbound();

        assertNotNull(firstMessage);
        assertEquals(8, firstMessage.readableBytes());
        assertNotNull(secondMessage);
        assertEquals(8, secondMessage.readableBytes());
        assertNull(nullMessage);
    }

    @Test
    void shouldFailWithLimitExceededWhenLargePayloadIsGiven() {
        var payload = Unpooled.buffer(129).writerIndex(128);

        this.channel.writeInbound(payload);
        this.channel.checkException();
    }
}
