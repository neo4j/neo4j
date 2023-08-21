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
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.embedded.EmbeddedChannel;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DynamicTest;
import org.junit.jupiter.api.TestFactory;

class ChunkFrameEncoderTest {

    private EmbeddedChannel channel;

    @BeforeEach
    void prepareChannel() {
        this.channel = new EmbeddedChannel(new ChunkFrameEncoder(64));
    }

    @TestFactory
    List<DynamicTest> shouldWrapMessages() {
        return IntStream.range(0, 128)
                .mapToObj(size -> {
                    var encoded = Unpooled.buffer(size);
                    for (var i = 0; i < size; ++i) {
                        encoded.writeByte(i % 128);
                    }
                    return encoded;
                })
                .map(payload -> DynamicTest.dynamicTest(payload.readableBytes() + " bytes", () -> {
                    this.channel.writeOutbound(payload.retainedSlice());
                    this.channel.checkException();

                    ByteBuf actual = this.channel.readOutbound();

                    try {
                        if (!payload.isReadable()) {
                            assertFalse(actual.isReadable());
                            return;
                        }

                        assertNotNull(actual);

                        var chunkLength = actual.readUnsignedShort();

                        if (payload.readableBytes() > 64) {
                            assertEquals(chunkLength, 64);
                            assertTrue(payload.readableBytes() > 64);

                            var slice = actual.readSlice(chunkLength);
                            assertEquals(payload.readSlice(chunkLength), slice);

                            chunkLength = actual.readUnsignedShort();

                            assertEquals(payload.readableBytes(), chunkLength);

                            slice = actual.readSlice(chunkLength);
                            assertEquals(payload.readSlice(chunkLength), slice);
                        } else {
                            assertEquals(chunkLength, payload.readableBytes());
                            assertEquals(payload, actual.readSlice(chunkLength));
                        }

                        assertFalse(actual.isReadable());
                        assertNull(this.channel.readOutbound());
                    } finally {
                        actual.release();
                    }
                }))
                .collect(Collectors.toList());
    }
}
