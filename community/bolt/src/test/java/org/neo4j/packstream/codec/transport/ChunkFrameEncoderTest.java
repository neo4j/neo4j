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
import static org.junit.jupiter.api.Assertions.assertNull;

import io.netty.buffer.ByteBuf;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.junit.jupiter.api.DynamicTest;
import org.junit.jupiter.api.TestFactory;
import org.neo4j.bolt.testing.annotation.StrictBufferExtension;
import org.neo4j.bolt.testing.assertions.ByteBufAssertions;
import org.neo4j.bolt.testing.channel.StrictBufferContext.RootStrictBufferContext;

@StrictBufferExtension
class ChunkFrameEncoderTest {

    private ChunkFrameEncoder createEncoder() {
        return new ChunkFrameEncoder(64);
    }

    @TestFactory
    List<DynamicTest> shouldWrapMessages(RootStrictBufferContext root) {
        return IntStream.range(0, 128)
                .mapToObj(size -> {
                    var encoded = root.scopedBuffer(size);
                    for (var i = 0; i < size; ++i) {
                        encoded.writeByte(i % 128);
                    }
                    return encoded;
                })
                .map(payload -> root.test(payload.readableBytes() + " bytes", (ctx) -> {
                    var channel = ctx.channel(this.createEncoder());

                    var inputBytes = ctx.input(payload);
                    channel.writeOutbound(inputBytes);
                    channel.checkException();

                    ByteBuf actual = ctx.output(channel.readOutbound());

                    if (!payload.isReadable()) {
                        ByteBufAssertions.assertThat(actual).hasNoRemainingReadableBytes();

                        return;
                    }

                    ByteBufAssertions.assertThat(actual).hasReadableBytes(2);
                    var chunkLength = actual.readUnsignedShort();

                    if (payload.readableBytes() > 64) {
                        var slice = actual.readSlice(chunkLength);

                        assertEquals(64, chunkLength);
                        ByteBufAssertions.assertThat(slice).hasReadableBytes(64).contains(slice);

                        chunkLength = actual.readUnsignedShort();
                        slice = actual.readSlice(chunkLength);

                        assertEquals(payload.readableBytes() - 64, chunkLength);
                        ByteBufAssertions.assertThat(slice)
                                .hasReadableBytes(chunkLength)
                                .contains(slice);
                    } else {
                        ByteBufAssertions.assertThat(actual)
                                .hasReadableBytes(payload.readableBytes())
                                .contains(payload);
                    }

                    ByteBufAssertions.assertThat(actual).hasNoRemainingReadableBytes();

                    assertNull(ctx.tracked(channel.readOutbound()));
                }))
                .collect(Collectors.toList());
    }
}
