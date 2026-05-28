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
package org.neo4j.bolt.negotiation.codec;

import io.netty.buffer.ByteBuf;
import java.util.EnumSet;
import java.util.List;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;
import org.neo4j.bolt.negotiation.message.ModernProtocolNegotiationInitMessage;
import org.neo4j.bolt.negotiation.message.ProtocolCapability;
import org.neo4j.bolt.negotiation.version.ProtocolVersion;
import org.neo4j.bolt.testing.annotation.StrictBufferExtension;
import org.neo4j.bolt.testing.assertions.ByteBufAssertions;
import org.neo4j.bolt.testing.assertions.ProtocolVersionAssertions;
import org.neo4j.bolt.testing.channel.StrictBufferContext;

@StrictBufferExtension
class ModernProtocolNegotiationInitMessageEncoderTest {

    @Test
    void shouldEncodeMessage(StrictBufferContext ctx) {
        var channel = ctx.channel(new ModernProtocolNegotiationInitMessageEncoder());

        var capabilities = EnumSet.noneOf(ProtocolCapability.class);
        capabilities.add(ProtocolCapability.FABRIC);

        channel.writeOutbound(new ModernProtocolNegotiationInitMessage(
                new ProtocolVersion(ProtocolVersion.MAX_MAJOR_BIT, 2, 0),
                List.of(
                        new ProtocolVersion(4, 2),
                        new ProtocolVersion(4, 3),
                        new ProtocolVersion(5, 5),
                        new ProtocolVersion(5, 6),
                        new ProtocolVersion(5, 8),
                        new ProtocolVersion(5, 9),
                        new ProtocolVersion(5, 11)),
                capabilities));

        var buffer = ctx.output(channel.<ByteBuf>readOutbound());

        ByteBufAssertions.assertThat(buffer).isNotNull().hasReadableBytes(9);

        var version = new ProtocolVersion(buffer.readInt());

        ProtocolVersionAssertions.assertThat(version)
                .hasMajor(ProtocolVersion.MAX_MAJOR_BIT)
                .hasMinor(2)
                .hasRange(0);

        Assertions.assertThat(buffer.readUnsignedByte()).isEqualTo((short) 4);

        ProtocolVersionAssertions.assertThat(new ProtocolVersion(buffer.readInt()))
                .hasMajor(4)
                .hasMinor(3)
                .hasRange(1);

        ProtocolVersionAssertions.assertThat(new ProtocolVersion(buffer.readInt()))
                .hasMajor(5)
                .hasMinor(6)
                .hasRange(1);

        ProtocolVersionAssertions.assertThat(new ProtocolVersion(buffer.readInt()))
                .hasMajor(5)
                .hasMinor(9)
                .hasRange(1);

        ProtocolVersionAssertions.assertThat(new ProtocolVersion(buffer.readInt()))
                .hasMajor(5)
                .hasMinor(11)
                .representsSingleVersion();

        Assertions.assertThat(buffer.readUnsignedByte()).isEqualTo((short) 1);
    }
}
