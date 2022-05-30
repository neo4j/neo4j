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
package org.neo4j.bolt.protocol.v41.message.decoder;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatExceptionOfType;

import org.junit.jupiter.api.Test;
import org.neo4j.packstream.error.reader.PackstreamReaderException;
import org.neo4j.packstream.error.struct.IllegalStructArgumentException;
import org.neo4j.packstream.error.struct.IllegalStructSizeException;
import org.neo4j.packstream.io.PackstreamBuf;
import org.neo4j.packstream.struct.StructHeader;

class HelloMessageDecoderTest {

    @Test
    void shouldReadMessageWithoutRoutingContext() throws PackstreamReaderException {
        var buf = PackstreamBuf.allocUnpooled()
                .writeMapHeader(2)
                .writeString("user_agent")
                .writeString("Example/1.0 (+https://github.com/neo4j)")
                .writeString("scheme")
                .writeString("none");

        var msg = HelloMessageDecoder.getInstance().read(buf, new StructHeader(1, (short) 0x42));

        assertThat(msg).isNotNull();
        assertThat(msg.userAgent()).isEqualTo("Example/1.0 (+https://github.com/neo4j)");
        assertThat(msg.authToken()).hasSize(2).containsEntry("scheme", "none");
        assertThat(msg.routingContext()).isNotNull();
        assertThat(msg.routingContext().isServerRoutingEnabled()).isFalse();
    }

    @Test
    void shouldReadMessageWithRoutingContext() throws PackstreamReaderException {
        var buf = PackstreamBuf.allocUnpooled()
                .writeMapHeader(3)
                .writeString("user_agent")
                .writeString("Example/1.0 (+https://github.com/neo4j)")
                .writeString("scheme")
                .writeString("none")
                .writeString("routing")
                .writeMapHeader(1)
                .writeString("region")
                .writeString("eu-west");

        var msg = HelloMessageDecoder.getInstance().read(buf, new StructHeader(1, (short) 0x42));

        assertThat(msg).isNotNull();
        assertThat(msg.userAgent()).isEqualTo("Example/1.0 (+https://github.com/neo4j)");
        assertThat(msg.authToken()).hasSize(2).containsEntry("scheme", "none");
        assertThat(msg.routingContext()).isNotNull();
        assertThat(msg.routingContext().isServerRoutingEnabled()).isTrue();
        assertThat(msg.routingContext().parameters()).hasSize(1).containsEntry("region", "eu-west");
        assertThat(msg.authToken()).doesNotContainKey("routing").doesNotContainKey("region");
    }

    @Test
    void shouldReadMessageWithEmptyRoutingContext() throws PackstreamReaderException {
        var buf = PackstreamBuf.allocUnpooled()
                .writeMapHeader(3)
                .writeString("user_agent")
                .writeString("Example/1.0 (+https://github.com/neo4j)")
                .writeString("scheme")
                .writeString("none")
                .writeString("routing")
                .writeMapHeader(0);

        var msg = HelloMessageDecoder.getInstance().read(buf, new StructHeader(1, (short) 0x42));

        assertThat(msg).isNotNull();
        assertThat(msg.userAgent()).isEqualTo("Example/1.0 (+https://github.com/neo4j)");
        assertThat(msg.authToken()).hasSize(2).containsEntry("scheme", "none");
        assertThat(msg.routingContext()).isNotNull();
        assertThat(msg.routingContext().isServerRoutingEnabled()).isTrue();
        assertThat(msg.routingContext().parameters()).isEmpty();
    }

    @Test
    void shouldFailWithIllegalStructArgumentWhenInvalidRoutingContextValueIsPassed() {
        var buf = PackstreamBuf.allocUnpooled()
                .writeMapHeader(3)
                .writeString("user_agent")
                .writeString("Example/1.0 (+https://github.com/neo4j)")
                .writeString("scheme")
                .writeString("none")
                .writeString("routing")
                .writeInt(42);

        var decoder = HelloMessageDecoder.getInstance();

        // See work note in HelloMessageDecoder
        assertThatExceptionOfType(IllegalStructArgumentException.class)
                .isThrownBy(() -> decoder.read(buf, new StructHeader(1, (short) 0x42)));
    }

    @Test
    void shouldFailWithIllegalStructArgumentWhenInvalidRoutingContextContentIsPassed() {
        var buf = PackstreamBuf.allocUnpooled()
                .writeMapHeader(3)
                .writeString("user_agent")
                .writeString("Example/1.0 (+https://github.com/neo4j)")
                .writeString("scheme")
                .writeString("none")
                .writeString("routing")
                .writeMapHeader(1)
                .writeString("foo")
                .writeInt(42);

        var decoder = HelloMessageDecoder.getInstance();

        assertThatExceptionOfType(IllegalStructArgumentException.class)
                .isThrownBy(() -> decoder.read(buf, new StructHeader(1, (short) 0x42)))
                .withMessage("Illegal value for field \"routing\": Must be a map with string keys and string values.");
    }

    @Test
    void shouldFailWithIllegalStructSizeWhenEmptyStructIsGiven() {
        var decoder = org.neo4j.bolt.protocol.v40.messaging.decoder.HelloMessageDecoder.getInstance();

        assertThatExceptionOfType(IllegalStructSizeException.class)
                .isThrownBy(() -> decoder.read(PackstreamBuf.allocUnpooled(), new StructHeader(0, (short) 0x42)))
                .withMessage("Illegal struct size: Expected struct to be 1 fields but got 0");
    }

    @Test
    void shouldFailWithIllegalStructSizeWhenLargeStructIsGiven() {
        var decoder = org.neo4j.bolt.protocol.v40.messaging.decoder.HelloMessageDecoder.getInstance();

        assertThatExceptionOfType(IllegalStructSizeException.class)
                .isThrownBy(() -> decoder.read(PackstreamBuf.allocUnpooled(), new StructHeader(2, (short) 0x42)))
                .withMessage("Illegal struct size: Expected struct to be 1 fields but got 2");
    }
}
