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
package org.neo4j.bolt.protocol.common.handler;

import io.netty.buffer.Unpooled;
import io.netty.channel.embedded.EmbeddedChannel;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.neo4j.bolt.protocol.error.ClientRequestComplexityExceeded;
import org.neo4j.packstream.error.reader.PackstreamReaderException;
import org.neo4j.packstream.io.PackstreamBuf;
import org.neo4j.packstream.struct.StructHeader;

class AuthenticationProtocolLimiterHandlerTest {

    private EmbeddedChannel channel;

    @BeforeEach
    void prepareChannel() {
        this.channel = new EmbeddedChannel(new AuthenticationProtocolLimiterHandler(64, 4));
    }

    @Test
    void shouldPassEmptyMessages() {
        var msg = PackstreamBuf.allocUnpooled().writeStructHeader(new StructHeader(0, (short) 0x42));

        this.channel.writeInbound(msg.getTarget());
        this.channel.checkException();

        var received = this.channel.readInbound();

        Assertions.assertThat(received).isSameAs(msg.getTarget());
    }

    @Test
    void shouldPassEmptyBuffers() {
        var msg = PackstreamBuf.allocUnpooled();

        this.channel.writeInbound(msg.getTarget());
        this.channel.checkException();

        var received = this.channel.readInbound();

        Assertions.assertThat(received).isSameAs(msg.getTarget());
    }

    @Test
    void shouldRejectInvalidRoots() {
        var msg = PackstreamBuf.allocUnpooled()
                .writeMapHeader(1)
                .writeString("foo")
                .writeBoolean(false);

        Assertions.assertThatExceptionOfType(PackstreamReaderException.class)
                .isThrownBy(() -> {
                    this.channel.writeInbound(msg.getTarget());
                    this.channel.checkException();
                })
                .withMessage("Encountered illegal root element: Expected struct");
    }

    @Test
    void shouldPassSimpleMessages() {
        var msg = PackstreamBuf.allocUnpooled()
                .writeStructHeader(new StructHeader(6, (short) 0x42))
                .writeNull()
                .writeBytes(Unpooled.wrappedBuffer(new byte[] {21, 42, 84}))
                .writeBoolean(true)
                .writeFloat(42.25)
                .writeInt(42)
                .writeString("foo");

        this.channel.writeInbound(msg.getTarget());
        this.channel.checkException();

        var received = this.channel.readInbound();

        Assertions.assertThat(received).isSameAs(msg.getTarget());
    }

    @Test
    void shouldPassEmptyCollections() {
        var msg = PackstreamBuf.allocUnpooled()
                .writeStructHeader(new StructHeader(3, (short) 0x42))
                .writeMapHeader(0)
                .writeListHeader(0)
                .writeStructHeader(new StructHeader(0, (short) 0x21));

        this.channel.writeInbound(msg.getTarget());
        this.channel.checkException();

        var received = this.channel.readInbound();

        Assertions.assertThat(received).isSameAs(msg.getTarget());
    }

    @Test
    void shouldPassComplexNestedMapsMessages() {
        var msg = PackstreamBuf.allocUnpooled()
                .writeStructHeader(new StructHeader(3, (short) 0x42))
                // Struct #0
                .writeMapHeader(3)
                // foo => false
                .writeString("foo")
                .writeBoolean(false)
                // bar => true, false, true, false
                .writeString("bar")
                .writeListHeader(4)
                .writeBoolean(true)
                .writeBoolean(false)
                .writeBoolean(true)
                .writeBoolean(false)
                // baz => { ... }
                .writeString("baz")
                .writeMapHeader(2)

                // the_answer => 42
                .writeString("the_answer")
                .writeInt(42)
                // not_the_answer => 21.25
                .writeString("not_the_answer")
                .writeFloat(21.25)

                // Struct #1
                .writeStructHeader(new StructHeader(4, (short) 0x21))
                .writeString("foo")
                .writeMapHeader(2)
                // "foo" => "bar"
                .writeString("foo")
                .writeString("bar")
                // "baz" => 42
                .writeString("baz")
                .writeInt(42)
                .writeListHeader(3)
                .writeBoolean(false)
                .writeBoolean(true)
                .writeBoolean(false)
                .writeNull()

                // Struct #2
                .writeString("fizz");

        this.channel.writeInbound(msg.getTarget());
        this.channel.checkException();

        var received = this.channel.readInbound();

        Assertions.assertThat(received).isSameAs(msg.getTarget());
    }

    @Test
    void shouldPassComplexNestedLists() {
        var msg = PackstreamBuf.allocUnpooled()
                .writeStructHeader(new StructHeader(2, (short) 0x42))
                // Struct #0
                .writeListHeader(3)
                .writeBoolean(false)
                .writeListHeader(4)
                .writeBoolean(true)
                .writeBoolean(false)
                .writeBoolean(true)
                .writeBoolean(false)
                .writeMapHeader(2)
                .writeString("the_answer")
                .writeInt(42)
                .writeString("not_the_answer")
                .writeFloat(21.25)
                // Struct #1
                .writeString("fizz");

        this.channel.writeInbound(msg.getTarget());
        this.channel.checkException();

        var received = this.channel.readInbound();

        Assertions.assertThat(received).isSameAs(msg.getTarget());
    }

    @Test
    void shouldPassComplexNestedStructs() {
        var msg = PackstreamBuf.allocUnpooled()
                .writeStructHeader(new StructHeader(2, (short) 0x42))
                // Struct #0
                .writeStructHeader(new StructHeader(3, (short) 0x21))
                .writeBoolean(false)
                .writeStructHeader(new StructHeader(4, (short) 0x55))
                .writeBoolean(true)
                .writeBoolean(false)
                .writeBoolean(true)
                .writeBoolean(false)
                .writeMapHeader(2)
                .writeString("the_answer")
                .writeInt(42)
                .writeString("not_the_answer")
                .writeFloat(21.25)
                // Struct #1
                .writeString("fizz");

        this.channel.writeInbound(msg.getTarget());
        this.channel.checkException();

        var received = this.channel.readInbound();

        Assertions.assertThat(received).isSameAs(msg.getTarget());
    }

    @Test
    void shouldPassReasonablyNestedLists() {
        var msg = PackstreamBuf.allocUnpooled()
                .writeStructHeader(new StructHeader(1, (short) 0x42))
                .writeListHeader(1)
                .writeListHeader(1)
                .writeListHeader(1)
                .writeBoolean(true);

        this.channel.writeInbound(msg.getTarget());
        this.channel.checkException();

        var received = this.channel.readInbound();

        Assertions.assertThat(received).isSameAs(msg.getTarget());
    }

    @Test
    void shouldRejectVeryLongLists() {
        var msg = PackstreamBuf.allocUnpooled()
                .writeStructHeader(new StructHeader(1, (short) 0x42))
                .writeListHeader(128);

        Assertions.assertThatExceptionOfType(ClientRequestComplexityExceeded.class)
                .isThrownBy(() -> {
                    this.channel.writeInbound(msg.getTarget());
                    this.channel.checkException();
                })
                .withMessage("Message has exceeded maximum permitted complexity of 64 elements");
    }

    @Test
    void shouldRejectVeryNestedLists() {
        var msg = PackstreamBuf.allocUnpooled()
                .writeStructHeader(new StructHeader(1, (short) 0x42))
                .writeListHeader(1)
                .writeListHeader(1)
                .writeListHeader(1)
                .writeListHeader(1)
                .writeBoolean(true);

        Assertions.assertThatExceptionOfType(ClientRequestComplexityExceeded.class)
                .isThrownBy(() -> {
                    this.channel.writeInbound(msg.getTarget());
                    this.channel.checkException();
                })
                .withMessage("Message has exceeded maximum permitted complexity of 4 levels");
    }

    @Test
    void shouldPermitReasonablyNestedMaps() {
        var msg = PackstreamBuf.allocUnpooled()
                .writeStructHeader(new StructHeader(1, (short) 0x42))
                .writeMapHeader(1)
                .writeString("foo")
                .writeMapHeader(1)
                .writeString("bar")
                .writeMapHeader(1)
                .writeString("some-key")
                .writeBoolean(true);

        this.channel.writeInbound(msg.getTarget());
        this.channel.checkException();

        var received = this.channel.readInbound();

        Assertions.assertThat(received).isSameAs(msg.getTarget());
    }

    @Test
    void shouldRejectVeryLongMaps() {
        var msg = PackstreamBuf.allocUnpooled()
                .writeStructHeader(new StructHeader(1, (short) 0x42))
                .writeMapHeader(128);

        Assertions.assertThatExceptionOfType(ClientRequestComplexityExceeded.class)
                .isThrownBy(() -> {
                    this.channel.writeInbound(msg.getTarget());
                    this.channel.checkException();
                })
                .withMessage("Message has exceeded maximum permitted complexity of 64 elements");
    }

    @Test
    void shouldRejectVeryNestedMaps() {
        var msg = PackstreamBuf.allocUnpooled()
                .writeStructHeader(new StructHeader(1, (short) 0x42))
                .writeMapHeader(1)
                .writeString("foo")
                .writeMapHeader(1)
                .writeString("bar")
                .writeMapHeader(1)
                .writeString("baz")
                .writeMapHeader(1)
                .writeString("some-key")
                .writeBoolean(true);

        Assertions.assertThatExceptionOfType(ClientRequestComplexityExceeded.class)
                .isThrownBy(() -> {
                    this.channel.writeInbound(msg.getTarget());
                    this.channel.checkException();
                })
                .withMessage("Message has exceeded maximum permitted complexity of 4 levels");
    }

    @Test
    void shouldPermitReasonablyNestedStructs() {
        var msg = PackstreamBuf.allocUnpooled()
                .writeStructHeader(new StructHeader(1, (short) 0x42))
                .writeStructHeader(new StructHeader(1, (short) 0x43))
                .writeStructHeader(new StructHeader(1, (short) 0x44))
                .writeStructHeader(new StructHeader(1, (short) 0x45))
                .writeBoolean(true);

        this.channel.writeInbound(msg.getTarget());
        this.channel.checkException();

        var received = this.channel.readInbound();

        Assertions.assertThat(received).isSameAs(msg.getTarget());
    }

    @Test
    void shouldRejectVeryLongStructs() {
        var msg = PackstreamBuf.allocUnpooled()
                .writeStructHeader(new StructHeader(1, (short) 0x42))
                .writeStructHeader(new StructHeader(128, (short) 0x21));

        Assertions.assertThatExceptionOfType(ClientRequestComplexityExceeded.class)
                .isThrownBy(() -> {
                    this.channel.writeInbound(msg.getTarget());
                    this.channel.checkException();
                })
                .withMessage("Message has exceeded maximum permitted complexity of 64 elements");
    }

    @Test
    void shouldRejectVeryLongRootStructs() {
        var msg = PackstreamBuf.allocUnpooled().writeStructHeader(new StructHeader(128, (short) 0x42));

        Assertions.assertThatExceptionOfType(ClientRequestComplexityExceeded.class)
                .isThrownBy(() -> {
                    this.channel.writeInbound(msg.getTarget());
                    this.channel.checkException();
                })
                .withMessage("Message has exceeded maximum permitted complexity of 64 elements");
    }

    @Test
    void shouldRejectVeryNestedStructs() {
        var msg = PackstreamBuf.allocUnpooled()
                .writeStructHeader(new StructHeader(1, (short) 0x42))
                .writeStructHeader(new StructHeader(1, (short) 0x43))
                .writeStructHeader(new StructHeader(1, (short) 0x44))
                .writeStructHeader(new StructHeader(1, (short) 0x45))
                .writeStructHeader(new StructHeader(1, (short) 0x45))
                .writeBoolean(true);

        Assertions.assertThatExceptionOfType(ClientRequestComplexityExceeded.class)
                .isThrownBy(() -> {
                    this.channel.writeInbound(msg.getTarget());
                    this.channel.checkException();
                })
                .withMessage("Message has exceeded maximum permitted complexity of 4 levels");
    }

    @Test
    void shouldRejectOverlyNestedInterleavedTypes() {
        var msg = PackstreamBuf.allocUnpooled()
                .writeStructHeader(new StructHeader(1, (short) 0x42))
                .writeMapHeader(1)
                .writeString("a")
                .writeListHeader(1)
                .writeMapHeader(1)
                .writeString("b")
                .writeListHeader(1)
                .writeBoolean(true);

        Assertions.assertThatExceptionOfType(ClientRequestComplexityExceeded.class)
                .isThrownBy(() -> {
                    this.channel.writeInbound(msg.getTarget());
                    this.channel.checkException();
                })
                .withMessage("Message has exceeded maximum permitted complexity of 4 levels");
    }
}
