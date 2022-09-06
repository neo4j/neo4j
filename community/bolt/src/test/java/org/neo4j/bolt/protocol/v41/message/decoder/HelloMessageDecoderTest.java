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

import java.util.List;
import org.assertj.core.api.InstanceOfAssertFactories;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;
import org.neo4j.bolt.protocol.common.connector.connection.Feature;
import org.neo4j.bolt.protocol.v41.message.request.HelloMessage;
import org.neo4j.bolt.protocol.v41.message.request.RoutingContext;
import org.neo4j.bolt.testing.mock.ConnectionMockFactory;
import org.neo4j.packstream.error.reader.PackstreamReaderException;
import org.neo4j.packstream.error.struct.IllegalStructArgumentException;
import org.neo4j.packstream.error.struct.IllegalStructSizeException;
import org.neo4j.packstream.io.PackstreamBuf;
import org.neo4j.packstream.io.value.PackstreamValueReader;
import org.neo4j.packstream.struct.StructHeader;
import org.neo4j.values.storable.Values;
import org.neo4j.values.virtual.MapValue;
import org.neo4j.values.virtual.MapValueBuilder;
import org.neo4j.values.virtual.VirtualValues;

class HelloMessageDecoderTest {

    @Test
    void shouldReadMessageWithoutRoutingContext() throws PackstreamReaderException {
        var buf = PackstreamBuf.allocUnpooled();
        var reader = Mockito.mock(PackstreamValueReader.class);

        var meta = new MapValueBuilder();
        meta.add("user_agent", Values.stringValue("Example/1.0 (+https://github.com/neo4j)"));
        meta.add("scheme", Values.stringValue("none"));

        Mockito.doReturn(meta.build()).when(reader).readPrimitiveMap(Mockito.anyLong());

        var connection =
                ConnectionMockFactory.newFactory().withValueReader(reader).build();

        var msg = HelloMessageDecoder.getInstance().read(connection, buf, new StructHeader(1, (short) 0x42));

        assertThat(msg).isNotNull();
        assertThat(msg.userAgent()).isEqualTo("Example/1.0 (+https://github.com/neo4j)");
        assertThat(msg.authToken()).hasSize(2).containsEntry("scheme", "none");

        assertThat(msg)
                .asInstanceOf(InstanceOfAssertFactories.type(HelloMessage.class))
                .extracting(HelloMessage::routingContext)
                .isNotNull()
                .extracting(RoutingContext::isServerRoutingEnabled)
                .isEqualTo(false);
    }

    @Test
    void shouldReadMessageWithPatchOptions() throws PackstreamReaderException {
        var buf = PackstreamBuf.allocUnpooled();
        var reader = Mockito.mock(PackstreamValueReader.class);

        var meta = new MapValueBuilder();
        meta.add("user_agent", Values.stringValue("Example/1.0 (+https://github.com/neo4j)"));
        meta.add("scheme", Values.stringValue("none"));
        meta.add("patch_bolt", VirtualValues.list(Values.stringValue("utc")));

        Mockito.doReturn(meta.build()).when(reader).readPrimitiveMap(Mockito.anyLong());

        var connection =
                ConnectionMockFactory.newFactory().withValueReader(reader).build();

        var msg = HelloMessageDecoder.getInstance().read(connection, buf, new StructHeader(1, (short) 0x42));

        assertThat(msg).isNotNull();
        assertThat(msg.userAgent()).isEqualTo("Example/1.0 (+https://github.com/neo4j)");
        assertThat(msg.authToken()).hasSize(3).containsEntry("scheme", "none");
        assertThat(msg.features()).isEqualTo(List.of(Feature.UTC_DATETIME));

        assertThat(msg)
                .asInstanceOf(InstanceOfAssertFactories.type(HelloMessage.class))
                .extracting(HelloMessage::routingContext)
                .isNotNull()
                .extracting(RoutingContext::isServerRoutingEnabled)
                .isEqualTo(false);
    }

    @Test
    void shouldIgnoreUnknownPatchOptions() throws PackstreamReaderException {
        var buf = PackstreamBuf.allocUnpooled();
        var reader = Mockito.mock(PackstreamValueReader.class);

        var meta = new MapValueBuilder();
        meta.add("user_agent", Values.stringValue("Example/1.0 (+https://github.com/neo4j)"));
        meta.add("scheme", Values.stringValue("none"));
        meta.add("patch_bolt", VirtualValues.list(Values.stringValue("monkey"), Values.stringValue("banana")));

        Mockito.doReturn(meta.build()).when(reader).readPrimitiveMap(Mockito.anyLong());

        var connection =
                ConnectionMockFactory.newFactory().withValueReader(reader).build();

        var msg = HelloMessageDecoder.getInstance().read(connection, buf, new StructHeader(1, (short) 0x42));

        assertThat(msg).isNotNull();
        assertThat(msg.userAgent()).isEqualTo("Example/1.0 (+https://github.com/neo4j)");
        assertThat(msg.authToken()).hasSize(3).containsEntry("scheme", "none");
        assertThat(msg.features()).isEmpty();

        assertThat(msg)
                .asInstanceOf(InstanceOfAssertFactories.type(HelloMessage.class))
                .extracting(HelloMessage::routingContext)
                .isNotNull()
                .extracting(RoutingContext::isServerRoutingEnabled)
                .isEqualTo(false);
    }

    @Test
    void shouldReadMessageWithRoutingContext() throws PackstreamReaderException {
        var buf = PackstreamBuf.allocUnpooled();
        var reader = Mockito.mock(PackstreamValueReader.class);

        var routing = new MapValueBuilder();
        routing.add("region", Values.stringValue("eu-west"));

        var meta = new MapValueBuilder();
        meta.add("user_agent", Values.stringValue("Example/1.0 (+https://github.com/neo4j)"));
        meta.add("scheme", Values.stringValue("none"));
        meta.add("routing", routing.build());

        Mockito.doReturn(meta.build()).when(reader).readPrimitiveMap(Mockito.anyLong());

        var connection =
                ConnectionMockFactory.newFactory().withValueReader(reader).build();

        var msg = HelloMessageDecoder.getInstance().read(connection, buf, new StructHeader(1, (short) 0x42));

        assertThat(msg).isNotNull();
        assertThat(msg.userAgent()).isEqualTo("Example/1.0 (+https://github.com/neo4j)");
        assertThat(msg.authToken())
                .hasSize(2)
                .containsEntry("scheme", "none")
                .doesNotContainKey("routing")
                .doesNotContainKey("region");

        assertThat(msg)
                .asInstanceOf(InstanceOfAssertFactories.type(HelloMessage.class))
                .extracting(HelloMessage::routingContext)
                .isNotNull()
                .satisfies(ctx -> {
                    assertThat(ctx.isServerRoutingEnabled()).isTrue();

                    assertThat(ctx.parameters()).hasSize(1).containsEntry("region", "eu-west");
                });
    }

    @Test
    void shouldReadMessageWithEmptyRoutingContext() throws PackstreamReaderException {
        var buf = PackstreamBuf.allocUnpooled();
        var reader = Mockito.mock(PackstreamValueReader.class);

        var meta = new MapValueBuilder();
        meta.add("user_agent", Values.stringValue("Example/1.0 (+https://github.com/neo4j)"));
        meta.add("scheme", Values.stringValue("none"));
        meta.add("routing", MapValue.EMPTY);

        Mockito.doReturn(meta.build()).when(reader).readPrimitiveMap(Mockito.anyLong());

        var connection =
                ConnectionMockFactory.newFactory().withValueReader(reader).build();

        var msg = HelloMessageDecoder.getInstance().read(connection, buf, new StructHeader(1, (short) 0x42));

        assertThat(msg).isNotNull();
        assertThat(msg.userAgent()).isEqualTo("Example/1.0 (+https://github.com/neo4j)");
        assertThat(msg.authToken()).hasSize(2).containsEntry("scheme", "none");

        assertThat(msg)
                .asInstanceOf(InstanceOfAssertFactories.type(HelloMessage.class))
                .extracting(HelloMessage::routingContext)
                .isNotNull()
                .satisfies(ctx -> {
                    assertThat(ctx.isServerRoutingEnabled()).isTrue();
                    assertThat(ctx.parameters()).isEmpty();
                });
    }

    @Test
    void shouldFailWithIllegalStructArgumentWhenInvalidRoutingContextValueIsPassed() throws PackstreamReaderException {
        var buf = PackstreamBuf.allocUnpooled();
        var reader = Mockito.mock(PackstreamValueReader.class);

        var meta = new MapValueBuilder();
        meta.add("user_agent", Values.stringValue("Example/1.0 (+https://github.com/neo4j)"));
        meta.add("scheme", Values.stringValue("none"));
        meta.add("routing", Values.longValue(42));

        Mockito.doReturn(meta.build()).when(reader).readPrimitiveMap(Mockito.anyLong());

        var connection =
                ConnectionMockFactory.newFactory().withValueReader(reader).build();

        var decoder = HelloMessageDecoder.getInstance();

        // See work note in HelloMessageDecoder
        assertThatExceptionOfType(IllegalStructArgumentException.class)
                .isThrownBy(() -> decoder.read(connection, buf, new StructHeader(1, (short) 0x42)));
    }

    @Test
    void shouldFailWithIllegalStructArgumentWhenInvalidRoutingContextContentIsPassed()
            throws PackstreamReaderException {
        var buf = PackstreamBuf.allocUnpooled();
        var reader = Mockito.mock(PackstreamValueReader.class);

        var routing = new MapValueBuilder();
        routing.add("foo", Values.longValue(42));

        var meta = new MapValueBuilder();
        meta.add("user_agent", Values.stringValue("Example/1.0 (+https://github.com/neo4j)"));
        meta.add("scheme", Values.stringValue("none"));
        meta.add("routing", routing.build());

        Mockito.doReturn(meta.build()).when(reader).readPrimitiveMap(Mockito.anyLong());

        var connection =
                ConnectionMockFactory.newFactory().withValueReader(reader).build();

        var decoder = HelloMessageDecoder.getInstance();

        assertThatExceptionOfType(IllegalStructArgumentException.class)
                .isThrownBy(() -> decoder.read(connection, buf, new StructHeader(1, (short) 0x42)))
                .withMessage("Illegal value for field \"routing\": Must be a map with string keys and string values.");
    }

    @Test
    void shouldFailWithIllegalStructSizeWhenEmptyStructIsGiven() {
        var decoder = HelloMessageDecoder.getInstance();

        assertThatExceptionOfType(IllegalStructSizeException.class)
                .isThrownBy(() -> decoder.read(
                        ConnectionMockFactory.newInstance(), PackstreamBuf.allocUnpooled(), new StructHeader(0, (short)
                                0x42)))
                .withMessage("Illegal struct size: Expected struct to be 1 fields but got 0");
    }

    @Test
    void shouldFailWithIllegalStructSizeWhenLargeStructIsGiven() {
        var decoder = HelloMessageDecoder.getInstance();

        assertThatExceptionOfType(IllegalStructSizeException.class)
                .isThrownBy(() -> decoder.read(
                        ConnectionMockFactory.newInstance(), PackstreamBuf.allocUnpooled(), new StructHeader(2, (short)
                                0x42)))
                .withMessage("Illegal struct size: Expected struct to be 1 fields but got 2");
    }
}
