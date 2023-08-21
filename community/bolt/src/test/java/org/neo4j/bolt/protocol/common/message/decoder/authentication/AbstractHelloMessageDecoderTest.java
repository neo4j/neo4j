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
package org.neo4j.bolt.protocol.common.message.decoder.authentication;

import static org.assertj.core.api.Assertions.assertThatExceptionOfType;

import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.List;
import org.assertj.core.api.Assertions;
import org.assertj.core.api.InstanceOfAssertFactories;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;
import org.neo4j.bolt.protocol.common.connector.connection.Feature;
import org.neo4j.bolt.protocol.common.message.decoder.MessageDecoder;
import org.neo4j.bolt.protocol.common.message.decoder.NonEmptyMessageDecoderTest;
import org.neo4j.bolt.protocol.common.message.request.authentication.HelloMessage;
import org.neo4j.bolt.protocol.common.message.request.connection.RoutingContext;
import org.neo4j.bolt.testing.mock.ConnectionMockFactory;
import org.neo4j.packstream.error.reader.PackstreamReaderException;
import org.neo4j.packstream.error.struct.IllegalStructArgumentException;
import org.neo4j.packstream.io.PackstreamBuf;
import org.neo4j.packstream.io.value.PackstreamValueReader;
import org.neo4j.packstream.struct.StructHeader;
import org.neo4j.values.storable.Values;
import org.neo4j.values.virtual.MapValueBuilder;
import org.neo4j.values.virtual.VirtualValues;

public abstract class AbstractHelloMessageDecoderTest<D extends MessageDecoder<HelloMessage>>
        implements NonEmptyMessageDecoderTest<D> {

    @Override
    public int maximumNumberOfFields() {
        return 1;
    }

    protected void appendRequiredFields(MapValueBuilder meta) {
        meta.add("user_agent", Values.stringValue("Example/1.0 (+https://github.com/neo4j)"));
        meta.add("scheme", Values.stringValue("none"));
    }

    @Test
    protected void shouldConvertSensitiveValues() throws PackstreamReaderException {
        var buf = PackstreamBuf.allocUnpooled();
        var reader = Mockito.mock(PackstreamValueReader.class);

        var meta = new MapValueBuilder();
        meta.add("user_agent", Values.stringValue("Example/1.0 (+https://github.com/neo4j)"));
        meta.add("scheme", Values.stringValue("something"));
        meta.add("principal", Values.stringValue("bob"));
        meta.add("credentials", Values.stringValue("5upers3cre7"));

        Mockito.doReturn(meta.build()).when(reader).readPrimitiveMap(Mockito.anyLong());

        var connection =
                ConnectionMockFactory.newFactory().withValueReader(reader).build();

        var msg = this.getDecoder().read(connection, buf, new StructHeader(1, (short) 0x42));
        Assertions.assertThat(msg.boltAgent()).isEqualTo(Collections.emptyMap());
        Assertions.assertThat(msg).isNotNull();
        Assertions.assertThat(msg.authToken())
                .containsEntry("principal", "bob")
                .containsEntry("credentials", "5upers3cre7".getBytes(StandardCharsets.UTF_8));
    }

    @Test
    protected void shouldReadMessageWithoutRoutingContext() throws PackstreamReaderException {
        var buf = PackstreamBuf.allocUnpooled();
        var reader = Mockito.mock(PackstreamValueReader.class);

        var meta = new MapValueBuilder();
        this.appendRequiredFields(meta);

        Mockito.doReturn(meta.build()).when(reader).readPrimitiveMap(Mockito.anyLong());

        var connection =
                ConnectionMockFactory.newFactory().withValueReader(reader).build();

        var msg = this.getDecoder().read(connection, buf, new StructHeader(1, (short) 0x42));

        Assertions.assertThat(msg).isNotNull();
        Assertions.assertThat(msg.userAgent()).isEqualTo("Example/1.0 (+https://github.com/neo4j)");

        Assertions.assertThat(msg)
                .asInstanceOf(InstanceOfAssertFactories.type(HelloMessage.class))
                .extracting(HelloMessage::routingContext)
                .isNotNull()
                .extracting(RoutingContext::isServerRoutingEnabled)
                .isEqualTo(false);
    }

    @Test
    protected void shouldReadMessageWithPatchOptions() throws PackstreamReaderException {
        var buf = PackstreamBuf.allocUnpooled();
        var reader = Mockito.mock(PackstreamValueReader.class);

        var meta = new MapValueBuilder();
        this.appendRequiredFields(meta);
        meta.add("patch_bolt", VirtualValues.list(Values.stringValue("utc")));

        Mockito.doReturn(meta.build()).when(reader).readPrimitiveMap(Mockito.anyLong());

        var connection =
                ConnectionMockFactory.newFactory().withValueReader(reader).build();

        var msg = this.getDecoder().read(connection, buf, new StructHeader(1, (short) 0x42));

        Assertions.assertThat(msg).isNotNull();
        Assertions.assertThat(msg.userAgent()).isEqualTo("Example/1.0 (+https://github.com/neo4j)");
        Assertions.assertThat(msg.features()).isEqualTo(List.of(Feature.UTC_DATETIME));

        Assertions.assertThat(msg)
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
        this.appendRequiredFields(meta);
        meta.add("patch_bolt", VirtualValues.list(Values.stringValue("monkey"), Values.stringValue("banana")));

        Mockito.doReturn(meta.build()).when(reader).readPrimitiveMap(Mockito.anyLong());

        var connection =
                ConnectionMockFactory.newFactory().withValueReader(reader).build();

        var msg = this.getDecoder().read(connection, buf, new StructHeader(1, (short) 0x42));

        Assertions.assertThat(msg).isNotNull();
        Assertions.assertThat(msg.userAgent()).isEqualTo("Example/1.0 (+https://github.com/neo4j)");
        Assertions.assertThat(msg.features()).isEmpty();

        Assertions.assertThat(msg)
                .asInstanceOf(InstanceOfAssertFactories.type(HelloMessage.class))
                .extracting(HelloMessage::routingContext)
                .isNotNull()
                .extracting(RoutingContext::isServerRoutingEnabled)
                .isEqualTo(false);
    }

    @Test
    protected void shouldFailWithIllegalStructArgumentWhenInvalidArgumentIsPassed() throws PackstreamReaderException {
        var buf = PackstreamBuf.allocUnpooled().writeInt(42);
        var reader = Mockito.mock(PackstreamValueReader.class);
        var ex = new PackstreamReaderException("Something went kaput :(");

        Mockito.doThrow(ex).when(reader).readPrimitiveMap(Mockito.anyLong());

        var connection =
                ConnectionMockFactory.newFactory().withValueReader(reader).build();

        assertThatExceptionOfType(IllegalStructArgumentException.class)
                .isThrownBy(() -> this.getDecoder().read(connection, buf, new StructHeader(1, (short) 0x42)))
                .withMessage("Illegal value for field \"extra\": Something went kaput :(")
                .withCause(ex);
    }

    @Test
    protected void shouldFailWithIllegalStructArgumentWhenInvalidMetadataEntryIsPassed()
            throws PackstreamReaderException {
        var buf = PackstreamBuf.allocUnpooled();
        var reader = Mockito.mock(PackstreamValueReader.class);

        var meta = new MapValueBuilder();
        meta.add("user_agent", Values.longValue(42));
        meta.add("scheme", Values.stringValue("none"));

        Mockito.doReturn(meta.build()).when(reader).readPrimitiveMap(Mockito.anyLong());

        var connection =
                ConnectionMockFactory.newFactory().withValueReader(reader).build();

        assertThatExceptionOfType(IllegalStructArgumentException.class)
                .isThrownBy(() -> this.getDecoder().read(connection, buf, new StructHeader(1, (short) 0x42)))
                .withMessage("Illegal value for field \"user_agent\": Expected string");
    }

    @Test
    protected void shouldFailWithIllegalStructArgumentWhenUserAgentIsOmitted() throws PackstreamReaderException {
        var buf = PackstreamBuf.allocUnpooled();
        var reader = Mockito.mock(PackstreamValueReader.class);

        var meta = new MapValueBuilder();
        meta.add("scheme", Values.stringValue("none"));

        Mockito.doReturn(meta.build()).when(reader).readPrimitiveMap(Mockito.anyLong());

        var connection =
                ConnectionMockFactory.newFactory().withValueReader(reader).build();

        assertThatExceptionOfType(IllegalStructArgumentException.class)
                .isThrownBy(() -> this.getDecoder().read(connection, buf, new StructHeader(1, (short) 0x42)))
                .withMessage("Illegal value for field \"user_agent\": Expected value to be non-null");
    }
}
