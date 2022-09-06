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
package org.neo4j.bolt.protocol.v40.messaging.decoder;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatExceptionOfType;

import java.nio.charset.StandardCharsets;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;
import org.neo4j.bolt.testing.mock.ConnectionMockFactory;
import org.neo4j.packstream.error.reader.PackstreamReaderException;
import org.neo4j.packstream.error.struct.IllegalStructArgumentException;
import org.neo4j.packstream.error.struct.IllegalStructSizeException;
import org.neo4j.packstream.io.PackstreamBuf;
import org.neo4j.packstream.io.value.PackstreamValueReader;
import org.neo4j.packstream.struct.StructHeader;
import org.neo4j.values.storable.Values;
import org.neo4j.values.virtual.MapValueBuilder;

class HelloMessageDecoderTest {

    @Test
    void shouldReadMessage() throws PackstreamReaderException {
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

        // ensure that readPrimitiveMap is the only interaction point on PackstreamValueReader as HELLO explicitly
        // forbids the use of complex structures (such as dates, points, etc) to reduce potential attack vectors that
        // could lead to denial of service attacks
        Mockito.verify(reader).readPrimitiveMap(Mockito.anyLong());
        Mockito.verifyNoMoreInteractions(reader);
    }

    @Test
    void shouldConvertSensitiveValues() throws PackstreamReaderException {
        var buf = PackstreamBuf.allocUnpooled();
        var reader = Mockito.mock(PackstreamValueReader.class);

        var meta = new MapValueBuilder();
        meta.add("user_agent", Values.stringValue("Example/1.0 (+https://github.com/neo4j)"));
        meta.add("scheme", Values.stringValue("something"));
        meta.add("credentials", Values.stringValue("5upers3cre7"));

        Mockito.doReturn(meta.build()).when(reader).readPrimitiveMap(Mockito.anyLong());

        var connection =
                ConnectionMockFactory.newFactory().withValueReader(reader).build();

        var msg = HelloMessageDecoder.getInstance().read(connection, buf, new StructHeader(1, (short) 0x42));

        assertThat(msg).isNotNull();
        assertThat(msg.authToken()).containsEntry("credentials", "5upers3cre7".getBytes(StandardCharsets.UTF_8));
    }

    @Test
    void shouldFailWithIllegalStructSizeWhenEmptyStructIsGiven() {
        var connection = ConnectionMockFactory.newInstance();
        var decoder = HelloMessageDecoder.getInstance();

        assertThatExceptionOfType(IllegalStructSizeException.class)
                .isThrownBy(() ->
                        decoder.read(connection, PackstreamBuf.allocUnpooled(), new StructHeader(0, (short) 0x42)))
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

    @Test
    void shouldFailWithIllegalStructArgumentWhenInvalidArgumentIsPassed() throws PackstreamReaderException {
        var buf = PackstreamBuf.allocUnpooled().writeInt(42);
        var reader = Mockito.mock(PackstreamValueReader.class);
        var ex = new PackstreamReaderException("Something went kaput :(");

        Mockito.doThrow(ex).when(reader).readPrimitiveMap(Mockito.anyLong());

        var connection =
                ConnectionMockFactory.newFactory().withValueReader(reader).build();

        var decoder = HelloMessageDecoder.getInstance();

        assertThatExceptionOfType(IllegalStructArgumentException.class)
                .isThrownBy(() -> decoder.read(connection, buf, new StructHeader(1, (short) 0x42)))
                .withMessage("Illegal value for field \"extra\": Something went kaput :(")
                .withCause(ex);
    }

    @Test
    void shouldFailWithIllegalStructArgumentWhenInvalidMetadataEntryIsPassed() throws PackstreamReaderException {
        var buf = PackstreamBuf.allocUnpooled();
        var reader = Mockito.mock(PackstreamValueReader.class);

        var meta = new MapValueBuilder();
        meta.add("user_agent", Values.longValue(42));
        meta.add("scheme", Values.stringValue("none"));

        Mockito.doReturn(meta.build()).when(reader).readPrimitiveMap(Mockito.anyLong());

        var connection =
                ConnectionMockFactory.newFactory().withValueReader(reader).build();

        var decoder = HelloMessageDecoder.getInstance();

        assertThatExceptionOfType(IllegalStructArgumentException.class)
                .isThrownBy(() -> decoder.read(connection, buf, new StructHeader(1, (short) 0x42)))
                .withMessage("Illegal value for field \"user_agent\": Expected value to be a string");
    }

    @Test
    void shouldFailWithIllegalStructArgumentWhenUserAgentIsOmitted() throws PackstreamReaderException {
        var buf = PackstreamBuf.allocUnpooled();
        var reader = Mockito.mock(PackstreamValueReader.class);

        var meta = new MapValueBuilder();
        meta.add("scheme", Values.stringValue("none"));

        Mockito.doReturn(meta.build()).when(reader).readPrimitiveMap(Mockito.anyLong());

        var connection =
                ConnectionMockFactory.newFactory().withValueReader(reader).build();

        var decoder = HelloMessageDecoder.getInstance();

        assertThatExceptionOfType(IllegalStructArgumentException.class)
                .isThrownBy(() -> decoder.read(connection, buf, new StructHeader(1, (short) 0x42)))
                .withMessage("Illegal value for field \"user_agent\": Expected \"user_agent\" to be non-null");
    }
}
