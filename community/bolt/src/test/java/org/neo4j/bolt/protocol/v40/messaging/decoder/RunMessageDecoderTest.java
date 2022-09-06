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

import java.time.Duration;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;
import org.neo4j.bolt.protocol.common.message.AccessMode;
import org.neo4j.bolt.testing.mock.ConnectionMockFactory;
import org.neo4j.packstream.error.reader.PackstreamReaderException;
import org.neo4j.packstream.error.reader.UnexpectedTypeException;
import org.neo4j.packstream.error.struct.IllegalStructArgumentException;
import org.neo4j.packstream.error.struct.IllegalStructSizeException;
import org.neo4j.packstream.io.PackstreamBuf;
import org.neo4j.packstream.io.value.PackstreamValueReader;
import org.neo4j.packstream.struct.StructHeader;
import org.neo4j.values.storable.Values;
import org.neo4j.values.virtual.MapValue;
import org.neo4j.values.virtual.MapValueBuilder;
import org.neo4j.values.virtual.VirtualValues;

class RunMessageDecoderTest {

    @Test
    void shouldReadMessage() throws PackstreamReaderException {
        var buf = PackstreamBuf.allocUnpooled();
        var reader = Mockito.mock(PackstreamValueReader.class);

        buf.writeString("RETURN $n");

        var params = new MapValueBuilder();
        params.add("n", Values.longValue(42));
        params.add("qid", Values.longValue(21));

        var txMeta = new MapValueBuilder();
        txMeta.add("foo", Values.stringValue("bar"));
        txMeta.add("the_answer", Values.longValue(42));

        var meta = new MapValueBuilder();
        meta.add(
                "bookmarks",
                VirtualValues.list(
                        Values.stringValue("neo4j:mock:bookmark1"), Values.stringValue("neo4j:mock:bookmark2")));
        meta.add("tx_timeout", Values.longValue(42));
        meta.add("mode", Values.stringValue("w"));
        meta.add("tx_metadata", txMeta.build());
        meta.add("db", Values.stringValue("neo4j"));

        Mockito.doReturn(params.build(), meta.build()).when(reader).readMap();

        var connection =
                ConnectionMockFactory.newFactory().withValueReader(reader).build();

        var msg = RunMessageDecoder.getInstance().read(connection, buf, new StructHeader(3, (short) 0x42));

        assertThat(msg).isNotNull();
        assertThat(msg.statement()).isEqualTo("RETURN $n");
        assertThat(msg.params().size()).isEqualTo(2);
        assertThat(msg.bookmarks()).isNotNull().isEmpty();
        assertThat(msg.transactionTimeout()).isEqualTo(Duration.ofMillis(42));
        assertThat(msg.getAccessMode()).isEqualTo(AccessMode.WRITE);
        assertThat(msg.transactionMetadata())
                .hasSize(2)
                .containsEntry("foo", "bar")
                .containsEntry("the_answer", 42L);
        assertThat(msg.databaseName()).isEqualTo("neo4j");
    }

    @Test
    void shouldFailWithIllegalStructSizeWhenEmptyStructIsGiven() {
        var decoder = RunMessageDecoder.getInstance();

        assertThatExceptionOfType(IllegalStructSizeException.class)
                .isThrownBy(() -> decoder.read(
                        ConnectionMockFactory.newInstance(), PackstreamBuf.allocUnpooled(), new StructHeader(0, (short)
                                0x42)))
                .withMessage("Illegal struct size: Expected struct to be 3 fields but got 0");
    }

    @Test
    void shouldFailWithIllegalStructSizeWhenSmallStructIsGiven() {
        var decoder = RunMessageDecoder.getInstance();

        assertThatExceptionOfType(IllegalStructSizeException.class)
                .isThrownBy(() -> decoder.read(
                        ConnectionMockFactory.newInstance(), PackstreamBuf.allocUnpooled(), new StructHeader(1, (short)
                                0x42)))
                .withMessage("Illegal struct size: Expected struct to be 3 fields but got 1");

        assertThatExceptionOfType(IllegalStructSizeException.class)
                .isThrownBy(() -> decoder.read(
                        ConnectionMockFactory.newInstance(), PackstreamBuf.allocUnpooled(), new StructHeader(2, (short)
                                0x42)))
                .withMessage("Illegal struct size: Expected struct to be 3 fields but got 2");
    }

    @Test
    void shouldFailWithIllegalStructSizeWhenLargeStructIsGiven() {
        var decoder = RunMessageDecoder.getInstance();

        assertThatExceptionOfType(IllegalStructSizeException.class)
                .isThrownBy(() -> decoder.read(
                        ConnectionMockFactory.newInstance(), PackstreamBuf.allocUnpooled(), new StructHeader(4, (short)
                                0x42)))
                .withMessage("Illegal struct size: Expected struct to be 3 fields but got 4");
    }

    @Test
    void shouldFailWithIllegalStructArgumentWhenInvalidStatementArgumentIsPassed() {
        var buf = PackstreamBuf.allocUnpooled().writeInt(42);

        var decoder = RunMessageDecoder.getInstance();

        assertThatExceptionOfType(IllegalStructArgumentException.class)
                .isThrownBy(
                        () -> decoder.read(ConnectionMockFactory.newInstance(), buf, new StructHeader(3, (short) 0x42)))
                .withMessage("Illegal value for field \"statement\": Unexpected type: Expected STRING but got INT")
                .withCauseInstanceOf(UnexpectedTypeException.class);
    }

    @Test
    void shouldFailWithIllegalStructArgumentWhenInvalidParamsArgumentIsPassed() throws PackstreamReaderException {
        var buf = PackstreamBuf.allocUnpooled().writeString("RETURN 1");
        var ex = new PackstreamReaderException("Something went kaput :(");

        var reader = Mockito.mock(PackstreamValueReader.class);
        Mockito.doThrow(ex).when(reader).readMap();

        var connection =
                ConnectionMockFactory.newFactory().withValueReader(reader).build();

        var decoder = RunMessageDecoder.getInstance();

        assertThatExceptionOfType(IllegalStructArgumentException.class)
                .isThrownBy(() -> decoder.read(connection, buf, new StructHeader(3, (short) 0x42)))
                .withMessage("Illegal value for field \"params\": Something went kaput :(")
                .withCause(ex);
    }

    @Test
    void shouldFailWithIllegalStructArgumentWhenInvalidMetadataEntryIsPassed() throws PackstreamReaderException {
        var buf = PackstreamBuf.allocUnpooled();
        var reader = Mockito.mock(PackstreamValueReader.class);

        buf.writeString("RETURN $n");

        var meta = new MapValueBuilder();
        meta.add("tx_timeout", Values.stringValue("✨✨ nonsense ✨✨"));

        Mockito.doReturn(MapValue.EMPTY, meta.build()).when(reader).readMap();

        var connection =
                ConnectionMockFactory.newFactory().withValueReader(reader).build();

        var decoder = RunMessageDecoder.getInstance();

        assertThatExceptionOfType(IllegalStructArgumentException.class)
                .isThrownBy(() -> decoder.read(connection, buf, new StructHeader(3, (short) 0x42)))
                .withMessage(
                        "Illegal value for field \"metadata\": Illegal value for field \"tx_timeout\": Expecting transaction timeout value to be a Long value, but got: String(\"✨✨ nonsense ✨✨\")")
                .withCauseInstanceOf(IllegalStructArgumentException.class);
    }
}
