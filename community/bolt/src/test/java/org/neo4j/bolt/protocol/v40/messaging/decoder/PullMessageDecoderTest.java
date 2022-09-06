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
import org.neo4j.values.virtual.MapValue;
import org.neo4j.values.virtual.MapValueBuilder;

class PullMessageDecoderTest {

    @Test
    @SuppressWarnings("unchecked")
    void shouldReadMessage() throws PackstreamReaderException {
        var buf = PackstreamBuf.allocUnpooled();
        var reader = Mockito.mock(PackstreamValueReader.class);

        var meta = new MapValueBuilder();
        meta.add("n", Values.longValue(42));
        meta.add("qid", Values.longValue(21));

        Mockito.doReturn(meta.build()).when(reader).readMap();

        var connection =
                ConnectionMockFactory.newFactory().withValueReader(reader).build();

        var msg = PullMessageDecoder.getInstance().read(connection, buf, new StructHeader(1, (short) 0x42));

        assertThat(msg).isNotNull();
        assertThat(msg.n()).isEqualTo(42);
        assertThat(msg.statementId()).isEqualTo(21);
    }

    @Test
    void shouldReadMessageWithAllRecordMarker() throws PackstreamReaderException {
        var buf = PackstreamBuf.allocUnpooled();
        var reader = Mockito.mock(PackstreamValueReader.class);

        var meta = new MapValueBuilder();
        meta.add("n", Values.longValue(-1));
        meta.add("qid", Values.longValue(21));

        Mockito.doReturn(meta.build()).when(reader).readMap();

        var connection =
                ConnectionMockFactory.newFactory().withValueReader(reader).build();

        var msg = PullMessageDecoder.getInstance().read(connection, buf, new StructHeader(1, (short) 0x42));

        assertThat(msg).isNotNull();
        assertThat(msg.n()).isEqualTo(-1);
        assertThat(msg.statementId()).isEqualTo(21);
    }

    @Test
    void shouldFailWithIllegalStructArgumentWhenZeroRecordsAreDiscarded() throws PackstreamReaderException {
        var buf = PackstreamBuf.allocUnpooled();
        var reader = Mockito.mock(PackstreamValueReader.class);

        var meta = new MapValueBuilder();
        meta.add("n", Values.longValue(0));
        meta.add("qid", Values.longValue(21));

        Mockito.doReturn(meta.build()).when(reader).readMap();

        var connection =
                ConnectionMockFactory.newFactory().withValueReader(reader).build();

        assertThatExceptionOfType(IllegalStructArgumentException.class)
                .isThrownBy(
                        () -> PullMessageDecoder.getInstance().read(connection, buf, new StructHeader(1, (short) 0x42)))
                .withMessage(
                        "Illegal value for field \"meta\": Illegal value for field \"n\": Expecting size to be at least 1, but got: 0")
                .withCauseInstanceOf(IllegalStructArgumentException.class);
    }

    @Test
    void shouldFailWithIllegalStructArgumentWhenNegativeRecordsAreDiscarded() throws PackstreamReaderException {
        var buf = PackstreamBuf.allocUnpooled();
        var reader = Mockito.mock(PackstreamValueReader.class);

        var meta = new MapValueBuilder();
        meta.add("n", Values.longValue(-2));
        meta.add("qid", Values.longValue(21));

        Mockito.doReturn(meta.build()).when(reader).readMap();

        var connection =
                ConnectionMockFactory.newFactory().withValueReader(reader).build();

        assertThatExceptionOfType(IllegalStructArgumentException.class)
                .isThrownBy(
                        () -> PullMessageDecoder.getInstance().read(connection, buf, new StructHeader(1, (short) 0x42)))
                .withMessage(
                        "Illegal value for field \"meta\": Illegal value for field \"n\": Expecting size to be at least 1, but got: -2")
                .withCauseInstanceOf(IllegalStructArgumentException.class);
    }

    @Test
    void shouldFailWithIllegalStructSizeWhenEmptyStructIsGiven() {
        var connection = ConnectionMockFactory.newInstance();

        assertThatExceptionOfType(IllegalStructSizeException.class)
                .isThrownBy(() -> PullMessageDecoder.getInstance()
                        .read(connection, PackstreamBuf.allocUnpooled(), new StructHeader(0, (short) 0x42)))
                .withMessage("Illegal struct size: Expected struct to be 1 fields but got 0");
    }

    @Test
    void shouldFailWithIllegalStructSizeWhenLargeStructIsGiven() {
        var connection = ConnectionMockFactory.newInstance();

        assertThatExceptionOfType(IllegalStructSizeException.class)
                .isThrownBy(() -> PullMessageDecoder.getInstance()
                        .read(connection, PackstreamBuf.allocUnpooled(), new StructHeader(2, (short) 0x42)))
                .withMessage("Illegal struct size: Expected struct to be 1 fields but got 2");
    }

    @Test
    void shouldFailWithIllegalStructArgumentWhenInvalidArgumentIsPassed() throws PackstreamReaderException {
        var ex = new PackstreamReaderException("Something went kaput :(");
        var buf = PackstreamBuf.allocUnpooled();
        var reader = Mockito.mock(PackstreamValueReader.class);

        Mockito.doThrow(ex).when(reader).readMap();

        var connection =
                ConnectionMockFactory.newFactory().withValueReader(reader).build();

        assertThatExceptionOfType(IllegalStructArgumentException.class)
                .isThrownBy(
                        () -> PullMessageDecoder.getInstance().read(connection, buf, new StructHeader(1, (short) 0x42)))
                .withMessage("Illegal value for field \"meta\": Something went kaput :(")
                .withCause(ex);
    }

    @Test
    void shouldFailWithIllegalStructArgumentWhenInvalidMetadataEntryIsPassed() throws PackstreamReaderException {
        var buf = PackstreamBuf.allocUnpooled();
        var reader = Mockito.mock(PackstreamValueReader.class);

        var meta = new MapValueBuilder();
        meta.add("n", Values.stringValue("✨✨ nonsense ✨✨"));

        Mockito.doReturn(meta.build()).when(reader).readMap();

        var connection =
                ConnectionMockFactory.newFactory().withValueReader(reader).build();

        assertThatExceptionOfType(IllegalStructArgumentException.class)
                .isThrownBy(
                        () -> PullMessageDecoder.getInstance().read(connection, buf, new StructHeader(1, (short) 0x42)))
                .withMessage(
                        "Illegal value for field \"meta\": Illegal value for field \"n\": Expecting size to be a Long value, but got: String(\"✨✨ nonsense ✨✨\")")
                .withCauseInstanceOf(IllegalStructArgumentException.class)
                .havingCause()
                .withMessage(
                        "Illegal value for field \"n\": Expecting size to be a Long value, but got: String(\"✨✨ nonsense ✨✨\")");
    }

    @Test
    void shouldFailWithIllegalStructArgumentWhenNumberOfRecordsIsOmitted() throws PackstreamReaderException {
        var buf = PackstreamBuf.allocUnpooled();
        var reader = Mockito.mock(PackstreamValueReader.class);

        Mockito.doReturn(MapValue.EMPTY).when(reader).readMap();

        var connection =
                ConnectionMockFactory.newFactory().withValueReader(reader).build();

        assertThatExceptionOfType(IllegalStructArgumentException.class)
                .isThrownBy(
                        () -> PullMessageDecoder.getInstance().read(connection, buf, new StructHeader(1, (short) 0x42)))
                .withMessage(
                        "Illegal value for field \"meta\": Illegal value for field \"n\": Expecting size to be a Long value, but got: NO_VALUE")
                .withCauseInstanceOf(IllegalStructArgumentException.class);
    }
}
