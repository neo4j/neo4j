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
package org.neo4j.bolt.protocol.common.message.decoder.streaming;

import static org.assertj.core.api.Assertions.assertThatExceptionOfType;

import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;
import org.neo4j.bolt.protocol.common.message.decoder.NonEmptyMessageDecoderTest;
import org.neo4j.bolt.protocol.common.message.request.streaming.AbstractStreamingMessage;
import org.neo4j.bolt.testing.mock.ConnectionMockFactory;
import org.neo4j.packstream.error.reader.PackstreamReaderException;
import org.neo4j.packstream.error.struct.IllegalStructArgumentException;
import org.neo4j.packstream.io.PackstreamBuf;
import org.neo4j.packstream.io.value.PackstreamValueReader;
import org.neo4j.packstream.struct.StructHeader;
import org.neo4j.values.storable.Values;
import org.neo4j.values.virtual.MapValueBuilder;

public interface StreamingMessageDecoderTest<
                D extends AbstractStreamingMessageDecoder<M>, M extends AbstractStreamingMessage>
        extends NonEmptyMessageDecoderTest<D> {

    @Override
    default int maximumNumberOfFields() {
        return 1;
    }

    @Test
    default void shouldReadMessage() throws PackstreamReaderException {
        var reader = Mockito.mock(PackstreamValueReader.class);
        var buf = PackstreamBuf.allocUnpooled();

        var builder = new MapValueBuilder();
        builder.add("qid", Values.longValue(42));
        builder.add("n", Values.longValue(7));
        var meta = builder.build();

        Mockito.doReturn(meta).when(reader).readMap();

        var connection =
                ConnectionMockFactory.newFactory().withValueReader(reader).build();

        var msg = this.getDecoder().read(connection, buf, new StructHeader(1, (short) 0x42));

        Assertions.assertThat(msg).isNotNull();

        Assertions.assertThat(msg.statementId()).isEqualTo(42);
        Assertions.assertThat(msg.n()).isEqualTo(7);
    }

    @Test
    default void shouldPermitOmittedStatementId() throws PackstreamReaderException {
        var reader = Mockito.mock(PackstreamValueReader.class);
        var buf = PackstreamBuf.allocUnpooled();

        var builder = new MapValueBuilder();
        builder.add("n", Values.longValue(7));
        var meta = builder.build();

        Mockito.doReturn(meta).when(reader).readMap();

        var connection =
                ConnectionMockFactory.newFactory().withValueReader(reader).build();

        var msg = this.getDecoder().read(connection, buf, new StructHeader(1, (short) 0x42));

        Assertions.assertThat(msg).isNotNull();

        Assertions.assertThat(msg.statementId()).isEqualTo(-1);
        Assertions.assertThat(msg.n()).isEqualTo(7);
    }

    @Test
    default void shouldFailWithIllegalStructArgumentWhenNegativeStreamLimitIsGiven() throws PackstreamReaderException {
        var buf = PackstreamBuf.allocUnpooled();
        var reader = Mockito.mock(PackstreamValueReader.class);

        var meta = new MapValueBuilder();
        meta.add("n", Values.longValue(-2));
        meta.add("qid", Values.longValue(21));

        Mockito.doReturn(meta.build()).when(reader).readMap();

        var connection =
                ConnectionMockFactory.newFactory().withValueReader(reader).build();

        assertThatExceptionOfType(IllegalStructArgumentException.class)
                .isThrownBy(() -> this.getDecoder().read(connection, buf, new StructHeader(1, (short) 0x42)))
                .withMessage(
                        "Illegal value for field \"meta\": Illegal value for field \"n\": Expecting size to be at least 1, but got: -2")
                .withCauseInstanceOf(IllegalStructArgumentException.class);
    }

    @Test
    default void shouldFailWithIllegalStructArgumentWhenInvalidArgumentIsPassed() throws PackstreamReaderException {
        var buf = PackstreamBuf.allocUnpooled();
        var reader = Mockito.mock(PackstreamValueReader.class);
        var ex = new PackstreamReaderException("Something went kaput :(");

        Mockito.doThrow(ex).when(reader).readMap();

        var connection =
                ConnectionMockFactory.newFactory().withValueReader(reader).build();

        assertThatExceptionOfType(IllegalStructArgumentException.class)
                .isThrownBy(() -> this.getDecoder().read(connection, buf, new StructHeader(1, (short) 0x42)))
                .withMessage("Illegal value for field \"meta\": Something went kaput :(")
                .withCause(ex);
    }

    @Test
    default void shouldFailWithIllegalStructArgumentWhenInvalidMetadataEntryIsPassed()
            throws PackstreamReaderException {
        var buf = PackstreamBuf.allocUnpooled();
        var reader = Mockito.mock(PackstreamValueReader.class);

        var meta = new MapValueBuilder();
        meta.add("n", Values.stringValue("✨✨ nonsense ✨✨"));

        Mockito.doReturn(meta.build()).when(reader).readMap();

        var connection =
                ConnectionMockFactory.newFactory().withValueReader(reader).build();

        assertThatExceptionOfType(IllegalStructArgumentException.class)
                .isThrownBy(() -> this.getDecoder().read(connection, buf, new StructHeader(1, (short) 0x42)))
                .withMessage("Illegal value for field \"meta\": Illegal value for field \"n\": Expected long")
                .withCauseInstanceOf(IllegalStructArgumentException.class);
    }

    @Test
    default void shouldFailWithIllegalStructArgumentWhenNumberOfRecordsIsOmitted() throws PackstreamReaderException {
        var reader = Mockito.mock(PackstreamValueReader.class);
        var ex = new PackstreamReaderException("Something went kaput :(");

        Mockito.doThrow(ex).when(reader).readMap();

        var connection =
                ConnectionMockFactory.newFactory().withValueReader(reader).build();
        var buf = PackstreamBuf.allocUnpooled().writeMapHeader(0);

        assertThatExceptionOfType(IllegalStructArgumentException.class)
                .isThrownBy(() -> this.getDecoder().read(connection, buf, new StructHeader(1, (short) 0x42)))
                .withMessage("Illegal value for field \"meta\": Something went kaput :(")
                .withCause(ex);
    }
}
