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
package org.neo4j.bolt.protocol.common.message.decoder.transaction;

import static org.assertj.core.api.Assertions.assertThatExceptionOfType;

import org.junit.jupiter.api.Test;
import org.mockito.Mockito;
import org.neo4j.bolt.protocol.common.message.decoder.MessageDecoder;
import org.neo4j.bolt.protocol.common.message.decoder.MultiParameterMessageDecoderTest;
import org.neo4j.bolt.protocol.common.message.request.transaction.RunMessage;
import org.neo4j.bolt.testing.mock.ConnectionMockFactory;
import org.neo4j.packstream.error.reader.PackstreamReaderException;
import org.neo4j.packstream.error.reader.UnexpectedTypeException;
import org.neo4j.packstream.error.struct.IllegalStructArgumentException;
import org.neo4j.packstream.io.PackstreamBuf;
import org.neo4j.packstream.io.value.PackstreamValueReader;
import org.neo4j.packstream.struct.StructHeader;
import org.neo4j.values.storable.Values;
import org.neo4j.values.virtual.MapValue;
import org.neo4j.values.virtual.MapValueBuilder;

public abstract class AbstractRunMessageDecoderTest<D extends MessageDecoder<RunMessage>>
        implements MultiParameterMessageDecoderTest<D> {

    @Test
    void shouldFailWithIllegalStructArgumentWhenInvalidStatementArgumentIsPassed() {
        var buf = PackstreamBuf.allocUnpooled().writeInt(42);

        assertThatExceptionOfType(IllegalStructArgumentException.class)
                .isThrownBy(() -> this.getDecoder()
                        .read(ConnectionMockFactory.newInstance(), buf, new StructHeader(3, (short) 0x42)))
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

        assertThatExceptionOfType(IllegalStructArgumentException.class)
                .isThrownBy(() -> this.getDecoder().read(connection, buf, new StructHeader(3, (short) 0x42)))
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

        assertThatExceptionOfType(IllegalStructArgumentException.class)
                .isThrownBy(() -> this.getDecoder().read(connection, buf, new StructHeader(3, (short) 0x42)))
                .withMessage(
                        "Illegal value for field \"metadata\": Illegal value for field \"tx_timeout\": Expected long")
                .withCauseInstanceOf(IllegalStructArgumentException.class);
    }
}
