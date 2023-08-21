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

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Stream;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.mockito.Mockito;
import org.neo4j.bolt.protocol.common.message.decoder.MessageDecoder;
import org.neo4j.bolt.protocol.common.message.decoder.NonEmptyMessageDecoderTest;
import org.neo4j.bolt.protocol.common.message.request.transaction.BeginMessage;
import org.neo4j.bolt.testing.mock.ConnectionMockFactory;
import org.neo4j.packstream.error.reader.PackstreamReaderException;
import org.neo4j.packstream.error.struct.IllegalStructArgumentException;
import org.neo4j.packstream.io.PackstreamBuf;
import org.neo4j.packstream.io.value.PackstreamValueReader;
import org.neo4j.packstream.struct.StructHeader;
import org.neo4j.values.AnyValue;
import org.neo4j.values.storable.Values;
import org.neo4j.values.virtual.MapValueBuilder;
import org.neo4j.values.virtual.VirtualValues;

public abstract class AbstractBeginMessageDecoderTest<D extends MessageDecoder<BeginMessage>>
        implements NonEmptyMessageDecoderTest<D> {

    @MethodSource("invalidBookmarks")
    @ParameterizedTest
    protected void shouldFailWithBookmarkParserExceptionWhenParsingFails(AnyValue bookmarkValue)
            throws PackstreamReaderException {
        var buf = PackstreamBuf.allocUnpooled();
        var reader = Mockito.mock(PackstreamValueReader.class);

        var meta = new MapValueBuilder();
        meta.add("bookmarks", bookmarkValue);

        Mockito.doReturn(meta.build()).when(reader).readMap();
        var connection = ConnectionMockFactory.newFactory()
                .withConnector(factory -> {})
                .withValueReader(reader)
                .build();

        assertThatExceptionOfType(IllegalStructArgumentException.class)
                .isThrownBy(() -> this.getDecoder().read(connection, buf, new StructHeader(1, (short) 0x42)))
                .withMessageContaining("Illegal value for field \"bookmarks\":");
    }

    public static Stream<Arguments> invalidBookmarks() {
        List<Arguments> args = new ArrayList<>();
        args.add(Arguments.arguments(Values.stringValue("neo4j:mock:bookmark1")));
        args.add(Arguments.arguments(
                VirtualValues.list(Values.stringValue("neo4j:mock:bookmark1"), Values.intValue(123))));
        return args.stream();
    }

    @Test
    protected void shouldFailWithIllegalStructArgumentWhenInvalidArgumentIsPassed() throws PackstreamReaderException {
        var buf = PackstreamBuf.allocUnpooled();
        var reader = Mockito.mock(PackstreamValueReader.class);
        var ex = new PackstreamReaderException("Something went kaput :(");

        Mockito.doThrow(ex).when(reader).readMap();

        var connection =
                ConnectionMockFactory.newFactory().withValueReader(reader).build();

        assertThatExceptionOfType(IllegalStructArgumentException.class)
                .isThrownBy(() -> this.getDecoder().read(connection, buf, new StructHeader(1, (short) 0x42)))
                .withMessage("Illegal value for field \"metadata\": Something went kaput :(")
                .withCause(ex);
    }

    @Test
    protected void shouldFailWithIllegalStructArgumentWhenInvalidMetadataEntryIsPassed()
            throws PackstreamReaderException {
        var buf = PackstreamBuf.allocUnpooled();
        var reader = Mockito.mock(PackstreamValueReader.class);

        var meta = new MapValueBuilder();
        meta.add("tx_timeout", Values.stringValue("✨✨ nonsense ✨✨"));

        Mockito.doReturn(meta.build()).when(reader).readMap();

        var connection =
                ConnectionMockFactory.newFactory().withValueReader(reader).build();

        assertThatExceptionOfType(IllegalStructArgumentException.class)
                .isThrownBy(() -> this.getDecoder().read(connection, buf, new StructHeader(1, (short) 0x42)))
                .withMessage(
                        "Illegal value for field \"metadata\": Illegal value for field \"tx_timeout\": Expected long")
                .withCauseInstanceOf(IllegalStructArgumentException.class);
    }
}
