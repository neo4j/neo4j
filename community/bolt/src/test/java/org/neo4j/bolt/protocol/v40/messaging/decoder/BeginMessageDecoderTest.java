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
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;

import java.time.Duration;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;
import org.neo4j.bolt.protocol.common.bookmark.BookmarkParser;
import org.neo4j.bolt.protocol.common.message.AccessMode;
import org.neo4j.bolt.protocol.error.bookmark.MalformedBookmarkException;
import org.neo4j.bolt.testing.mock.ConnectionMockFactory;
import org.neo4j.packstream.error.reader.PackstreamReaderException;
import org.neo4j.packstream.error.struct.IllegalStructArgumentException;
import org.neo4j.packstream.error.struct.IllegalStructSizeException;
import org.neo4j.packstream.io.PackstreamBuf;
import org.neo4j.packstream.io.value.PackstreamValueReader;
import org.neo4j.packstream.struct.StructHeader;
import org.neo4j.values.storable.Values;
import org.neo4j.values.virtual.MapValueBuilder;
import org.neo4j.values.virtual.VirtualValues;

class BeginMessageDecoderTest {

    @Test
    void shouldReadMessage() throws PackstreamReaderException {
        var buf = PackstreamBuf.allocUnpooled();
        var reader = Mockito.mock(PackstreamValueReader.class);

        var txMetadata = new MapValueBuilder();
        txMetadata.add("foo", Values.stringValue("bar"));
        txMetadata.add("baz", Values.longValue(4));

        var meta = new MapValueBuilder();
        meta.add(
                "bookmarks",
                VirtualValues.list(
                        Values.stringValue("neo4j:mock:bookmark1"), Values.stringValue("neo4j:mock:bookmark2")));
        meta.add("tx_timeout", Values.longValue(42));
        meta.add("mode", Values.stringValue("w"));
        meta.add("tx_metadata", txMetadata.build());
        meta.add("db", Values.stringValue("neo4j"));

        Mockito.doReturn(meta.build()).when(reader).readMap();

        var connection =
                ConnectionMockFactory.newFactory().withValueReader(reader).build();

        var msg = BeginMessageDecoder.getInstance().read(connection, buf, new StructHeader(1, (short) 0x42));

        assertThat(msg).isNotNull();
        assertThat(msg.bookmarks()).isEmpty();
        assertThat(msg.transactionTimeout()).isEqualByComparingTo(Duration.ofMillis(42));
        assertThat(msg.getAccessMode()).isEqualTo(AccessMode.WRITE);
        assertThat(msg.transactionMetadata()).containsEntry("foo", "bar").containsEntry("baz", 4L);
        assertThat(msg.databaseName()).isEqualTo("neo4j");
    }

    @Test
    void shouldFailWithIllegalStructSizeWhenEmptyStructIsGiven() {
        assertThatExceptionOfType(IllegalStructSizeException.class)
                .isThrownBy(() -> BeginMessageDecoder.getInstance()
                        .read(
                                ConnectionMockFactory.newInstance(),
                                PackstreamBuf.allocUnpooled(),
                                new StructHeader(0, (short) 0x42)))
                .withMessage("Illegal struct size: Expected struct to be 1 fields but got 0");
    }

    @Test
    void shouldFailWithIllegalStructSizeWhenLargerStructIsGiven() {
        assertThatExceptionOfType(IllegalStructSizeException.class)
                .isThrownBy(() -> BeginMessageDecoder.getInstance()
                        .read(
                                ConnectionMockFactory.newInstance(),
                                PackstreamBuf.allocUnpooled(),
                                new StructHeader(2, (short) 0x42)))
                .withMessage("Illegal struct size: Expected struct to be 1 fields but got 2");
    }

    @Test
    void shouldFailWithBookmarkParserExceptionWhenParsingFails() throws PackstreamReaderException {
        var buf = PackstreamBuf.allocUnpooled();
        var reader = Mockito.mock(PackstreamValueReader.class);

        var meta = new MapValueBuilder();
        meta.add(
                "bookmarks",
                VirtualValues.list(
                        Values.stringValue("neo4j:mock:bookmark1"), Values.stringValue("neo4j:mock:bookmark2")));

        Mockito.doReturn(meta.build()).when(reader).readMap();

        var bookmarkParser = mock(BookmarkParser.class);
        doThrow(new MalformedBookmarkException("Something went wrong! :("))
                .when(bookmarkParser)
                .parseBookmarks(any());

        var connection = ConnectionMockFactory.newFactory()
                .withConnector(factory -> factory.withBookmarkParser(bookmarkParser))
                .withValueReader(reader)
                .build();

        assertThatExceptionOfType(MalformedBookmarkException.class)
                .isThrownBy(() ->
                        BeginMessageDecoder.getInstance().read(connection, buf, new StructHeader(1, (short) 0x42)))
                .withMessage("Something went wrong! :(");
    }

    @Test
    void shouldFailWithIllegalStructArgumentWhenInvalidArgumentIsPassed() throws PackstreamReaderException {
        var buf = PackstreamBuf.allocUnpooled();
        var reader = Mockito.mock(PackstreamValueReader.class);
        var ex = new PackstreamReaderException("Something went kaput :(");

        Mockito.doThrow(ex).when(reader).readMap();

        var connection =
                ConnectionMockFactory.newFactory().withValueReader(reader).build();

        assertThatExceptionOfType(IllegalStructArgumentException.class)
                .isThrownBy(() ->
                        BeginMessageDecoder.getInstance().read(connection, buf, new StructHeader(1, (short) 0x42)))
                .withMessage("Illegal value for field \"metadata\": Something went kaput :(")
                .withCause(ex);
    }

    @Test
    void shouldFailWithIllegalStructArgumentWhenInvalidMetadataEntryIsPassed() throws PackstreamReaderException {
        var buf = PackstreamBuf.allocUnpooled();
        var reader = Mockito.mock(PackstreamValueReader.class);

        var meta = new MapValueBuilder();
        meta.add("tx_timeout", Values.stringValue("✨✨ nonsense ✨✨"));

        Mockito.doReturn(meta.build()).when(reader).readMap();

        var connection =
                ConnectionMockFactory.newFactory().withValueReader(reader).build();

        assertThatExceptionOfType(IllegalStructArgumentException.class)
                .isThrownBy(() ->
                        BeginMessageDecoder.getInstance().read(connection, buf, new StructHeader(1, (short) 0x42)))
                .withMessage(
                        "Illegal value for field \"metadata\": Illegal value for field \"tx_timeout\": Expecting transaction timeout value to be a Long value, but got: String(\"✨✨ nonsense ✨✨\")")
                .withCauseInstanceOf(IllegalStructArgumentException.class);
    }
}
