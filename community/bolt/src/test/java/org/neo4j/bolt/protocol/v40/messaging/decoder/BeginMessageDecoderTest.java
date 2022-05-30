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
import static org.mockito.Mockito.RETURNS_DEFAULTS;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;

import java.time.Duration;
import org.junit.jupiter.api.Test;
import org.neo4j.bolt.protocol.common.bookmark.BookmarksParser;
import org.neo4j.bolt.protocol.common.message.AccessMode;
import org.neo4j.bolt.protocol.error.bookmark.BookmarkParserException;
import org.neo4j.bolt.protocol.error.bookmark.MalformedBookmarkException;
import org.neo4j.packstream.error.reader.PackstreamReaderException;
import org.neo4j.packstream.error.reader.UnexpectedTypeException;
import org.neo4j.packstream.error.struct.IllegalStructArgumentException;
import org.neo4j.packstream.error.struct.IllegalStructSizeException;
import org.neo4j.packstream.io.PackstreamBuf;
import org.neo4j.packstream.struct.StructHeader;
import org.neo4j.values.virtual.ListValue;

class BeginMessageDecoderTest {

    @Test
    void shouldReadMessage() throws PackstreamReaderException {
        var bookmarkParser = mock(BookmarksParser.class, RETURNS_DEFAULTS);

        var buf = PackstreamBuf.allocUnpooled()
                .writeMapHeader(5)
                .writeString("bookmarks")
                .writeListHeader(2)
                .writeString("neo4j:mock:bookmark1")
                .writeString("neo4j:mock:bookmark2")
                .writeString("tx_timeout")
                .writeInt(42)
                .writeString("mode")
                .writeString("w")
                .writeString("tx_metadata")
                .writeMapHeader(2)
                .writeString("foo") // key
                .writeString("bar") // value
                .writeString("baz") // key
                .writeInt(4) // value - decided by a fair dice roll
                .writeString("db")
                .writeString("neo4j");

        var msg = new BeginMessageDecoder(bookmarkParser).read(buf, new StructHeader(1, (short) 0x42));

        assertThat(msg).isNotNull();
        assertThat(msg.bookmarks()).isEmpty();
        assertThat(msg.transactionTimeout()).isEqualByComparingTo(Duration.ofMillis(42));
        assertThat(msg.getAccessMode()).isEqualTo(AccessMode.WRITE);
        assertThat(msg.transactionMetadata()).containsEntry("foo", "bar").containsEntry("baz", 4L);
        assertThat(msg.databaseName()).isEqualTo("neo4j");

        verify(bookmarkParser).parseBookmarks(any(ListValue.class));
        verifyNoMoreInteractions(bookmarkParser);
    }

    @Test
    void shouldFailWithIllegalStructSizeWhenEmptyStructIsGiven() {
        var decoder = new BeginMessageDecoder(mock(BookmarksParser.class));

        assertThatExceptionOfType(IllegalStructSizeException.class)
                .isThrownBy(() -> decoder.read(PackstreamBuf.allocUnpooled(), new StructHeader(0, (short) 0x42)))
                .withMessage("Illegal struct size: Expected struct to be 1 fields but got 0");
    }

    @Test
    void shouldFailWithIllegalStructSizeWhenLargerStructIsGiven() {
        var decoder = new BeginMessageDecoder(mock(BookmarksParser.class));

        assertThatExceptionOfType(IllegalStructSizeException.class)
                .isThrownBy(() -> decoder.read(PackstreamBuf.allocUnpooled(), new StructHeader(2, (short) 0x42)))
                .withMessage("Illegal struct size: Expected struct to be 1 fields but got 2");
    }

    @Test
    void shouldFailWithBookmarkParserExceptionWhenParsingFails() throws BookmarkParserException {
        var bookmarkParser = mock(BookmarksParser.class);
        doThrow(new MalformedBookmarkException("Something went wrong! :("))
                .when(bookmarkParser)
                .parseBookmarks(any());

        var buf = PackstreamBuf.allocUnpooled()
                .writeMapHeader(1)
                .writeString("bookmarks")
                .writeListHeader(2)
                .writeString("neo4j:mock:bookmark")
                .writeString("neo4j:mock:bookmark");

        var decoder = new BeginMessageDecoder(bookmarkParser);

        assertThatExceptionOfType(MalformedBookmarkException.class)
                .isThrownBy(() -> decoder.read(buf, new StructHeader(1, (short) 0x42)))
                .withMessage("Something went wrong! :(");
    }

    @Test
    void shouldFailWithIllegalStructArgumentWhenInvalidArgumentIsPassed() {
        var buf = PackstreamBuf.allocUnpooled().writeInt(42);

        var decoder = new BeginMessageDecoder(mock(BookmarksParser.class));

        assertThatExceptionOfType(IllegalStructArgumentException.class)
                .isThrownBy(() -> decoder.read(buf, new StructHeader(1, (short) 0x42)))
                .withMessage("Illegal value for field \"metadata\": Unexpected type: Expected MAP but got INT")
                .withCauseInstanceOf(UnexpectedTypeException.class);
    }

    @Test
    void shouldFailWithIllegalStructArgumentWhenInvalidMetadataEntryIsPassed() {
        var buf = PackstreamBuf.allocUnpooled()
                .writeMapHeader(1)
                .writeString("tx_timeout")
                .writeString("✨✨ nonsense ✨✨");

        var decoder = new BeginMessageDecoder(mock(BookmarksParser.class));

        assertThatExceptionOfType(IllegalStructArgumentException.class)
                .isThrownBy(() -> decoder.read(buf, new StructHeader(1, (short) 0x42)))
                .withMessage(
                        "Illegal value for field \"metadata\": Illegal value for field \"tx_timeout\": Expecting transaction timeout value to be a Long value, but got: String(\"✨✨ nonsense ✨✨\")")
                .withCauseInstanceOf(IllegalStructArgumentException.class);
    }
}
