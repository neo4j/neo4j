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
import static org.mockito.Mockito.RETURNS_DEFAULTS;
import static org.mockito.Mockito.mock;

import java.time.Duration;
import org.junit.jupiter.api.Test;
import org.neo4j.bolt.protocol.common.bookmark.BookmarksParser;
import org.neo4j.bolt.protocol.common.message.AccessMode;
import org.neo4j.packstream.error.reader.PackstreamReaderException;
import org.neo4j.packstream.error.reader.UnexpectedTypeException;
import org.neo4j.packstream.error.struct.IllegalStructArgumentException;
import org.neo4j.packstream.error.struct.IllegalStructSizeException;
import org.neo4j.packstream.io.PackstreamBuf;
import org.neo4j.packstream.struct.StructHeader;

class RunMessageDecoderTest {

    @Test
    void shouldReadMessage() throws PackstreamReaderException {
        var bookmarkParser = mock(BookmarksParser.class, RETURNS_DEFAULTS);

        var buf = PackstreamBuf.allocUnpooled()
                .writeString("RETURN $n")
                .writeMapHeader(1)
                .writeString("n")
                .writeInt(1)
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
                .writeString("foo")
                .writeString("bar")
                .writeString("the_answer")
                .writeInt(42)
                .writeString("db")
                .writeString("neo4j");

        var msg = new RunMessageDecoder(bookmarkParser).read(buf, new StructHeader(3, (short) 0x42));

        assertThat(msg).isNotNull();
        assertThat(msg.statement()).isEqualTo("RETURN $n");
        assertThat(msg.params().size()).isEqualTo(1);
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
        var decoder = new RunMessageDecoder(mock(BookmarksParser.class));

        assertThatExceptionOfType(IllegalStructSizeException.class)
                .isThrownBy(() -> decoder.read(PackstreamBuf.allocUnpooled(), new StructHeader(0, (short) 0x42)))
                .withMessage("Illegal struct size: Expected struct to be 3 fields but got 0");
    }

    @Test
    void shouldFailWithIllegalStructSizeWhenSmallStructIsGiven() {
        var decoder = new RunMessageDecoder(mock(BookmarksParser.class));

        assertThatExceptionOfType(IllegalStructSizeException.class)
                .isThrownBy(() -> decoder.read(PackstreamBuf.allocUnpooled(), new StructHeader(1, (short) 0x42)))
                .withMessage("Illegal struct size: Expected struct to be 3 fields but got 1");

        assertThatExceptionOfType(IllegalStructSizeException.class)
                .isThrownBy(() -> decoder.read(PackstreamBuf.allocUnpooled(), new StructHeader(2, (short) 0x42)))
                .withMessage("Illegal struct size: Expected struct to be 3 fields but got 2");
    }

    @Test
    void shouldFailWithIllegalStructSizeWhenLargeStructIsGiven() {
        var decoder = new RunMessageDecoder(mock(BookmarksParser.class));

        assertThatExceptionOfType(IllegalStructSizeException.class)
                .isThrownBy(() -> decoder.read(PackstreamBuf.allocUnpooled(), new StructHeader(4, (short) 0x42)))
                .withMessage("Illegal struct size: Expected struct to be 3 fields but got 4");
    }

    @Test
    void shouldFailWithIllegalStructArgumentWhenInvalidStatementArgumentIsPassed() {
        var buf = PackstreamBuf.allocUnpooled().writeInt(42);

        var decoder = new RunMessageDecoder(mock(BookmarksParser.class));

        assertThatExceptionOfType(IllegalStructArgumentException.class)
                .isThrownBy(() -> decoder.read(buf, new StructHeader(3, (short) 0x42)))
                .withMessage("Illegal value for field \"statement\": Unexpected type: Expected STRING but got INT")
                .withCauseInstanceOf(UnexpectedTypeException.class);
    }

    @Test
    void shouldFailWithIllegalStructArgumentWhenInvalidParamsArgumentIsPassed() {
        var buf = PackstreamBuf.allocUnpooled().writeString("RETURN 1").writeInt(42);

        var decoder = new RunMessageDecoder(mock(BookmarksParser.class));

        assertThatExceptionOfType(IllegalStructArgumentException.class)
                .isThrownBy(() -> decoder.read(buf, new StructHeader(3, (short) 0x42)))
                .withMessage("Illegal value for field \"params\": Unexpected type: Expected MAP but got INT")
                .withCauseInstanceOf(UnexpectedTypeException.class);
    }

    @Test
    void shouldFailWithIllegalStructArgumentWhenInvalidMetadataEntryIsPassed() {
        var buf = PackstreamBuf.allocUnpooled()
                .writeString("RETURN 1")
                .writeMapHeader(0)
                .writeMapHeader(1)
                .writeString("tx_timeout")
                .writeString("✨✨ nonsense ✨✨");

        var decoder = new RunMessageDecoder(mock(BookmarksParser.class));

        assertThatExceptionOfType(IllegalStructArgumentException.class)
                .isThrownBy(() -> decoder.read(buf, new StructHeader(3, (short) 0x42)))
                .withMessage(
                        "Illegal value for field \"metadata\": Illegal value for field \"tx_timeout\": Expecting transaction timeout value to be a Long value, but got: String(\"✨✨ nonsense ✨✨\")")
                .withCauseInstanceOf(IllegalStructArgumentException.class);
    }
}
