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
import org.neo4j.packstream.error.reader.PackstreamReaderException;
import org.neo4j.packstream.error.reader.UnexpectedTypeException;
import org.neo4j.packstream.error.struct.IllegalStructArgumentException;
import org.neo4j.packstream.error.struct.IllegalStructSizeException;
import org.neo4j.packstream.io.PackstreamBuf;
import org.neo4j.packstream.struct.StructHeader;

class PullMessageDecoderTest {

    @Test
    void shouldReadMessage() throws PackstreamReaderException {
        var buf = PackstreamBuf.allocUnpooled()
                .writeMapHeader(2)
                .writeString("n")
                .writeInt(42)
                .writeString("qid")
                .writeInt(21);

        var msg = PullMessageDecoder.getInstance().read(buf, new StructHeader(1, (short) 0x42));

        assertThat(msg).isNotNull();
        assertThat(msg.n()).isEqualTo(42);
        assertThat(msg.statementId()).isEqualTo(21);
    }

    @Test
    void shouldReadMessageWithAllRecordMarker() throws PackstreamReaderException {
        var buf = PackstreamBuf.allocUnpooled()
                .writeMapHeader(2)
                .writeString("n")
                .writeInt(-1)
                .writeString("qid")
                .writeInt(21);

        var msg = PullMessageDecoder.getInstance().read(buf, new StructHeader(1, (short) 0x42));

        assertThat(msg).isNotNull();
        assertThat(msg.n()).isEqualTo(-1);
        assertThat(msg.statementId()).isEqualTo(21);
    }

    @Test
    void shouldFailWithIllegalStructArgumentWhenZeroRecordsAreDiscarded() {
        var buf = PackstreamBuf.allocUnpooled()
                .writeMapHeader(2)
                .writeString("n")
                .writeInt(0)
                .writeString("qid")
                .writeInt(21);

        assertThatExceptionOfType(IllegalStructArgumentException.class)
                .isThrownBy(() -> PullMessageDecoder.getInstance().read(buf, new StructHeader(1, (short) 0x42)))
                .withMessage(
                        "Illegal value for field \"meta\": Illegal value for field \"n\": Expecting size to be at least 1, but got: 0")
                .withCauseInstanceOf(IllegalStructArgumentException.class);
    }

    @Test
    void shouldFailWithIllegalStructArgumentWhenNegativeRecordsAreDiscarded() {
        var buf = PackstreamBuf.allocUnpooled()
                .writeMapHeader(2)
                .writeString("n")
                .writeInt(-2)
                .writeString("qid")
                .writeInt(21);

        assertThatExceptionOfType(IllegalStructArgumentException.class)
                .isThrownBy(() -> PullMessageDecoder.getInstance().read(buf, new StructHeader(1, (short) 0x42)))
                .withMessage(
                        "Illegal value for field \"meta\": Illegal value for field \"n\": Expecting size to be at least 1, but got: -2")
                .withCauseInstanceOf(IllegalStructArgumentException.class);
    }

    @Test
    void shouldFailWithIllegalStructSizeWhenEmptyStructIsGiven() {
        var decoder = PullMessageDecoder.getInstance();

        assertThatExceptionOfType(IllegalStructSizeException.class)
                .isThrownBy(() -> decoder.read(PackstreamBuf.allocUnpooled(), new StructHeader(0, (short) 0x42)))
                .withMessage("Illegal struct size: Expected struct to be 1 fields but got 0");
    }

    @Test
    void shouldFailWithIllegalStructSizeWhenLargeStructIsGiven() {
        var decoder = PullMessageDecoder.getInstance();

        assertThatExceptionOfType(IllegalStructSizeException.class)
                .isThrownBy(() -> decoder.read(PackstreamBuf.allocUnpooled(), new StructHeader(2, (short) 0x42)))
                .withMessage("Illegal struct size: Expected struct to be 1 fields but got 2");
    }

    @Test
    void shouldFailWithIllegalStructArgumentWhenInvalidArgumentIsPassed() {
        var buf = PackstreamBuf.allocUnpooled().writeInt(42);

        var decoder = PullMessageDecoder.getInstance();

        assertThatExceptionOfType(IllegalStructArgumentException.class)
                .isThrownBy(() -> decoder.read(buf, new StructHeader(1, (short) 0x42)))
                .withMessage("Illegal value for field \"meta\": Unexpected type: Expected MAP but got INT")
                .withCauseInstanceOf(UnexpectedTypeException.class);
    }

    @Test
    void shouldFailWithIllegalStructArgumentWhenInvalidMetadataEntryIsPassed() {
        var buf =
                PackstreamBuf.allocUnpooled().writeMapHeader(1).writeString("n").writeString("✨✨ nonsense ✨✨");

        var decoder = PullMessageDecoder.getInstance();

        assertThatExceptionOfType(IllegalStructArgumentException.class)
                .isThrownBy(() -> decoder.read(buf, new StructHeader(1, (short) 0x42)))
                .withMessage(
                        "Illegal value for field \"meta\": Illegal value for field \"n\": Expecting size to be a Long value, but got: String(\"✨✨ nonsense ✨✨\")")
                .withCauseInstanceOf(IllegalStructArgumentException.class)
                .havingCause()
                .withMessage(
                        "Illegal value for field \"n\": Expecting size to be a Long value, but got: String(\"✨✨ nonsense ✨✨\")");
    }

    @Test
    void shouldFailWithIllegalStructArgumentWhenNumberOfRecordsIsOmitted() {
        var buf = PackstreamBuf.allocUnpooled().writeMapHeader(0);

        var decoder = PullMessageDecoder.getInstance();

        assertThatExceptionOfType(IllegalStructArgumentException.class)
                .isThrownBy(() -> decoder.read(buf, new StructHeader(1, (short) 0x42)))
                .withMessage(
                        "Illegal value for field \"meta\": Illegal value for field \"n\": Expecting size to be a Long value, but got: NO_VALUE")
                .withCauseInstanceOf(IllegalStructArgumentException.class);
    }
}
