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
import java.time.LocalDate;
import org.junit.jupiter.api.Test;
import org.neo4j.packstream.error.reader.PackstreamReaderException;
import org.neo4j.packstream.error.reader.UnexpectedTypeException;
import org.neo4j.packstream.error.struct.IllegalStructArgumentException;
import org.neo4j.packstream.error.struct.IllegalStructSizeException;
import org.neo4j.packstream.io.NativeStruct;
import org.neo4j.packstream.io.PackstreamBuf;
import org.neo4j.packstream.struct.StructHeader;

class HelloMessageDecoderTest {

    @Test
    void shouldReadMessage() throws PackstreamReaderException {
        var buf = PackstreamBuf.allocUnpooled()
                .writeMapHeader(2)
                .writeString("user_agent")
                .writeString("Example/1.0 (+https://github.com/neo4j)")
                .writeString("scheme")
                .writeString("none");

        var msg = HelloMessageDecoder.getInstance().read(buf, new StructHeader(1, (short) 0x42));

        assertThat(msg).isNotNull();
        assertThat(msg.userAgent()).isEqualTo("Example/1.0 (+https://github.com/neo4j)");
        assertThat(msg.authToken()).hasSize(2).containsEntry("scheme", "none");
    }

    @Test
    void shouldConvertSensitiveValues() throws PackstreamReaderException {
        var buf = PackstreamBuf.allocUnpooled()
                .writeMapHeader(2)
                .writeString("user_agent")
                .writeString("Example/1.0 (+https://github.com/neo4j)")
                .writeString("credentials")
                .writeString("5upers3cre7");

        var msg = HelloMessageDecoder.getInstance().read(buf, new StructHeader(1, (short) 0x42));

        assertThat(msg).isNotNull();
        assertThat(msg.authToken()).containsEntry("credentials", "5upers3cre7".getBytes(StandardCharsets.UTF_8));
    }

    @Test
    void shouldFailWithIllegalStructSizeWhenEmptyStructIsGiven() {
        var decoder = HelloMessageDecoder.getInstance();

        assertThatExceptionOfType(IllegalStructSizeException.class)
                .isThrownBy(() -> decoder.read(PackstreamBuf.allocUnpooled(), new StructHeader(0, (short) 0x42)))
                .withMessage("Illegal struct size: Expected struct to be 1 fields but got 0");
    }

    @Test
    void shouldFailWithIllegalStructSizeWhenLargeStructIsGiven() {
        var decoder = HelloMessageDecoder.getInstance();

        assertThatExceptionOfType(IllegalStructSizeException.class)
                .isThrownBy(() -> decoder.read(PackstreamBuf.allocUnpooled(), new StructHeader(2, (short) 0x42)))
                .withMessage("Illegal struct size: Expected struct to be 1 fields but got 2");
    }

    @Test
    void shouldFailWithIllegalStructArgumentWhenInvalidArgumentIsPassed() {
        var buf = PackstreamBuf.allocUnpooled().writeInt(42);

        var decoder = HelloMessageDecoder.getInstance();

        assertThatExceptionOfType(IllegalStructArgumentException.class)
                .isThrownBy(() -> decoder.read(buf, new StructHeader(1, (short) 0x42)))
                .withMessage("Illegal value for field \"extra\": Unexpected type: Expected MAP but got INT")
                .withCauseInstanceOf(UnexpectedTypeException.class);
    }

    @Test
    void shouldFailWithIllegalStructArgumentWhenInvalidMetadataEntryIsPassed() {
        var buf = PackstreamBuf.allocUnpooled()
                .writeMapHeader(1)
                .writeString("user_agent")
                .writeInt(42);

        var decoder = HelloMessageDecoder.getInstance();

        assertThatExceptionOfType(IllegalStructArgumentException.class)
                .isThrownBy(() -> decoder.read(buf, new StructHeader(1, (short) 0x42)))
                .withMessage("Illegal value for field \"user_agent\": Expected value to be a string");
    }

    @Test
    void shouldFailWithIllegalStructArgumentWhenUserAgentIsOmitted() {
        var buf = PackstreamBuf.allocUnpooled()
                .writeMapHeader(1)
                .writeString("scheme")
                .writeString("none");

        var decoder = HelloMessageDecoder.getInstance();

        assertThatExceptionOfType(IllegalStructArgumentException.class)
                .isThrownBy(() -> decoder.read(buf, new StructHeader(1, (short) 0x42)))
                .withMessage("Illegal value for field \"user_agent\": Expected \"user_agent\" to be non-null");
    }

    @Test
    void shouldFailWithIllegalStructArgumentWhenComplexValueIsPassed() {
        var buf = PackstreamBuf.allocUnpooled()
                .writeMapHeader(3)
                .writeString("user_agent")
                .writeString("Example/1.0 (+https://github.com/neo4j)")
                .writeString("scheme")
                .writeString("none")
                .writeString("credentials");
        NativeStruct.writeDate(buf, LocalDate.EPOCH);

        var decoder = HelloMessageDecoder.getInstance();

        assertThatExceptionOfType(IllegalStructArgumentException.class)
                .isThrownBy(() -> decoder.read(buf, new StructHeader(1, (short) 0x42)))
                .withMessage("Illegal value for field \"extra\": Unexpected type: STRUCT")
                .withCauseInstanceOf(UnexpectedTypeException.class);
    }
}
