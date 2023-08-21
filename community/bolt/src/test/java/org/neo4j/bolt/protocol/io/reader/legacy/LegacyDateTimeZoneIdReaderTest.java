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
package org.neo4j.bolt.protocol.io.reader.legacy;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatExceptionOfType;

import java.time.temporal.ChronoField;
import org.junit.jupiter.api.Test;
import org.neo4j.packstream.error.reader.PackstreamReaderException;
import org.neo4j.packstream.error.struct.IllegalStructSizeException;
import org.neo4j.packstream.io.PackstreamBuf;
import org.neo4j.packstream.struct.StructHeader;
import org.neo4j.values.storable.StringValue;

class LegacyDateTimeZoneIdReaderTest {

    @Test
    void shouldReadDateTimeZoneId() throws PackstreamReaderException {
        // Important: This is _NOT_ a UNIX timestamp - This value refers to epoch as observed _WITHIN_ the current
        // offset.
        var buf =
                PackstreamBuf.allocUnpooled().writeInt(803137062).writeInt(1337).writeString("Europe/Berlin");

        var dateTime = LegacyDateTimeZoneIdReader.getInstance().read(null, buf, new StructHeader(3, (short) 0x42));

        assertThat(dateTime.get(ChronoField.YEAR)).isEqualTo(1995);
        assertThat(dateTime.get(ChronoField.MONTH_OF_YEAR)).isEqualTo(6);
        assertThat(dateTime.get(ChronoField.DAY_OF_MONTH)).isEqualTo(14);

        assertThat(dateTime.get(ChronoField.HOUR_OF_DAY)).isEqualTo(13);
        assertThat(dateTime.get(ChronoField.MINUTE_OF_HOUR)).isEqualTo(37);
        assertThat(dateTime.get(ChronoField.SECOND_OF_MINUTE)).isEqualTo(42);
        assertThat(dateTime.get(ChronoField.NANO_OF_SECOND)).isEqualTo(1337);

        assertThat(((StringValue) dateTime.get("timezone")).stringValue()).isEqualTo("Europe/Berlin");
    }

    @Test
    void shouldFailWithIllegalStructSizeWhenEmptyStructIsGiven() {
        var reader = LegacyDateTimeZoneIdReader.getInstance();

        assertThatExceptionOfType(IllegalStructSizeException.class)
                .isThrownBy(() -> reader.read(null, PackstreamBuf.allocUnpooled(), new StructHeader(0, (short) 0x42)))
                .withMessage("Illegal struct size: Expected struct to be 3 fields but got 0")
                .withNoCause();
    }

    @Test
    void shouldFailWithIllegalStructSizeWhenSmallStructIsGiven() {
        var reader = LegacyDateTimeZoneIdReader.getInstance();

        assertThatExceptionOfType(IllegalStructSizeException.class)
                .isThrownBy(() -> reader.read(null, PackstreamBuf.allocUnpooled(), new StructHeader(2, (short) 0x42)))
                .withMessage("Illegal struct size: Expected struct to be 3 fields but got 2")
                .withNoCause();
    }

    @Test
    void shouldFailWithIllegalStructSizeWhenLargeStructIsGiven() {
        var reader = LegacyDateTimeZoneIdReader.getInstance();

        assertThatExceptionOfType(IllegalStructSizeException.class)
                .isThrownBy(() -> reader.read(null, PackstreamBuf.allocUnpooled(), new StructHeader(4, (short) 0x42)))
                .withMessage("Illegal struct size: Expected struct to be 3 fields but got 4")
                .withNoCause();
    }
}
