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
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.time.temporal.ChronoField;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;
import org.neo4j.packstream.error.reader.PackstreamReaderException;
import org.neo4j.packstream.error.struct.IllegalStructArgumentException;
import org.neo4j.packstream.error.struct.IllegalStructSizeException;
import org.neo4j.packstream.io.PackstreamBuf;
import org.neo4j.packstream.struct.StructHeader;

class LegacyDateTimeReaderTest {

    @Test
    void shouldReadDateTime() throws PackstreamReaderException {
        // Important: This is _NOT_ a UNIX timestamp - This value refers to epoch as observed _WITHIN_ the current
        // offset.
        var buf = PackstreamBuf.allocUnpooled().writeInt(14218662).writeInt(436).writeInt(7200);

        var dateTime = LegacyDateTimeReader.getInstance().read(null, buf, new StructHeader(3, (short) 0x42));

        Assertions.assertThat(dateTime.get(ChronoField.YEAR)).isEqualTo(1970);
        Assertions.assertThat(dateTime.get(ChronoField.MONTH_OF_YEAR)).isEqualTo(6);
        Assertions.assertThat(dateTime.get(ChronoField.DAY_OF_MONTH)).isEqualTo(14);

        Assertions.assertThat(dateTime.get(ChronoField.HOUR_OF_DAY)).isEqualTo(13);
        Assertions.assertThat(dateTime.get(ChronoField.MINUTE_OF_HOUR)).isEqualTo(37);
        Assertions.assertThat(dateTime.get(ChronoField.SECOND_OF_MINUTE)).isEqualTo(42);
        Assertions.assertThat(dateTime.get(ChronoField.NANO_OF_SECOND)).isEqualTo(436);
    }

    @Test
    void shouldFailWithIllegalStructArgumentWhenLargeNanosIsGiven() {
        var buf = PackstreamBuf.allocUnpooled()
                .writeInt(14218662)
                .writeInt(Integer.MAX_VALUE + 1L)
                .writeInt(7200);

        assertThatThrownBy(() -> LegacyDateTimeReader.getInstance().read(null, buf, new StructHeader(3, (short) 0x42)))
                .isInstanceOf(IllegalStructArgumentException.class)
                .hasMessage("Illegal value for field \"nanoseconds\": Value is out of bounds")
                .hasNoCause()
                .satisfies(ex -> assertThat(((IllegalStructArgumentException) ex).getFieldName())
                        .isEqualTo("nanoseconds"));
    }

    @Test
    void shouldFailWithIllegalStructArgumentWhenLargeNegativeNanosIsGiven() {
        var buf = PackstreamBuf.allocUnpooled()
                .writeInt(14218662)
                .writeInt(Integer.MIN_VALUE - 1L)
                .writeInt(7200);

        assertThatThrownBy(() -> LegacyDateTimeReader.getInstance().read(null, buf, new StructHeader(3, (short) 0x42)))
                .isInstanceOf(IllegalStructArgumentException.class)
                .hasMessage("Illegal value for field \"nanoseconds\": Value is out of bounds")
                .hasNoCause()
                .satisfies(ex -> assertThat(((IllegalStructArgumentException) ex).getFieldName())
                        .isEqualTo("nanoseconds"));
    }

    @Test
    void shouldFailWithIllegalStructArgumentWhenLargeTimezoneOffsetIsGiven() {
        var buf = PackstreamBuf.allocUnpooled().writeInt(14218662).writeInt(374).writeInt(Integer.MAX_VALUE + 1L);

        assertThatThrownBy(() -> LegacyDateTimeReader.getInstance().read(null, buf, new StructHeader(3, (short) 0x42)))
                .isInstanceOf(IllegalStructArgumentException.class)
                .hasMessage("Illegal value for field \"tz_offset_seconds\": Value is out of bounds")
                .hasNoCause()
                .satisfies(ex -> assertThat(((IllegalStructArgumentException) ex).getFieldName())
                        .isEqualTo("tz_offset_seconds"));
    }

    @Test
    void shouldFailWithIllegalStructArgumentWhenLargeNegativeTimezoneOffsetIsGiven() {
        var buf = PackstreamBuf.allocUnpooled().writeInt(14218662).writeInt(374).writeInt(Integer.MIN_VALUE - 1L);

        assertThatThrownBy(() -> LegacyDateTimeReader.getInstance().read(null, buf, new StructHeader(3, (short) 0x42)))
                .isInstanceOf(IllegalStructArgumentException.class)
                .hasMessage("Illegal value for field \"tz_offset_seconds\": Value is out of bounds")
                .hasNoCause()
                .satisfies(ex -> assertThat(((IllegalStructArgumentException) ex).getFieldName())
                        .isEqualTo("tz_offset_seconds"));
    }

    @Test
    void shouldFailWithIllegalStructSizeWhenEmptyStructIsGiven() {
        var reader = LegacyDateTimeReader.getInstance();

        assertThatExceptionOfType(IllegalStructSizeException.class)
                .isThrownBy(() -> reader.read(null, PackstreamBuf.allocUnpooled(), new StructHeader(0, (short) 0x42)))
                .withMessage("Illegal struct size: Expected struct to be 3 fields but got 0")
                .withNoCause();
    }

    @Test
    void shouldFailWithIllegalStructSizeWhenSmallStructIsGiven() {
        var reader = LegacyDateTimeReader.getInstance();

        assertThatExceptionOfType(IllegalStructSizeException.class)
                .isThrownBy(() -> reader.read(null, PackstreamBuf.allocUnpooled(), new StructHeader(2, (short) 0x42)))
                .withMessage("Illegal struct size: Expected struct to be 3 fields but got 2")
                .withNoCause();
    }

    @Test
    void shouldFailWithIllegalStructSizeWhenLargeStructIsGiven() {
        var reader = LegacyDateTimeReader.getInstance();

        assertThatExceptionOfType(IllegalStructSizeException.class)
                .isThrownBy(() -> reader.read(null, PackstreamBuf.allocUnpooled(), new StructHeader(4, (short) 0x42)))
                .withMessage("Illegal struct size: Expected struct to be 3 fields but got 4")
                .withNoCause();
    }
}
