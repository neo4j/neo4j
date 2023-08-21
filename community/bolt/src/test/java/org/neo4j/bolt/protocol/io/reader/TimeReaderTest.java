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
package org.neo4j.bolt.protocol.io.reader;

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

class TimeReaderTest {

    @Test
    void shouldReadTime() throws PackstreamReaderException {
        var buf = PackstreamBuf.allocUnpooled().writeInt(5_470_000_000_000L).writeInt(7200);

        var time = TimeReader.getInstance().read(null, buf, new StructHeader(2, (short) 0x42));

        Assertions.assertThat(time.get(ChronoField.HOUR_OF_DAY)).isEqualTo(1);
        Assertions.assertThat(time.get(ChronoField.MINUTE_OF_HOUR)).isEqualTo(31);
        Assertions.assertThat(time.get(ChronoField.SECOND_OF_MINUTE)).isEqualTo(10);
    }

    @Test
    void shouldFailWithIllegalStructArgumentWhenLargeTimeZoneOffsetIsGiven() {
        var buf = PackstreamBuf.allocUnpooled().writeInt(5_470_000_000_000L).writeInt(Integer.MAX_VALUE + 1L);

        assertThatThrownBy(() -> TimeReader.getInstance().read(null, buf, new StructHeader(2, (short) 0x42)))
                .isInstanceOf(IllegalStructArgumentException.class)
                .hasMessage("Illegal value for field \"tz_offset_seconds\": Value is out of bounds")
                .hasNoCause();
    }

    @Test
    void shouldFailWithIllegalStructArgumentWhenLargeNegativeTimeZoneOffsetIsGiven() {
        var buf = PackstreamBuf.allocUnpooled().writeInt(5_470_000_000_000L).writeInt(Integer.MIN_VALUE - 1L);

        assertThatThrownBy(() -> TimeReader.getInstance().read(null, buf, new StructHeader(2, (short) 0x42)))
                .isInstanceOf(IllegalStructArgumentException.class)
                .hasMessage("Illegal value for field \"tz_offset_seconds\": Value is out of bounds")
                .hasNoCause();
    }

    @Test
    void shouldFailWithIllegalStructSizeWhenEmptyStructIsGiven() {
        var reader = TimeReader.getInstance();

        assertThatExceptionOfType(IllegalStructSizeException.class)
                .isThrownBy(() -> reader.read(null, PackstreamBuf.allocUnpooled(), new StructHeader(0, (short) 0x42)))
                .withMessage("Illegal struct size: Expected struct to be 2 fields but got 0")
                .withNoCause();
    }

    @Test
    void shouldFailWithIllegalStructSizeWhenSmallStructIsGiven() {
        var reader = TimeReader.getInstance();

        assertThatExceptionOfType(IllegalStructSizeException.class)
                .isThrownBy(() -> reader.read(null, PackstreamBuf.allocUnpooled(), new StructHeader(1, (short) 0x42)))
                .withMessage("Illegal struct size: Expected struct to be 2 fields but got 1")
                .withNoCause();
    }

    @Test
    void shouldFailWithIllegalStructSizeWhenLargeStructIsGiven() {
        var reader = TimeReader.getInstance();

        assertThatExceptionOfType(IllegalStructSizeException.class)
                .isThrownBy(() -> reader.read(null, PackstreamBuf.allocUnpooled(), new StructHeader(3, (short) 0x42)))
                .withMessage("Illegal struct size: Expected struct to be 2 fields but got 3")
                .withNoCause();
    }
}
