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

import java.time.temporal.ChronoField;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;
import org.neo4j.packstream.error.reader.PackstreamReaderException;
import org.neo4j.packstream.error.struct.IllegalStructSizeException;
import org.neo4j.packstream.io.PackstreamBuf;
import org.neo4j.packstream.struct.StructHeader;

class LocalDateTimeReaderTest {
    @Test
    void shouldReadLocalDateTime() throws PackstreamReaderException {
        var buf = PackstreamBuf.allocUnpooled().writeInt(803137062).writeInt(1337);

        var localDateTime = LocalDateTimeReader.getInstance().read(null, buf, new StructHeader(2, (short) 0x42));

        Assertions.assertThat(localDateTime.get(ChronoField.YEAR)).isEqualTo(1995);
        Assertions.assertThat(localDateTime.get(ChronoField.MONTH_OF_YEAR)).isEqualTo(6);
        Assertions.assertThat(localDateTime.get(ChronoField.DAY_OF_MONTH)).isEqualTo(14);

        Assertions.assertThat(localDateTime.get(ChronoField.HOUR_OF_DAY)).isEqualTo(13);
        Assertions.assertThat(localDateTime.get(ChronoField.MINUTE_OF_HOUR)).isEqualTo(37);
        Assertions.assertThat(localDateTime.get(ChronoField.SECOND_OF_MINUTE)).isEqualTo(42);
        Assertions.assertThat(localDateTime.get(ChronoField.NANO_OF_SECOND)).isEqualTo(1337);
    }

    @Test
    void shouldFailWithIllegalStructSizeWhenEmptyStructIsGiven() {
        var reader = LocalDateTimeReader.getInstance();

        assertThatExceptionOfType(IllegalStructSizeException.class)
                .isThrownBy(() -> reader.read(null, PackstreamBuf.allocUnpooled(), new StructHeader(0, (short) 0x42)))
                .withMessage("Illegal struct size: Expected struct to be 2 fields but got 0")
                .withNoCause();
    }

    @Test
    void shouldFailWithIllegalStructSizeWhenSmallStructIsGiven() {
        var reader = LocalDateTimeReader.getInstance();

        assertThatExceptionOfType(IllegalStructSizeException.class)
                .isThrownBy(() -> reader.read(null, PackstreamBuf.allocUnpooled(), new StructHeader(1, (short) 0x42)))
                .withMessage("Illegal struct size: Expected struct to be 2 fields but got 1")
                .withNoCause();
    }

    @Test
    void shouldFailWithIllegalStructSizeWhenLargeStructIsGiven() {
        var reader = LocalDateTimeReader.getInstance();

        assertThatExceptionOfType(IllegalStructSizeException.class)
                .isThrownBy(() -> reader.read(null, PackstreamBuf.allocUnpooled(), new StructHeader(3, (short) 0x42)))
                .withMessage("Illegal struct size: Expected struct to be 2 fields but got 3")
                .withNoCause();
    }
}
