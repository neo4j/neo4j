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

import java.time.temporal.ChronoUnit;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;
import org.neo4j.packstream.error.reader.PackstreamReaderException;
import org.neo4j.packstream.error.struct.IllegalStructSizeException;
import org.neo4j.packstream.io.PackstreamBuf;
import org.neo4j.packstream.struct.StructHeader;

class DurationReaderTest {

    @Test
    void shouldReadDuration() throws PackstreamReaderException {
        var buf = PackstreamBuf.allocUnpooled()
                .writeInt(2)
                .writeInt(9)
                .writeInt(3602)
                .writeInt(329);

        var duration = DurationReader.getInstance().read(null, buf, new StructHeader(4, (short) 0x42));

        Assertions.assertThat(duration.get(ChronoUnit.MONTHS)).isEqualTo(2);
        Assertions.assertThat(duration.get(ChronoUnit.DAYS)).isEqualTo(9);
        Assertions.assertThat(duration.get(ChronoUnit.SECONDS)).isEqualTo(3602);
        Assertions.assertThat(duration.get(ChronoUnit.NANOS)).isEqualTo(329);
    }

    @Test
    void shouldFailWithIllegalStructSizeWhenEmptyStructIsGiven() {
        var reader = DurationReader.getInstance();

        assertThatExceptionOfType(IllegalStructSizeException.class)
                .isThrownBy(() -> reader.read(null, PackstreamBuf.allocUnpooled(), new StructHeader(0, (short) 0x42)))
                .withMessage("Illegal struct size: Expected struct to be 4 fields but got 0")
                .withNoCause();
    }

    @Test
    void shouldFailWithIllegalStructSizeWhenSmallStructIsGiven() {
        var reader = DurationReader.getInstance();

        assertThatExceptionOfType(IllegalStructSizeException.class)
                .isThrownBy(() -> reader.read(null, PackstreamBuf.allocUnpooled(), new StructHeader(2, (short) 0x42)))
                .withMessage("Illegal struct size: Expected struct to be 4 fields but got 2")
                .withNoCause();
    }

    @Test
    void shouldFailWithIllegalStructSizeWhenLargeStructIsGiven() {
        var reader = DurationReader.getInstance();

        assertThatExceptionOfType(IllegalStructSizeException.class)
                .isThrownBy(() -> reader.read(null, PackstreamBuf.allocUnpooled(), new StructHeader(5, (short) 0x42)))
                .withMessage("Illegal struct size: Expected struct to be 4 fields but got 5")
                .withNoCause();
    }
}
