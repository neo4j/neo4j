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

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.stream.LongStream;
import java.util.stream.Stream;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.DynamicTest;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestFactory;
import org.neo4j.exceptions.InvalidArgumentException;
import org.neo4j.exceptions.InvalidSpatialArgumentException;
import org.neo4j.packstream.error.reader.PackstreamReaderException;
import org.neo4j.packstream.error.struct.IllegalStructArgumentException;
import org.neo4j.packstream.error.struct.IllegalStructSizeException;
import org.neo4j.packstream.io.PackstreamBuf;
import org.neo4j.packstream.struct.StructHeader;
import org.neo4j.packstream.struct.StructReader;
import org.neo4j.values.storable.CoordinateReferenceSystem;
import org.neo4j.values.storable.PointValue;

public abstract class AbstractPointReaderTest {

    protected abstract StructReader<?, PointValue> getReader();

    protected abstract double[] getCoordinates();

    protected abstract long getStructSize();

    @Test
    void shouldReadPoint() throws PackstreamReaderException {
        var coords = this.getCoordinates();
        var buf = PackstreamBuf.allocUnpooled();

        var crs = CoordinateReferenceSystem.CARTESIAN;
        if (coords.length == 3) {
            crs = CoordinateReferenceSystem.CARTESIAN_3D;
        }

        buf.writeInt(crs.getCode());
        for (var coord : coords) {
            buf.writeFloat(coord);
        }

        var value = this.getReader().read(null, buf, new StructHeader(this.getStructSize(), (short) 0x42));

        Assertions.assertThat(value.getCoordinateReferenceSystem()).isEqualTo(crs);
        Assertions.assertThat(value.coordinate()).isEqualTo(coords);
    }

    @Test
    void shouldFailWithIllegalStructSizeWhenEmptyStructIsGiven() {
        Assertions.assertThatExceptionOfType(IllegalStructSizeException.class)
                .isThrownBy(() ->
                        this.getReader().read(null, PackstreamBuf.allocUnpooled(), new StructHeader(0, (short) 0x42)))
                .withMessage("Illegal struct size: Expected struct to be 3 fields but got 0")
                .withNoCause();
    }

    @TestFactory
    Stream<DynamicTest> shouldFailWithIllegalStructSizeWhenSmallStructIsGiven() {
        return LongStream.range(1, this.getStructSize())
                .mapToObj(size -> DynamicTest.dynamicTest(
                        size + " elements", () -> Assertions.assertThatExceptionOfType(IllegalStructSizeException.class)
                                .isThrownBy(() -> this.getReader()
                                        .read(null, PackstreamBuf.allocUnpooled(), new StructHeader(size, (short)
                                                0x42)))
                                .withMessage("Illegal struct size: Expected struct to be " + this.getStructSize()
                                        + " fields but got " + size)
                                .withNoCause()));
    }

    @Test
    void shouldFailWithIllegalStructSizeWhenLargeStructIsGiven() {
        var invalidSize = this.getStructSize() + 1;

        Assertions.assertThatExceptionOfType(IllegalStructSizeException.class)
                .isThrownBy(() -> this.getReader()
                        .read(null, PackstreamBuf.allocUnpooled(), new StructHeader(invalidSize, (short) 0x42)))
                .withMessage("Illegal struct size: Expected struct to be " + this.getStructSize() + " fields but got "
                        + invalidSize)
                .withNoCause();
    }

    @Test
    void shouldFailWithIllegalStructArgumentWhenLargeCodeIsGiven() {
        var coords = this.getCoordinates();
        var buf = PackstreamBuf.allocUnpooled().writeInt(Integer.MAX_VALUE + 1L);

        for (var coord : coords) {
            buf.writeFloat(coord);
        }

        assertThatThrownBy(() -> this.getReader().read(null, buf, new StructHeader(this.getStructSize(), (short) 0x42)))
                .isInstanceOf(IllegalStructArgumentException.class)
                .hasMessage("Illegal value for field \"crs\": crs code exceeds valid bounds")
                .hasNoCause()
                .satisfies(ex -> assertThat(((IllegalStructArgumentException) ex).getFieldName())
                        .isEqualTo("crs"));
    }

    @Test
    void shouldFailWithIllegalStructArgumentWhenNegativeCodeIsGiven() {
        var coords = this.getCoordinates();
        var buf = PackstreamBuf.allocUnpooled().writeInt(Integer.MIN_VALUE - 1L);

        for (var coord : coords) {
            buf.writeFloat(coord);
        }

        assertThatThrownBy(() -> this.getReader().read(null, buf, new StructHeader(this.getStructSize(), (short) 0x42)))
                .isInstanceOf(IllegalStructArgumentException.class)
                .hasMessage("Illegal value for field \"crs\": crs code exceeds valid bounds")
                .hasNoCause()
                .satisfies(ex -> assertThat(((IllegalStructArgumentException) ex).getFieldName())
                        .isEqualTo("crs"));
    }

    @Test
    void shouldFailWithIllegalStructArgumentWhenLargeNegativeCodeIsGiven() {
        var coords = this.getCoordinates();
        var buf = PackstreamBuf.allocUnpooled().writeInt(Integer.MAX_VALUE + 1L);

        for (var coord : coords) {
            buf.writeFloat(coord);
        }

        assertThatThrownBy(() -> this.getReader().read(null, buf, new StructHeader(this.getStructSize(), (short) 0x42)))
                .isInstanceOf(IllegalStructArgumentException.class)
                .hasMessage("Illegal value for field \"crs\": crs code exceeds valid bounds")
                .hasNoCause()
                .satisfies(ex -> assertThat(((IllegalStructArgumentException) ex).getFieldName())
                        .isEqualTo("crs"));
    }

    @Test
    void shouldFailWithIllegalStructArgumentWhenInvalidCoordinateReferenceSystemIsGiven() {
        var coords = this.getCoordinates();
        var buf = PackstreamBuf.allocUnpooled().writeInt(42);

        for (var coord : coords) {
            buf.writeFloat(coord);
        }

        assertThatThrownBy(() -> this.getReader().read(null, buf, new StructHeader(this.getStructSize(), (short) 0x42)))
                .isInstanceOf(IllegalStructArgumentException.class)
                .hasMessage("Illegal value for field \"crs\": Illegal coordinate reference system: \"42\"")
                .hasCauseInstanceOf(InvalidArgumentException.class)
                .satisfies(ex -> assertThat(((IllegalStructArgumentException) ex).getFieldName())
                        .isEqualTo("crs"));
    }

    @Test
    void shouldFailWithIllegalStructArgumentWhenInvalidNumberOfCoordinatesIsGiven() {
        var coords = this.getCoordinates();
        var crs = CoordinateReferenceSystem.CARTESIAN_3D;
        if (coords.length == 3) {
            crs = CoordinateReferenceSystem.CARTESIAN;
        }

        var buf = PackstreamBuf.allocUnpooled().writeInt(crs.getCode());

        for (var coord : coords) {
            buf.writeFloat(coord);
        }

        var coordMsg = "x=21.0, y=42.0";
        if (coords.length == 3) {
            coordMsg += ", z=84.0";
        }

        assertThatThrownBy(() -> this.getReader().read(null, buf, new StructHeader(this.getStructSize(), (short) 0x42)))
                .isInstanceOf(IllegalStructArgumentException.class)
                .hasMessage("Illegal value for field \"coords\": Illegal CRS/coords combination (crs=" + crs.getName()
                        + ", " + coordMsg + ")")
                .hasCauseInstanceOf(InvalidSpatialArgumentException.class)
                .satisfies(ex -> assertThat(((IllegalStructArgumentException) ex).getFieldName())
                        .isEqualTo("coords"));
    }
}
