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
import static org.neo4j.bolt.testing.util.ErrorUtil.useNewMessage;

import java.util.stream.LongStream;
import java.util.stream.Stream;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.DynamicTest;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestFactory;
import org.neo4j.exceptions.InvalidArgumentException;
import org.neo4j.exceptions.InvalidSpatialArgumentException;
import org.neo4j.gqlstatus.ErrorGqlStatusObjectAssertions;
import org.neo4j.gqlstatus.GqlStatusInfoCodes;
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
            buf.writeFloat64(coord);
        }

        var value = this.getReader().read(null, buf, new StructHeader(this.getStructSize(), (short) 0x42));

        Assertions.assertThat(value.getCoordinateReferenceSystem()).isEqualTo(crs);
        Assertions.assertThat(value.coordinate()).isEqualTo(coords);
    }

    @Test
    void shouldFailWithIllegalStructSizeWhenEmptyStructIsGiven() {
        ErrorGqlStatusObjectAssertions.assertThatThrownBy(() ->
                        this.getReader().read(null, PackstreamBuf.allocUnpooled(), new StructHeader(0, (short) 0x42)))
                .isInstanceOf(IllegalStructSizeException.class)
                .hasMessage(useNewMessage(
                                "08N11: The request is invalid and could not be processed by the server. See cause for further details.")
                        .whenLegacyFallbackTo("Illegal struct size: Expected struct to be 3 fields but got 0"))
                .hasNoCause()
                .hasGqlStatus(GqlStatusInfoCodes.STATUS_08N11)
                .hasStatusDescription(
                        "error: connection exception - request error. The request is invalid and could not be processed by the server. See cause for further details.")
                .gqlCause()
                .hasGqlStatus(GqlStatusInfoCodes.STATUS_22N57)
                .hasStatusDescription(
                        "error: data exception - invalid protocol type. Protocol type is invalid. Invalid number of struct components (received 0 but expected 3).");
    }

    @TestFactory
    Stream<DynamicTest> shouldFailWithIllegalStructSizeWhenSmallStructIsGiven() {
        return LongStream.range(1, this.getStructSize())
                .mapToObj(size -> DynamicTest.dynamicTest(
                        size + " elements",
                        () -> ErrorGqlStatusObjectAssertions.assertThatThrownBy(() -> this.getReader()
                                        .read(null, PackstreamBuf.allocUnpooled(), new StructHeader(size, (short)
                                                0x42)))
                                .isInstanceOf(IllegalStructSizeException.class)
                                .hasMessage(useNewMessage(
                                                "08N11: The request is invalid and could not be processed by the server. See cause for further details.")
                                        .whenLegacyFallbackTo("Illegal struct size: Expected struct to be "
                                                + this.getStructSize() + " fields but got " + size))
                                .hasNoCause()
                                .hasGqlStatus(GqlStatusInfoCodes.STATUS_08N11)
                                .hasStatusDescription(
                                        "error: connection exception - request error. The request is invalid and could not be processed by the server. See cause for further details.")
                                .gqlCause()
                                .hasGqlStatus(GqlStatusInfoCodes.STATUS_22N57)
                                .hasStatusDescription(String.format(
                                        "error: data exception - invalid protocol type. Protocol type is invalid. Invalid number of struct components (received %s but expected %s).",
                                        size, this.getStructSize()))));
    }

    @Test
    void shouldFailWithIllegalStructSizeWhenLargeStructIsGiven() {
        var invalidSize = this.getStructSize() + 1;

        ErrorGqlStatusObjectAssertions.assertThatThrownBy(() -> this.getReader()
                        .read(null, PackstreamBuf.allocUnpooled(), new StructHeader(invalidSize, (short) 0x42)))
                .isInstanceOf(IllegalStructSizeException.class)
                .hasMessage(useNewMessage(
                                "08N11: The request is invalid and could not be processed by the server. See cause for further details.")
                        .whenLegacyFallbackTo("Illegal struct size: Expected struct to be " + this.getStructSize()
                                + " fields but got " + invalidSize))
                .hasNoCause()
                .hasGqlStatus(GqlStatusInfoCodes.STATUS_08N11)
                .hasStatusDescription(
                        "error: connection exception - request error. The request is invalid and could not be processed by the server. See cause for further details.")
                .gqlCause()
                .hasGqlStatus(GqlStatusInfoCodes.STATUS_22N57)
                .hasStatusDescription(String.format(
                        "error: data exception - invalid protocol type. Protocol type is invalid. Invalid number of struct components (received %s but expected %s).",
                        invalidSize, this.getStructSize()));
    }

    @Test
    void shouldFailWithIllegalStructArgumentWhenLargeCodeIsGiven() {
        var coords = this.getCoordinates();
        var buf = PackstreamBuf.allocUnpooled().writeInt(Integer.MAX_VALUE + 1L);

        for (var coord : coords) {
            buf.writeFloat64(coord);
        }
        assertFailsWithCrsOutOfBounds(buf);
    }

    @Test
    void shouldFailWithIllegalStructArgumentWhenNegativeCodeIsGiven() {
        var coords = this.getCoordinates();
        var buf = PackstreamBuf.allocUnpooled().writeInt(Integer.MIN_VALUE - 1L);

        for (var coord : coords) {
            buf.writeFloat64(coord);
        }
        assertFailsWithCrsOutOfBounds(buf);
    }

    @Test
    void shouldFailWithIllegalStructArgumentWhenLargeNegativeCodeIsGiven() {
        var coords = this.getCoordinates();
        var buf = PackstreamBuf.allocUnpooled().writeInt(Integer.MAX_VALUE + 1L);

        for (var coord : coords) {
            buf.writeFloat64(coord);
        }
        assertFailsWithCrsOutOfBounds(buf);
    }

    @Test
    void shouldFailWithIllegalStructArgumentWhenInvalidCoordinateReferenceSystemIsGiven() {
        var coords = this.getCoordinates();
        var buf = PackstreamBuf.allocUnpooled().writeInt(42);

        for (var coord : coords) {
            buf.writeFloat64(coord);
        }

        var assertion = ErrorGqlStatusObjectAssertions.assertThatThrownBy(
                        () -> this.getReader().read(null, buf, new StructHeader(this.getStructSize(), (short) 0x42)))
                .isInstanceOf(IllegalStructArgumentException.class)
                .hasMessage(useNewMessage("08N06: General network protocol error.")
                        .whenLegacyFallbackTo(
                                "Illegal value for field \"crs\": Illegal coordinate reference system: \"42\""));

        assertion
                .hasGqlStatus(GqlStatusInfoCodes.STATUS_08N06)
                .hasStatusDescription("error: connection exception - protocol error. General network protocol error.")
                .gqlCause()
                .hasGqlStatus(GqlStatusInfoCodes.STATUS_22000)
                .hasStatusDescription("error: data exception")
                .gqlCause()
                .hasGqlStatus(GqlStatusInfoCodes.STATUS_22N21)
                .hasStatusDescription(
                        "error: data exception - unsupported coordinate reference system. Unsupported coordinate reference system (CRS): code=42.");

        assertion
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
            buf.writeFloat64(coord);
        }

        var coordMsg = "x=21.0, y=42.0";
        if (coords.length == 3) {
            coordMsg += ", z=84.0";
        }

        assertThatThrownBy(() -> this.getReader().read(null, buf, new StructHeader(this.getStructSize(), (short) 0x42)))
                .isInstanceOf(IllegalStructArgumentException.class)
                .hasMessage(useNewMessage("08N06: General network protocol error.")
                        .whenLegacyFallbackTo("Illegal value for field \"coords\": Illegal CRS/coords combination (crs="
                                + crs.getName() + ", " + coordMsg + ")"))
                .hasCauseInstanceOf(InvalidSpatialArgumentException.class)
                .satisfies(ex -> assertThat(((IllegalStructArgumentException) ex).getFieldName())
                        .isEqualTo("coords"));
    }

    private void assertFailsWithCrsOutOfBounds(PackstreamBuf buf) {
        ErrorGqlStatusObjectAssertions.assertThatThrownBy(
                        () -> this.getReader().read(null, buf, new StructHeader(this.getStructSize(), (short) 0x42)))
                .isInstanceOf(IllegalStructArgumentException.class)
                .hasMessage(useNewMessage("08N06: General network protocol error.")
                        .whenLegacyFallbackTo("Illegal value for field \"crs\": crs code exceeds valid bounds"))
                .hasNoCause()
                .satisfies(ex -> assertThat(((IllegalStructArgumentException) ex).getFieldName())
                        .isEqualTo("crs"))
                .hasGqlStatus(GqlStatusInfoCodes.STATUS_08N06)
                .hasStatusDescription("error: connection exception - protocol error. General network protocol error.")
                .gqlCause()
                .hasGqlStatus(GqlStatusInfoCodes.STATUS_22N29)
                .hasStatusDescription(
                        "error: data exception - unknown coordinate reference system. Unknown coordinate reference system (CRS).")
                .gqlCause()
                .hasGqlStatus(GqlStatusInfoCodes.STATUS_22003)
                .hasStatusDescription(
                        "error: data exception - numeric value out of range. The numeric value crs is outside the required range.");
    }
}
