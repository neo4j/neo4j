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
package org.neo4j.packstream.io;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.jupiter.api.DynamicTest.dynamicTest;

import java.time.temporal.ChronoField;
import java.time.temporal.ChronoUnit;
import java.util.Arrays;
import java.util.stream.Stream;
import org.junit.jupiter.api.DynamicTest;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestFactory;
import org.neo4j.exceptions.InvalidArgumentException;
import org.neo4j.packstream.error.reader.LimitExceededException;
import org.neo4j.packstream.error.reader.PackstreamReaderException;
import org.neo4j.packstream.error.reader.UnexpectedStructException;
import org.neo4j.packstream.error.reader.UnexpectedTypeException;
import org.neo4j.packstream.error.struct.IllegalStructArgumentException;
import org.neo4j.packstream.error.struct.IllegalStructSizeException;
import org.neo4j.packstream.struct.StructHeader;
import org.neo4j.values.storable.CoordinateReferenceSystem;
import org.neo4j.values.storable.StringValue;

public class NativeStructReadTest {

    @TestFactory
    Stream<DynamicTest> shouldReadPoint() {
        return Stream.of(new double[] {21, 42}, new double[] {21, 42, 84})
                .map(coords -> dynamicTest(Arrays.toString(coords), () -> {
                    var buf = PackstreamBuf.allocUnpooled();

                    var crs = CoordinateReferenceSystem.CARTESIAN;
                    var type = NativeStructType.POINT_2D;
                    if (coords.length == 3) {
                        crs = CoordinateReferenceSystem.CARTESIAN_3D;
                        type = NativeStructType.POINT_3D;
                    }

                    buf.writeStructHeader(new StructHeader(3, type.getTag())).writeInt(crs.getCode());

                    for (var coord : coords) {
                        buf.writeFloat(coord);
                    }

                    var value = NativeStruct.readPoint(buf);

                    assertThat(value.getCoordinateReferenceSystem()).isEqualTo(crs);
                    assertThat(value.coordinate()).isEqualTo(coords);
                }));
    }

    @TestFactory
    Stream<DynamicTest> readPointShouldFailWithIllegalStructArgumentWhenLargeCodeIsGiven() {
        return Stream.of(new double[] {21.0, 42.0}, new double[] {21.0, 42.0, 84.0})
                .map(coords -> dynamicTest(Arrays.toString(coords), () -> {
                    var type = NativeStructType.POINT_2D;
                    if (coords.length == 3) {
                        type = NativeStructType.POINT_3D;
                    }

                    var buf = PackstreamBuf.allocUnpooled()
                            .writeStructHeader(new StructHeader(coords.length + 1, type.getTag()))
                            .writeInt(Integer.MAX_VALUE + 1L);

                    for (var coord : coords) {
                        buf.writeFloat(coord);
                    }

                    assertThatThrownBy(() -> NativeStruct.readPoint(buf))
                            .isInstanceOf(IllegalStructArgumentException.class)
                            .hasMessage("Illegal value for field \"crs\": crs code exceeds valid bounds")
                            .hasNoCause()
                            .satisfies(ex -> assertThat(((IllegalStructArgumentException) ex).getFieldName())
                                    .isEqualTo("crs"));
                }));
    }

    @TestFactory
    Stream<DynamicTest> readPointShouldFailWithIllegalStructArgumentWhenLargeNegativeCodeIsGiven() {
        return Stream.of(new double[] {21.0, 42.0}, new double[] {21.0, 42.0, 84.0})
                .map(coords -> dynamicTest(Arrays.toString(coords), () -> {
                    var type = NativeStructType.POINT_2D;
                    if (coords.length == 3) {
                        type = NativeStructType.POINT_3D;
                    }

                    var buf = PackstreamBuf.allocUnpooled()
                            .writeStructHeader(new StructHeader(coords.length + 1, type.getTag()))
                            .writeInt(Integer.MAX_VALUE + 1L);

                    for (var coord : coords) {
                        buf.writeFloat(coord);
                    }

                    assertThatThrownBy(() -> NativeStruct.readPoint(buf))
                            .isInstanceOf(IllegalStructArgumentException.class)
                            .hasMessage("Illegal value for field \"crs\": crs code exceeds valid bounds")
                            .hasNoCause()
                            .satisfies(ex -> assertThat(((IllegalStructArgumentException) ex).getFieldName())
                                    .isEqualTo("crs"));
                }));
    }

    @TestFactory
    Stream<DynamicTest> readPointShouldFailWhenInvalidCoordinateReferenceSystemIsGiven() {
        return Stream.of(new double[] {21.0, 42.0}, new double[] {21.0, 42.0, 84.0})
                .map(coords -> dynamicTest(Arrays.toString(coords), () -> {
                    var type = NativeStructType.POINT_2D;
                    if (coords.length == 3) {
                        type = NativeStructType.POINT_3D;
                    }

                    var buf = PackstreamBuf.allocUnpooled()
                            .writeStructHeader(new StructHeader(coords.length + 1, type.getTag()))
                            .writeInt(-1);

                    for (var coord : coords) {
                        buf.writeFloat(coord);
                    }

                    assertThatThrownBy(() -> NativeStruct.readPoint(buf))
                            .isInstanceOf(IllegalStructArgumentException.class)
                            .hasMessage("Illegal value for field \"crs\": Illegal coordinate reference system: \"-1\"")
                            .hasCauseInstanceOf(InvalidArgumentException.class)
                            .satisfies(ex -> assertThat(((IllegalStructArgumentException) ex).getFieldName())
                                    .isEqualTo("crs"));
                }));
    }

    @TestFactory
    Stream<DynamicTest> readPointShouldFailWhenInvalidNumberOfCoordinatesIsGiven() {
        return Stream.of(new double[] {21.0, 42.0}, new double[] {21.0, 42.0, 84.0})
                .map(coords -> dynamicTest(Arrays.toString(coords), () -> {
                    var type = NativeStructType.POINT_2D;
                    var crs = CoordinateReferenceSystem.CARTESIAN_3D;
                    if (coords.length == 3) {
                        type = NativeStructType.POINT_3D;
                        crs = CoordinateReferenceSystem.CARTESIAN;
                    }

                    var buf = PackstreamBuf.allocUnpooled()
                            .writeStructHeader(new StructHeader(coords.length + 1, type.getTag()))
                            .writeInt(crs.getCode());

                    for (var coord : coords) {
                        buf.writeFloat(coord);
                    }

                    var coordMsg = "x=21.0, y=42.0";
                    if (coords.length == 3) {
                        coordMsg += ", z=84.0";
                    }

                    assertThatThrownBy(() -> NativeStruct.readPoint(buf))
                            .isInstanceOf(IllegalStructArgumentException.class)
                            .hasMessage("Illegal value for field \"coords\": Illegal CRS/coords combination (crs="
                                    + crs.getName() + ", " + coordMsg + ")")
                            .hasCauseInstanceOf(IllegalArgumentException.class)
                            .satisfies(ex -> assertThat(((IllegalStructArgumentException) ex).getFieldName())
                                    .isEqualTo("coords"));
                }));
    }

    @Test
    void shouldReadPoint2d() throws UnexpectedTypeException, IllegalStructArgumentException {
        var buf = PackstreamBuf.allocUnpooled()
                .writeInt(CoordinateReferenceSystem.WGS_84.getCode())
                .writeFloat(21.25)
                .writeFloat(42.5);

        var point = NativeStruct.readPoint2d(buf);

        assertThat(point.getCoordinateReferenceSystem()).isEqualTo(CoordinateReferenceSystem.WGS_84);
        assertThat(point.coordinate()).isEqualTo(new double[] {21.25, 42.5});
    }

    @Test
    void readPoint2dShouldFailWhenInvalidCoordinateReferenceSystemIsGiven() {
        var buf = PackstreamBuf.allocUnpooled().writeInt(-1).writeFloat(21.25).writeFloat(42.5);

        assertThatThrownBy(() -> NativeStruct.readPoint2d(buf))
                .isInstanceOf(IllegalStructArgumentException.class)
                .hasMessage("Illegal value for field \"crs\": Illegal coordinate reference system: \"-1\"")
                .hasCauseInstanceOf(InvalidArgumentException.class)
                .satisfies(ex -> assertThat(((IllegalStructArgumentException) ex).getFieldName())
                        .isEqualTo("crs"));
    }

    @Test
    void readPoint2dShouldFailWhenInvalidNumberOfCoordinatesIsGiven() {
        var buf = PackstreamBuf.allocUnpooled()
                .writeInt(CoordinateReferenceSystem.CARTESIAN_3D.getCode())
                .writeFloat(21.25)
                .writeFloat(42.5);

        assertThatThrownBy(() -> NativeStruct.readPoint2d(buf))
                .isInstanceOf(IllegalStructArgumentException.class)
                .hasMessage(
                        "Illegal value for field \"coords\": Illegal CRS/coords combination (crs=cartesian-3d, x=21.25, y=42.5)")
                .hasCauseInstanceOf(IllegalArgumentException.class)
                .satisfies(ex -> assertThat(((IllegalStructArgumentException) ex).getFieldName())
                        .isEqualTo("coords"));
    }

    @Test
    void shouldReadPoint3d() throws UnexpectedTypeException, IllegalStructArgumentException {
        var buf = PackstreamBuf.allocUnpooled()
                .writeInt(CoordinateReferenceSystem.WGS_84_3D.getCode())
                .writeFloat(21.125)
                .writeFloat(42.25)
                .writeFloat(84.5);

        var point = NativeStruct.readPoint3d(buf);

        assertThat(point.getCoordinateReferenceSystem()).isEqualTo(CoordinateReferenceSystem.WGS_84_3D);
        assertThat(point.coordinate()).isEqualTo(new double[] {21.125, 42.25, 84.5});
    }

    @Test
    void readPoint3dShouldFailWhenInvalidCoordinateReferenceSystemIsGiven() {
        var buf = PackstreamBuf.allocUnpooled()
                .writeInt(-1)
                .writeFloat(21.125)
                .writeFloat(42.25)
                .writeFloat(84.5);

        assertThatThrownBy(() -> NativeStruct.readPoint3d(buf))
                .isInstanceOf(IllegalStructArgumentException.class)
                .hasMessage("Illegal value for field \"crs\": Illegal coordinate reference system: \"-1\"")
                .hasCauseInstanceOf(InvalidArgumentException.class)
                .satisfies(ex -> assertThat(((IllegalStructArgumentException) ex).getFieldName())
                        .isEqualTo("crs"));
    }

    @Test
    void readPoint3dShouldFailWhenInvalidNumberOfCoordinatesIsGiven() {
        var buf = PackstreamBuf.allocUnpooled()
                .writeInt(CoordinateReferenceSystem.CARTESIAN.getCode())
                .writeFloat(21.125)
                .writeFloat(42.25)
                .writeFloat(84.5);

        assertThatThrownBy(() -> NativeStruct.readPoint3d(buf))
                .isInstanceOf(IllegalStructArgumentException.class)
                .hasMessage(
                        "Illegal value for field \"coords\": Illegal CRS/coords combination (crs=cartesian, x=21.125, y=42.25, z=84.5)")
                .hasCauseInstanceOf(IllegalArgumentException.class)
                .satisfies(ex -> assertThat(((IllegalStructArgumentException) ex).getFieldName())
                        .isEqualTo("coords"));
    }

    @Test
    void shouldReadDuration()
            throws UnexpectedStructException, LimitExceededException, UnexpectedTypeException,
                    IllegalStructSizeException {
        var buf = PackstreamBuf.allocUnpooled()
                .writeInt(2)
                .writeInt(9)
                .writeInt(3602)
                .writeInt(329);

        var duration = NativeStruct.readDuration(buf);

        assertThat(duration.get(ChronoUnit.MONTHS)).isEqualTo(2);
        assertThat(duration.get(ChronoUnit.DAYS)).isEqualTo(9);
        assertThat(duration.get(ChronoUnit.SECONDS)).isEqualTo(3602);
        assertThat(duration.get(ChronoUnit.NANOS)).isEqualTo(329);
    }

    @Test
    void shouldReadDate()
            throws UnexpectedStructException, LimitExceededException, UnexpectedTypeException,
                    IllegalStructSizeException {
        var buf = PackstreamBuf.allocUnpooled().writeInt(42);

        var date = NativeStruct.readDate(buf);

        assertThat(date.get(ChronoField.YEAR)).isEqualTo(1970);
        assertThat(date.get(ChronoField.MONTH_OF_YEAR)).isEqualTo(2);
        assertThat(date.get(ChronoField.DAY_OF_MONTH)).isEqualTo(12);
    }

    @Test
    void shouldReadLocalTime()
            throws UnexpectedStructException, LimitExceededException, UnexpectedTypeException,
                    IllegalStructSizeException {
        var buf = PackstreamBuf.allocUnpooled().writeInt(5_470_000_000_000L);

        var time = NativeStruct.readLocalTime(buf);

        assertThat(time.get(ChronoField.HOUR_OF_DAY)).isEqualTo(1);
        assertThat(time.get(ChronoField.MINUTE_OF_HOUR)).isEqualTo(31);
        assertThat(time.get(ChronoField.SECOND_OF_MINUTE)).isEqualTo(10);
    }

    @Test
    void shouldReadDateTime() throws UnexpectedTypeException, IllegalStructArgumentException {
        // Important: This is _NOT_ a UNIX timestamp - This value refers to epoch as observed _WITHIN_ the current
        // offset.
        var buf = PackstreamBuf.allocUnpooled().writeInt(14218662).writeInt(436).writeInt(7200);

        var dateTime = NativeStruct.readDateTime(buf);

        assertThat(dateTime.get(ChronoField.YEAR)).isEqualTo(1970);
        assertThat(dateTime.get(ChronoField.MONTH_OF_YEAR)).isEqualTo(6);
        assertThat(dateTime.get(ChronoField.DAY_OF_MONTH)).isEqualTo(14);

        assertThat(dateTime.get(ChronoField.HOUR_OF_DAY)).isEqualTo(13);
        assertThat(dateTime.get(ChronoField.MINUTE_OF_HOUR)).isEqualTo(37);
        assertThat(dateTime.get(ChronoField.SECOND_OF_MINUTE)).isEqualTo(42);
        assertThat(dateTime.get(ChronoField.NANO_OF_SECOND)).isEqualTo(436);
    }

    @Test
    void readDateTimeShouldFailWithIllegalStructArgumentWhenLargeNanosIsGiven() {
        var buf = PackstreamBuf.allocUnpooled()
                .writeInt(14218662)
                .writeInt(Integer.MAX_VALUE + 1L)
                .writeInt(7200);

        assertThatThrownBy(() -> NativeStruct.readDateTime(buf))
                .isInstanceOf(IllegalStructArgumentException.class)
                .hasMessage("Illegal value for field \"nanoseconds\": Value is out of bounds")
                .hasNoCause()
                .satisfies(ex -> assertThat(((IllegalStructArgumentException) ex).getFieldName())
                        .isEqualTo("nanoseconds"));
    }

    @Test
    void readDateTimeShouldFailWithIllegalStructArgumentWhenLargeNegativeNanosIsGiven() {
        var buf = PackstreamBuf.allocUnpooled()
                .writeInt(14218662)
                .writeInt(Integer.MIN_VALUE - 1L)
                .writeInt(7200);

        assertThatThrownBy(() -> NativeStruct.readDateTime(buf))
                .isInstanceOf(IllegalStructArgumentException.class)
                .hasMessage("Illegal value for field \"nanoseconds\": Value is out of bounds")
                .hasNoCause()
                .satisfies(ex -> assertThat(((IllegalStructArgumentException) ex).getFieldName())
                        .isEqualTo("nanoseconds"));
    }

    @Test
    void readDateTimeShouldFailWithIllegalStructArgumentWhenLargeTimezoneOffsetIsGiven() {
        var buf = PackstreamBuf.allocUnpooled().writeInt(14218662).writeInt(374).writeInt(Integer.MAX_VALUE + 1L);

        assertThatThrownBy(() -> NativeStruct.readDateTime(buf))
                .isInstanceOf(IllegalStructArgumentException.class)
                .hasMessage("Illegal value for field \"tz_offset_seconds\": Value is out of bounds")
                .hasNoCause()
                .satisfies(ex -> assertThat(((IllegalStructArgumentException) ex).getFieldName())
                        .isEqualTo("tz_offset_seconds"));
    }

    @Test
    void readDateTimeShouldFailWithIllegalStructArgumentWhenLargeNegativeTimezoneOffsetIsGiven() {
        var buf = PackstreamBuf.allocUnpooled().writeInt(14218662).writeInt(374).writeInt(Integer.MIN_VALUE - 1L);

        assertThatThrownBy(() -> NativeStruct.readDateTime(buf))
                .isInstanceOf(IllegalStructArgumentException.class)
                .hasMessage("Illegal value for field \"tz_offset_seconds\": Value is out of bounds")
                .hasNoCause()
                .satisfies(ex -> assertThat(((IllegalStructArgumentException) ex).getFieldName())
                        .isEqualTo("tz_offset_seconds"));
    }

    @Test
    void shouldReadTime()
            throws UnexpectedStructException, LimitExceededException, UnexpectedTypeException,
                    IllegalStructSizeException, IllegalStructArgumentException {
        var buf = PackstreamBuf.allocUnpooled().writeInt(5_470_000_000_000L).writeInt(7200);

        var time = NativeStruct.readTime(buf);

        assertThat(time.get(ChronoField.HOUR_OF_DAY)).isEqualTo(1);
        assertThat(time.get(ChronoField.MINUTE_OF_HOUR)).isEqualTo(31);
        assertThat(time.get(ChronoField.SECOND_OF_MINUTE)).isEqualTo(10);
    }

    @Test
    void readTimeShouldFailWithIllegalStructArgumentWhenLargeTimeZoneOffsetIsGiven() {
        var buf = PackstreamBuf.allocUnpooled().writeInt(5_470_000_000_000L).writeInt(Integer.MAX_VALUE + 1L);

        assertThatThrownBy(() -> NativeStruct.readTime(buf))
                .isInstanceOf(IllegalStructArgumentException.class)
                .hasMessage("Illegal value for field \"tz_offset_seconds\": Value is out of bounds")
                .hasNoCause();
    }

    @Test
    void readTimeShouldFailWithIllegalStructArgumentWhenLargeNegativeTimeZoneOffsetIsGiven() {
        var buf = PackstreamBuf.allocUnpooled().writeInt(5_470_000_000_000L).writeInt(Integer.MIN_VALUE - 1L);

        assertThatThrownBy(() -> NativeStruct.readTime(buf))
                .isInstanceOf(IllegalStructArgumentException.class)
                .hasMessage("Illegal value for field \"tz_offset_seconds\": Value is out of bounds")
                .hasNoCause();
    }

    @Test
    void shouldReadLocalDateTime()
            throws UnexpectedStructException, LimitExceededException, UnexpectedTypeException,
                    IllegalStructSizeException {
        var buf = PackstreamBuf.allocUnpooled().writeInt(803137062).writeInt(1337);

        var localDateTime = NativeStruct.readLocalDateTime(buf);

        assertThat(localDateTime.get(ChronoField.YEAR)).isEqualTo(1995);
        assertThat(localDateTime.get(ChronoField.MONTH_OF_YEAR)).isEqualTo(6);
        assertThat(localDateTime.get(ChronoField.DAY_OF_MONTH)).isEqualTo(14);

        assertThat(localDateTime.get(ChronoField.HOUR_OF_DAY)).isEqualTo(13);
        assertThat(localDateTime.get(ChronoField.MINUTE_OF_HOUR)).isEqualTo(37);
        assertThat(localDateTime.get(ChronoField.SECOND_OF_MINUTE)).isEqualTo(42);
        assertThat(localDateTime.get(ChronoField.NANO_OF_SECOND)).isEqualTo(1337);
    }

    @Test
    void shouldReadDateTimeZoneId() throws PackstreamReaderException {
        // Important: This is _NOT_ a UNIX timestamp - This value refers to epoch as observed _WITHIN_ the current
        // offset.
        var buf =
                PackstreamBuf.allocUnpooled().writeInt(803137062).writeInt(1337).writeString("Europe/Berlin");

        var dateTime = NativeStruct.readDateTimeZoneId(buf);

        assertThat(dateTime.get(ChronoField.YEAR)).isEqualTo(1995);
        assertThat(dateTime.get(ChronoField.MONTH_OF_YEAR)).isEqualTo(6);
        assertThat(dateTime.get(ChronoField.DAY_OF_MONTH)).isEqualTo(14);

        assertThat(dateTime.get(ChronoField.HOUR_OF_DAY)).isEqualTo(13);
        assertThat(dateTime.get(ChronoField.MINUTE_OF_HOUR)).isEqualTo(37);
        assertThat(dateTime.get(ChronoField.SECOND_OF_MINUTE)).isEqualTo(42);
        assertThat(dateTime.get(ChronoField.NANO_OF_SECOND)).isEqualTo(1337);

        assertThat(((StringValue) dateTime.get("timezone")).stringValue()).isEqualTo("Europe/Berlin");
    }
}
