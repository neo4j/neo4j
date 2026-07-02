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
package org.neo4j.bolt.protocol.io.writer;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.DynamicTest.dynamicTest;
import static org.neo4j.internal.helpers.TimeUtil.zoneOffsetOfTotalSeconds;

import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.OffsetDateTime;
import java.time.OffsetTime;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.util.Arrays;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.commons.lang3.ArrayUtils;
import org.junit.jupiter.api.DynamicTest;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestFactory;
import org.mockito.Mockito;
import org.neo4j.bolt.negotiation.version.ProtocolVersion;
import org.neo4j.bolt.protocol.io.StructType;
import org.neo4j.bolt.protocol.io.pipeline.WriterContext;
import org.neo4j.packstream.error.reader.LimitExceededException;
import org.neo4j.packstream.error.reader.PackstreamReaderException;
import org.neo4j.packstream.error.reader.UnexpectedTypeException;
import org.neo4j.packstream.io.PackstreamBuf;
import org.neo4j.packstream.io.TypeMarker;
import org.neo4j.values.storable.CoordinateReferenceSystem;

class DefaultVersionedValueWriterTest {

    @TestFactory
    Stream<DynamicTest> shouldWritePoint() {
        return Stream.of(new double[] {21, 42}, new double[] {21, 42, 84})
                .map(coords -> dynamicTest(Arrays.toString(coords), () -> {
                    var buf = PackstreamBuf.allocUnpooled();
                    var ctx = Mockito.mock(WriterContext.class);

                    Mockito.doReturn(buf).when(ctx).buffer();

                    DefaultVersionedValueWriter.getInstance()
                            .writePoint(ctx, CoordinateReferenceSystem.CARTESIAN, coords);

                    var header = buf.readStructHeader();
                    var code = buf.readInt();
                    var x = buf.readFloat64();
                    var y = buf.readFloat64();
                    double z = 0;
                    if (coords.length == 3) {
                        z = buf.readFloat64();
                    }

                    assertThat(header.length()).isEqualTo(coords.length + 1);
                    assertThat(header.tag())
                            .isEqualTo(
                                    coords.length == 3 ? StructType.POINT_3D.getTag() : StructType.POINT_2D.getTag());

                    assertThat(code).isEqualTo(CoordinateReferenceSystem.CARTESIAN.getCode());
                    assertThat(x).isEqualTo(coords[0]);
                    assertThat(y).isEqualTo(coords[1]);
                    if (coords.length == 3) {
                        assertThat(z).isEqualTo(coords[2]);
                    }

                    assertThat(buf.raw().isReadable()).isFalse();
                }));
    }

    @Test
    void shouldWritePoint2d() throws LimitExceededException, UnexpectedTypeException {
        var buf = PackstreamBuf.allocUnpooled();
        var ctx = Mockito.mock(WriterContext.class);

        Mockito.doReturn(buf).when(ctx).buffer();

        DefaultVersionedValueWriter.getInstance()
                .writePoint(ctx, CoordinateReferenceSystem.WGS_84, new double[] {42.5, 85});

        var header = buf.readStructHeader();
        var code = buf.readInt();
        var x = buf.readFloat64();
        var y = buf.readFloat64();

        assertThat(header.length()).isEqualTo(3);
        assertThat(header.tag()).isEqualTo(StructType.POINT_2D.getTag());

        assertThat(code).isEqualTo(CoordinateReferenceSystem.WGS_84.getCode());
        assertThat(x).isEqualTo(42.5);
        assertThat(y).isEqualTo(85);

        assertThat(buf.raw().isReadable()).isFalse();
    }

    @Test
    void shouldWritePoint3d() throws LimitExceededException, UnexpectedTypeException {
        var buf = PackstreamBuf.allocUnpooled();
        var ctx = Mockito.mock(WriterContext.class);

        Mockito.doReturn(buf).when(ctx).buffer();

        DefaultVersionedValueWriter.getInstance()
                .writePoint(ctx, CoordinateReferenceSystem.WGS_84_3D, new double[] {42.25, 84.5, 169});

        var header = buf.readStructHeader();
        var code = buf.readInt();
        var x = buf.readFloat64();
        var y = buf.readFloat64();
        var z = buf.readFloat64();

        assertThat(header.length()).isEqualTo(4);
        assertThat(header.tag()).isEqualTo(StructType.POINT_3D.getTag());

        assertThat(code).isEqualTo(CoordinateReferenceSystem.WGS_84_3D.getCode());
        assertThat(x).isEqualTo(42.25);
        assertThat(y).isEqualTo(84.5);
        assertThat(z).isEqualTo(169);

        assertThat(buf.raw().isReadable()).isFalse();
    }

    @Test
    void shouldWriteDuration() throws LimitExceededException, UnexpectedTypeException {
        var buf = PackstreamBuf.allocUnpooled();
        var ctx = Mockito.mock(WriterContext.class);

        Mockito.doReturn(buf).when(ctx).buffer();

        DefaultVersionedValueWriter.getInstance().writeDuration(ctx, 3, 2, 1, 214284);

        var header = buf.readStructHeader();
        var months = buf.readInt();
        var days = buf.readInt();
        var seconds = buf.readInt();
        var nanos = buf.readInt();

        assertThat(header.length()).isEqualTo(4);
        assertThat(header.tag()).isEqualTo(StructType.DURATION.getTag());

        assertThat(months).isEqualTo(3);
        assertThat(days).isEqualTo(2);
        assertThat(seconds).isEqualTo(1);
        assertThat(nanos).isEqualTo(214284);

        assertThat(buf.raw().isReadable()).isFalse();
    }

    @TestFactory
    Stream<DynamicTest> shouldWriteDate() {
        return Stream.of(
                        LocalDate.of(1995, 6, 14),
                        LocalDate.of(1999, 7, 14),
                        LocalDate.of(2003, 5, 16),
                        LocalDate.of(2003, 10, 27),
                        LocalDate.of(2021, 12, 22),
                        LocalDate.of(2060, 3, 12))
                .map(date -> dynamicTest(date.toString(), () -> {
                    var buf = PackstreamBuf.allocUnpooled();
                    var ctx = Mockito.mock(WriterContext.class);

                    Mockito.doReturn(buf).when(ctx).buffer();

                    DefaultVersionedValueWriter.getInstance().writeDate(ctx, date);

                    var header = buf.readStructHeader();
                    var epochDay = buf.readInt();

                    assertThat(header.length()).isEqualTo(1);
                    assertThat(header.tag()).isEqualTo(StructType.DATE.getTag());

                    assertThat(epochDay).isEqualTo(date.toEpochDay());

                    assertThat(buf.raw().isReadable()).isFalse();
                }));
    }

    @TestFactory
    Stream<DynamicTest> shouldWriteLocalTime() {
        return Stream.of(LocalTime.of(0, 0, 0, 0), LocalTime.of(14, 37, 15, 4284), LocalTime.of(23, 59, 55, 998298))
                .map(time -> dynamicTest(time.toString(), () -> {
                    var buf = PackstreamBuf.allocUnpooled();
                    var ctx = Mockito.mock(WriterContext.class);

                    Mockito.doReturn(buf).when(ctx).buffer();

                    DefaultVersionedValueWriter.getInstance().writeLocalTime(ctx, time);

                    var header = buf.readStructHeader();
                    var nanoOfDay = buf.readInt();

                    assertThat(header.length()).isEqualTo(1);
                    assertThat(header.tag()).isEqualTo(StructType.LOCAL_TIME.getTag());

                    assertThat(nanoOfDay).isEqualTo(time.toNanoOfDay());

                    assertThat(buf.raw().isReadable()).isFalse();
                }));
    }

    @TestFactory
    Stream<DynamicTest> shouldWriteTime() {
        return Stream.of(
                        OffsetTime.of(0, 0, 0, 0, ZoneOffset.UTC),
                        OffsetTime.of(14, 37, 15, 4284, ZoneOffset.of("+02:00")),
                        OffsetTime.of(23, 59, 55, 998298, ZoneOffset.of("+05:15")))
                .map(time -> dynamicTest(time.toString(), () -> {
                    var buf = PackstreamBuf.allocUnpooled();
                    var ctx = Mockito.mock(WriterContext.class);

                    Mockito.doReturn(buf).when(ctx).buffer();

                    DefaultVersionedValueWriter.getInstance().writeTime(ctx, time);

                    var header = buf.readStructHeader();
                    var nanoOfDay = buf.readInt();
                    var offset = buf.readInt();

                    assertThat(header.length()).isEqualTo(2);
                    assertThat(header.tag()).isEqualTo(StructType.TIME.getTag());

                    assertThat(nanoOfDay).isEqualTo(time.toLocalTime().toNanoOfDay());
                    assertThat(offset).isEqualTo(time.getOffset().getTotalSeconds());

                    assertThat(buf.raw().isReadable()).isFalse();
                }));
    }

    @TestFactory
    Stream<DynamicTest> shouldWriteLocalDateTime() {
        return Stream.of(
                        LocalDateTime.of(1995, 6, 14, 13, 57, 15, 9494),
                        LocalDateTime.of(1999, 7, 14, 2, 20, 4, 66712),
                        LocalDateTime.of(2003, 5, 16, 17, 53, 53, 223),
                        LocalDateTime.of(2003, 10, 27, 22, 59, 57, 993999),
                        LocalDateTime.of(2021, 12, 22, 2, 19, 45, 3325),
                        LocalDateTime.of(2060, 3, 12, 1, 17, 3, 12))
                .map(dateTime -> dynamicTest(dateTime.toString(), () -> {
                    var buf = PackstreamBuf.allocUnpooled();
                    var ctx = Mockito.mock(WriterContext.class);

                    Mockito.doReturn(buf).when(ctx).buffer();

                    DefaultVersionedValueWriter.getInstance().writeLocalDateTime(ctx, dateTime);

                    var header = buf.readStructHeader();
                    var epochSecond = buf.readInt();
                    var nanoOfDay = buf.readInt();

                    assertThat(header.length()).isEqualTo(2);
                    assertThat(header.tag()).isEqualTo(StructType.LOCAL_DATE_TIME.getTag());

                    assertThat(epochSecond).isEqualTo(dateTime.toEpochSecond(ZoneOffset.UTC));
                    assertThat(nanoOfDay).isEqualTo(dateTime.getNano());

                    assertThat(buf.raw().isReadable()).isFalse();
                }));
    }

    @TestFactory
    Stream<DynamicTest> shouldWriteDateTime() {
        return Stream.of(
                        OffsetDateTime.of(1995, 6, 14, 13, 57, 15, 9494, zoneOffsetOfTotalSeconds(3600)),
                        OffsetDateTime.of(1999, 7, 14, 2, 20, 4, 66712, zoneOffsetOfTotalSeconds(300)),
                        OffsetDateTime.of(2003, 5, 16, 17, 53, 53, 223, zoneOffsetOfTotalSeconds(10000)),
                        OffsetDateTime.of(2003, 10, 27, 22, 59, 57, 993999, zoneOffsetOfTotalSeconds(90)),
                        OffsetDateTime.of(2021, 12, 22, 2, 19, 45, 3325, zoneOffsetOfTotalSeconds(32)),
                        OffsetDateTime.of(2060, 3, 12, 1, 17, 3, 12, zoneOffsetOfTotalSeconds(16)))
                .map(dateTime -> dynamicTest(dateTime.toString(), () -> {
                    var buf = PackstreamBuf.allocUnpooled();
                    var ctx = Mockito.mock(WriterContext.class);

                    Mockito.doReturn(buf).when(ctx).buffer();

                    DefaultVersionedValueWriter.getInstance().writeDateTime(ctx, dateTime);

                    var header = buf.readStructHeader();
                    var epochSeconds = buf.readInt();
                    var nanos = buf.readInt();
                    var offsetSeconds = buf.readInt();

                    assertThat(header.length()).isEqualTo(3);
                    assertThat(header.tag()).isEqualTo(StructType.DATE_TIME.getTag());

                    assertThat(epochSeconds).isEqualTo(dateTime.toEpochSecond());
                    assertThat(nanos).isEqualTo(dateTime.getNano());
                    assertThat(offsetSeconds).isEqualTo(dateTime.getOffset().getTotalSeconds());

                    assertThat(buf.raw().isReadable()).isFalse();
                }));
    }

    @TestFactory
    Stream<DynamicTest> shouldWriteDateTimeZoneId() {
        return Stream.of(
                        ZonedDateTime.of(1995, 6, 14, 13, 57, 15, 9494, ZoneId.of("Europe/Berlin")),
                        ZonedDateTime.of(1999, 7, 14, 2, 20, 4, 66712, ZoneId.of("Etc/GMT+2")),
                        ZonedDateTime.of(2003, 5, 16, 17, 53, 53, 223, ZoneId.of("US/Hawaii")),
                        ZonedDateTime.of(2003, 10, 27, 22, 59, 57, 993999, ZoneId.of("US/Pacific")),
                        ZonedDateTime.of(2021, 12, 22, 2, 19, 45, 3325, ZoneId.of("Asia/Tokyo")),
                        ZonedDateTime.of(2060, 3, 12, 1, 17, 3, 12, ZoneId.of("UTC")))
                .map(dateTime -> dynamicTest(dateTime.toString(), () -> {
                    var buf = PackstreamBuf.allocUnpooled();
                    var ctx = Mockito.mock(WriterContext.class);

                    Mockito.doReturn(buf).when(ctx).buffer();

                    DefaultVersionedValueWriter.getInstance().writeDateTime(ctx, dateTime);

                    var header = buf.readStructHeader();
                    var epochSecond = buf.readInt();
                    var nanoOfDay = buf.readInt();
                    var zoneId = buf.readString();

                    assertThat(header.length()).isEqualTo(3);
                    assertThat(header.tag()).isEqualTo(StructType.DATE_TIME_ZONE_ID.getTag());

                    assertThat(epochSecond).isEqualTo(dateTime.toEpochSecond());
                    assertThat(nanoOfDay).isEqualTo(dateTime.getNano());
                    assertThat(zoneId).isEqualTo(dateTime.getZone().getId());

                    assertThat(buf.raw().isReadable()).isFalse();
                }));
    }

    @TestFactory
    Stream<DynamicTest> shouldWriteInt8Vector() {
        return Stream.<VectorSpecification<Byte>>of(
                        new VectorSpecification<>(),
                        VectorSpecification.vector8(0, 21, 42),
                        VectorSpecification.vector8(0, 21, 42, 84, 73, 97, 62, 21, 33, 99, 128))
                .map(vector -> dynamicTest(vector.toString(), () -> {
                    var buf = PackstreamBuf.allocUnpooled();
                    var ctx = Mockito.mock(WriterContext.class);

                    Mockito.doReturn(buf).when(ctx).buffer();

                    DefaultVersionedValueWriter.getInstance()
                            .writeVector(ctx, ArrayUtils.toPrimitive(vector.coordinates));

                    var header = buf.readStructHeader();
                    var tag = buf.readBytes();
                    var coordinates = buf.readBytes();

                    assertThat(header.length()).isEqualTo(2);
                    assertThat(header.tag()).isEqualTo((short) 'V');
                    assertThat(tag.readableBytes()).isEqualTo(1);
                    assertThat(tag.readUnsignedByte()).isEqualTo(TypeMarker.INT8.getValue());
                    assertThat(coordinates.readableBytes()).isEqualTo(vector.dimensions);

                    for (var i = 0; i < vector.dimensions; i++) {
                        var coordinate = coordinates.readByte();
                        assertThat(coordinate).isEqualTo(vector.coordinates[i]);
                    }

                    assertThat(coordinates.isReadable()).isFalse();
                }));
    }

    @TestFactory
    Stream<DynamicTest> shouldWriteInt16Vector() {
        return Stream.<VectorSpecification<Short>>of(
                        new VectorSpecification<>(),
                        VectorSpecification.vector16(0, 21, 42),
                        VectorSpecification.vector16(0, 21, 42, 84, 73, 97, 62, 21, 33, 99, 128))
                .map(vector -> dynamicTest(vector.toString(), () -> {
                    var buf = PackstreamBuf.allocUnpooled();
                    var ctx = Mockito.mock(WriterContext.class);

                    Mockito.doReturn(buf).when(ctx).buffer();

                    DefaultVersionedValueWriter.getInstance()
                            .writeVector(ctx, ArrayUtils.toPrimitive(vector.coordinates));

                    var header = buf.readStructHeader();
                    var tag = buf.readBytes();
                    var coordinates = buf.readBytes();

                    assertThat(header.length()).isEqualTo(2);
                    assertThat(header.tag()).isEqualTo((short) 'V');
                    assertThat(tag.readableBytes()).isEqualTo(1);
                    assertThat(tag.readUnsignedByte()).isEqualTo(TypeMarker.INT16.getValue());
                    assertThat(coordinates.readableBytes()).isEqualTo(vector.dimensions * 2);

                    for (var i = 0; i < vector.dimensions; i++) {
                        var coordinate = coordinates.readShort();
                        assertThat(coordinate).isEqualTo(vector.coordinates[i]);
                    }

                    assertThat(coordinates.isReadable()).isFalse();
                }));
    }

    @TestFactory
    Stream<DynamicTest> shouldWriteInt32Vector() {
        return Stream.<VectorSpecification<Integer>>of(
                        new VectorSpecification<>(),
                        VectorSpecification.vector32(0, 21, 42),
                        VectorSpecification.vector32(0, 21, 42, 84, 73, 97, 62, 21, 33, 99, 128))
                .map(vector -> dynamicTest(vector.toString(), () -> {
                    var buf = PackstreamBuf.allocUnpooled();
                    var ctx = Mockito.mock(WriterContext.class);

                    Mockito.doReturn(buf).when(ctx).buffer();

                    DefaultVersionedValueWriter.getInstance()
                            .writeVector(ctx, ArrayUtils.toPrimitive(vector.coordinates));

                    var header = buf.readStructHeader();
                    var tag = buf.readBytes();
                    var coordinates = buf.readBytes();

                    assertThat(header.length()).isEqualTo(2);
                    assertThat(header.tag()).isEqualTo((short) 'V');
                    assertThat(tag.readableBytes()).isEqualTo(1);
                    assertThat(tag.readUnsignedByte()).isEqualTo(TypeMarker.INT32.getValue());
                    assertThat(coordinates.readableBytes()).isEqualTo(vector.dimensions * 4);

                    for (var i = 0; i < vector.dimensions; i++) {
                        var coordinate = coordinates.readInt();
                        assertThat(coordinate).isEqualTo(vector.coordinates[i]);
                    }

                    assertThat(coordinates.isReadable()).isFalse();
                }));
    }

    @TestFactory
    Stream<DynamicTest> shouldWriteInt64Vector() {
        return Stream.<VectorSpecification<Long>>of(
                        new VectorSpecification<>(),
                        VectorSpecification.vector64(0, 21, 42),
                        VectorSpecification.vector64(0, 21, 42, 84, 73, 97, 62, 21, 33, 99, 128))
                .map(vector -> dynamicTest(vector.toString(), () -> {
                    var buf = PackstreamBuf.allocUnpooled();
                    var ctx = Mockito.mock(WriterContext.class);

                    Mockito.doReturn(buf).when(ctx).buffer();

                    DefaultVersionedValueWriter.getInstance()
                            .writeVector(ctx, ArrayUtils.toPrimitive(vector.coordinates));

                    var header = buf.readStructHeader();
                    var tag = buf.readBytes();
                    var coordinates = buf.readBytes();

                    assertThat(header.length()).isEqualTo(2);
                    assertThat(header.tag()).isEqualTo((short) 'V');
                    assertThat(tag.readableBytes()).isEqualTo(1);
                    assertThat(tag.readUnsignedByte()).isEqualTo(TypeMarker.INT64.getValue());
                    assertThat(coordinates.readableBytes()).isEqualTo(vector.dimensions * 8);

                    for (var i = 0; i < vector.dimensions; i++) {
                        var coordinate = coordinates.readLong();
                        assertThat(coordinate).isEqualTo(vector.coordinates[i]);
                    }

                    assertThat(coordinates.isReadable()).isFalse();
                }));
    }

    @TestFactory
    Stream<DynamicTest> shouldWriteFloat32Vector() {
        return Stream.<VectorSpecification<Float>>of(
                        new VectorSpecification<>(),
                        VectorSpecification.vector32(0.125f, 21.25f, 42.5f),
                        VectorSpecification.vector32(
                                0.125f, 21.25f, 42.5f, 84.75f, 73.125f, 97.25f, 62.5f, 21.75f, 33.125f, 99.25f, 128.5f))
                .map(vector -> dynamicTest(vector.toString(), () -> {
                    var buf = PackstreamBuf.allocUnpooled();
                    var ctx = Mockito.mock(WriterContext.class);

                    Mockito.doReturn(buf).when(ctx).buffer();

                    DefaultVersionedValueWriter.getInstance()
                            .writeVector(ctx, ArrayUtils.toPrimitive(vector.coordinates));

                    var header = buf.readStructHeader();
                    var tag = buf.readBytes();
                    var coordinates = buf.readBytes();

                    assertThat(header.length()).isEqualTo(2);
                    assertThat(header.tag()).isEqualTo((short) 'V');
                    assertThat(tag.readableBytes()).isEqualTo(1);
                    assertThat(tag.readUnsignedByte()).isEqualTo(TypeMarker.FLOAT32.getValue());
                    assertThat(coordinates.readableBytes()).isEqualTo(vector.dimensions * 4);

                    for (var i = 0; i < vector.dimensions; i++) {
                        var coordinate = coordinates.readFloat();
                        assertThat(coordinate).isEqualTo(vector.coordinates[i]);
                    }

                    assertThat(coordinates.isReadable()).isFalse();
                }));
    }

    @TestFactory
    Stream<DynamicTest> shouldWriteFloat64Vector() {
        return Stream.<VectorSpecification<Double>>of(
                        new VectorSpecification<>(),
                        VectorSpecification.vector64(0.125, 21.25, 42.5),
                        VectorSpecification.vector64(
                                0.125, 21.25, 42.5, 84.75, 73.125, 97.25, 62.5, 21.75, 33.125, 99.25, 128.5))
                .map(vector -> dynamicTest(vector.toString(), () -> {
                    var buf = PackstreamBuf.allocUnpooled();
                    var ctx = Mockito.mock(WriterContext.class);

                    Mockito.doReturn(buf).when(ctx).buffer();

                    DefaultVersionedValueWriter.getInstance()
                            .writeVector(ctx, ArrayUtils.toPrimitive(vector.coordinates));

                    var header = buf.readStructHeader();
                    var tag = buf.readBytes();
                    var coordinates = buf.readBytes();

                    assertThat(header.length()).isEqualTo(2);
                    assertThat(header.tag()).isEqualTo((short) 'V');
                    assertThat(tag.readableBytes()).isEqualTo(1);
                    assertThat(tag.readUnsignedByte()).isEqualTo(TypeMarker.FLOAT64.getValue());
                    assertThat(coordinates.readableBytes()).isEqualTo(vector.dimensions * 8);

                    for (var i = 0; i < vector.dimensions; i++) {
                        var coordinate = coordinates.readDouble();
                        assertThat(coordinate).isEqualTo(vector.coordinates[i]);
                    }

                    assertThat(coordinates.isReadable()).isFalse();
                }));
    }

    @Test
    void shouldWriteUnsupportedType() throws PackstreamReaderException {
        var buf = PackstreamBuf.allocUnpooled();
        var ctx = Mockito.mock(WriterContext.class);

        Mockito.doReturn(buf).when(ctx).buffer();

        DefaultVersionedValueWriter.getInstance()
                .writeUnsupportedType(ctx, "NewFangledType", new ProtocolVersion(43, 12), null);

        var header = buf.readStructHeader();
        var typeName = buf.readString();
        var majorVersion = buf.readInt();
        var minorVersion = buf.readInt();
        var map = buf.readMap(PackstreamBuf::readString);

        assertThat(header.length()).isEqualTo(4);
        assertThat(header.tag()).isEqualTo(StructType.UNSUPPORTED.getTag());

        assertThat(typeName).isEqualTo("NewFangledType");
        assertThat(majorVersion).isEqualTo(43);
        assertThat(minorVersion).isEqualTo(12);
        assertThat(map).isEqualTo(Map.of());

        assertThat(buf.raw().isReadable()).isFalse();
    }

    @Test
    void shouldWriteUnsupportedTypeWithMessage() throws PackstreamReaderException {
        var buf = PackstreamBuf.allocUnpooled();
        var ctx = Mockito.mock(WriterContext.class);

        Mockito.doReturn(buf).when(ctx).buffer();

        DefaultVersionedValueWriter.getInstance()
                .writeUnsupportedType(ctx, "NewFangledType", new ProtocolVersion(43, 12), "not supported here");

        var header = buf.readStructHeader();
        var typeName = buf.readString();
        var majorVersion = buf.readInt();
        var minorVersion = buf.readInt();
        var map = buf.readMap(PackstreamBuf::readString);

        assertThat(header.length()).isEqualTo(4);
        assertThat(header.tag()).isEqualTo(StructType.UNSUPPORTED.getTag());

        assertThat(typeName).isEqualTo("NewFangledType");
        assertThat(majorVersion).isEqualTo(43);
        assertThat(minorVersion).isEqualTo(12);
        assertThat(map).isEqualTo(Map.of("message", "not supported here"));

        assertThat(buf.raw().isReadable()).isFalse();
    }

    private record VectorSpecification<N>(int dimensions, N[] coordinates) {

        public VectorSpecification(N... coordinates) {
            this(coordinates.length, coordinates);
        }

        public static VectorSpecification<Byte> vector8(int... coordinates) {
            var copy = new Byte[coordinates.length];
            for (int i = 0; i < coordinates.length; i++) {
                copy[i] = (byte) coordinates[i];
            }

            return new VectorSpecification<>(copy);
        }

        public static VectorSpecification<Short> vector16(int... coordinates) {
            var copy = new Short[coordinates.length];
            for (int i = 0; i < coordinates.length; i++) {
                copy[i] = (short) coordinates[i];
            }

            return new VectorSpecification<>(copy);
        }

        public static VectorSpecification<Integer> vector32(int... coordinates) {
            return new VectorSpecification<>(ArrayUtils.toObject(coordinates));
        }

        public static VectorSpecification<Long> vector64(long... coordinates) {
            return new VectorSpecification<>(ArrayUtils.toObject(coordinates));
        }

        public static VectorSpecification<Float> vector32(float... coordinates) {
            return new VectorSpecification<>(ArrayUtils.toObject(coordinates));
        }

        public static VectorSpecification<Double> vector64(double... coordinates) {
            return new VectorSpecification<>(ArrayUtils.toObject(coordinates));
        }

        @Override
        public String toString() {
            return "Vector(" + this.dimensions + ", ["
                    + Stream.of(this.coordinates).map(Object::toString).collect(Collectors.joining(",")) + "])";
        }
    }
}
