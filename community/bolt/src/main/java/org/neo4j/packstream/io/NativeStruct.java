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

import static java.lang.String.format;
import static java.time.ZoneOffset.UTC;
import static java.util.Objects.requireNonNull;
import static org.neo4j.packstream.io.NativeStructType.DATE;
import static org.neo4j.packstream.io.NativeStructType.DATE_TIME;
import static org.neo4j.packstream.io.NativeStructType.DATE_TIME_ZONE_ID;
import static org.neo4j.packstream.io.NativeStructType.DURATION;
import static org.neo4j.packstream.io.NativeStructType.LOCAL_DATE_TIME;
import static org.neo4j.packstream.io.NativeStructType.LOCAL_TIME;
import static org.neo4j.packstream.io.NativeStructType.POINT_2D;
import static org.neo4j.packstream.io.NativeStructType.POINT_3D;
import static org.neo4j.packstream.io.NativeStructType.TIME;

import java.time.DateTimeException;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.OffsetDateTime;
import java.time.OffsetTime;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.time.zone.ZoneRulesException;
import java.util.Arrays;
import org.neo4j.exceptions.InvalidArgumentException;
import org.neo4j.packstream.error.reader.LimitExceededException;
import org.neo4j.packstream.error.reader.PackstreamReaderException;
import org.neo4j.packstream.error.reader.UnexpectedStructException;
import org.neo4j.packstream.error.reader.UnexpectedTypeException;
import org.neo4j.packstream.error.struct.IllegalStructArgumentException;
import org.neo4j.packstream.error.struct.IllegalStructSizeException;
import org.neo4j.packstream.struct.StructHeader;
import org.neo4j.util.VisibleForTesting;
import org.neo4j.values.storable.CoordinateReferenceSystem;
import org.neo4j.values.storable.DateTimeValue;
import org.neo4j.values.storable.DateValue;
import org.neo4j.values.storable.DurationValue;
import org.neo4j.values.storable.LocalDateTimeValue;
import org.neo4j.values.storable.LocalTimeValue;
import org.neo4j.values.storable.PointValue;
import org.neo4j.values.storable.TimeValue;
import org.neo4j.values.storable.Values;
import org.neo4j.values.utils.TemporalUtil;

/**
 * Provides utility functions which permit the encoding of native Packstream structs.
 */
public final class NativeStruct {
    private NativeStruct() {}

    /**
     * Writes the header of an arbitrary native struct type to the given buffer.
     *
     * @param buf    a buffer.
     * @param length a struct length.
     * @param type   a struct type.
     */
    @VisibleForTesting
    static void writeHeader(PackstreamBuf buf, long length, NativeStructType type) {
        requireNonNull(buf, "buf cannot be null");

        buf.writeStructHeader(new StructHeader(length, type.getTag()));
    }

    /**
     * Writes the header of an arbitrary native struct type to the given buffer.
     *
     * @param buf  a buffer.
     * @param type a struct type.
     */
    @VisibleForTesting
    static void writeHeader(PackstreamBuf buf, NativeStructType type) {
        writeHeader(buf, type.getDefaultSize(), type);
    }

    /**
     * Decodes a 2D or 3D point from a given buffer.
     *
     * @param buf a buffer.
     * @return a point value.
     * @throws LimitExceededException         when one of the given values exceeds its limit.
     * @throws UnexpectedTypeException        when an unexpected value type is encountered.
     * @throws UnexpectedStructException      when an unexpected struct type is encountered.
     * @throws IllegalStructSizeException     when the struct contains insufficient or too much data.
     * @throws IllegalStructArgumentException when a struct argument is outside of its bounds.
     */
    public static PointValue readPoint(PackstreamBuf buf)
            throws LimitExceededException, UnexpectedTypeException, UnexpectedStructException,
                    IllegalStructSizeException, IllegalStructArgumentException {
        var header = buf.readStructHeader();
        var type = NativeStructType.byTag(header.tag());

        return switch (type) {
            case POINT_2D -> readPoint2d(buf);
            case POINT_3D -> readPoint3d(buf);
            default -> throw new UnexpectedStructException(header);
        };
    }

    /**
     * Writes a point of arbitrary length to the given buffer.
     *
     * @param buf    a buffer.
     * @param crs    a coordinate reference system.
     * @param coords a set of coordinates.
     * @throws IllegalArgumentException when the given number of coordinates is not supported.
     */
    public static void writePoint(PackstreamBuf buf, CoordinateReferenceSystem crs, double[] coords) {
        requireNonNull(crs, "crs cannot be null");

        switch (coords.length) {
            case 2 -> writePoint2d(buf, crs, coords[0], coords[1]);
            case 3 -> writePoint3d(buf, crs, coords[0], coords[1], coords[2]);
            default -> throw new IllegalArgumentException("Point with 2D or 3D coordinate expected, " + "got crs=" + crs
                    + ", coordinate=" + Arrays.toString(coords));
        }
    }

    /**
     * Decodes a 2D struct from a given buffer.
     *
     * @param buf a buffer.
     * @return a point value.
     * @throws UnexpectedTypeException        when an unexpected value type is encountered.
     * @throws IllegalStructArgumentException when a struct argument is outside of its bounds.
     */
    public static PointValue readPoint2d(PackstreamBuf buf)
            throws UnexpectedTypeException, IllegalStructArgumentException {
        var crsCode = buf.readInt();
        var x = buf.readFloat();
        var y = buf.readFloat();

        if (crsCode > Integer.MAX_VALUE || crsCode < Integer.MIN_VALUE) {
            throw new IllegalStructArgumentException("crs", "crs code exceeds valid bounds");
        }

        CoordinateReferenceSystem crs;
        try {
            crs = CoordinateReferenceSystem.get((int) crsCode);
        } catch (InvalidArgumentException ex) {
            throw new IllegalStructArgumentException(
                    "crs", format("Illegal coordinate reference system: \"%s\"", crsCode), ex);
        }

        try {
            return Values.pointValue(crs, x, y);
        } catch (IllegalArgumentException ex) {
            throw new IllegalStructArgumentException(
                    "coords", format("Illegal CRS/coords combination (crs=%s, x=%s, y=%s)", crs, x, y), ex);
        }
    }

    /**
     * Writes a two-dimensional point to the given buffer.
     *
     * @param buf a buffer.
     * @param crs a coordinate reference system.
     * @param x   an X coordinate.
     * @param y   a Y coordinate.
     */
    public static void writePoint2d(PackstreamBuf buf, CoordinateReferenceSystem crs, double x, double y) {
        requireNonNull(crs, "crs cannot be null");

        writeHeader(buf, POINT_2D);

        buf.writeInt(crs.getCode()).writeFloat(x).writeFloat(y);
    }

    /**
     * Decodes a 3D point from a given buffer.
     *
     * @param buf a buffer.
     * @return a point value.
     * @throws UnexpectedTypeException        when an unexpected value type is encountered.
     * @throws IllegalStructArgumentException when a struct argument is outside of its expected bounds.
     */
    public static PointValue readPoint3d(PackstreamBuf buf)
            throws UnexpectedTypeException, IllegalStructArgumentException {
        var crsCode = buf.readInt();
        var x = buf.readFloat();
        var y = buf.readFloat();
        var z = buf.readFloat();

        if (crsCode > Integer.MAX_VALUE || crsCode < Integer.MIN_VALUE) {
            throw new IllegalStructArgumentException("crs", "crs code exceeds valid bounds");
        }

        CoordinateReferenceSystem crs;
        try {
            crs = CoordinateReferenceSystem.get((int) crsCode);
        } catch (InvalidArgumentException ex) {
            throw new IllegalStructArgumentException(
                    "crs", format("Illegal coordinate reference system: \"%s\"", crsCode), ex);
        }

        try {
            return Values.pointValue(crs, x, y, z);
        } catch (IllegalArgumentException ex) {
            throw new IllegalStructArgumentException(
                    "coords", format("Illegal CRS/coords combination (crs=%s, x=%s, y=%s, z=%s)", crs, x, y, z), ex);
        }
    }

    /**
     * Writes a three-dimensional point to the given buffer.
     *
     * @param buf a buffer.
     * @param crs a coordinate reference system.
     * @param x   an X coordinate.
     * @param y   a Y coordinate.
     * @param z   a Z coordinate.
     */
    public static void writePoint3d(PackstreamBuf buf, CoordinateReferenceSystem crs, double x, double y, double z) {
        requireNonNull(crs, "crs cannot be null");

        writeHeader(buf, POINT_3D);

        buf.writeInt(crs.getCode()).writeFloat(x).writeFloat(y).writeFloat(z);
    }

    /**
     * Decodes a duration value from a given buffer.
     *
     * @param buf a buffer.
     * @return a duration value.
     * @throws LimitExceededException     when a value exceeds its designated limits.
     * @throws UnexpectedTypeException    when an unexpected value type is encountered.
     * @throws UnexpectedStructException  when an unexpected struct type is encountered.
     * @throws IllegalStructSizeException when a struct provides insufficient or extra data.
     */
    public static DurationValue readDuration(PackstreamBuf buf)
            throws LimitExceededException, UnexpectedTypeException, UnexpectedStructException,
                    IllegalStructSizeException {
        var months = buf.readInt();
        var days = buf.readInt();
        var seconds = buf.readInt();
        var nanos = buf.readInt();

        return DurationValue.duration(months, days, seconds, nanos);
    }

    /**
     * Writes an arbitrary duration to the given buffer.
     *
     * @param buf         a buffer.
     * @param months      a number of months.
     * @param days        a number of days.
     * @param seconds     a number of seconds within a given day.
     * @param nanoseconds a number of nanoseconds within a given second.
     */
    public static void writeDuration(PackstreamBuf buf, long months, long days, long seconds, int nanoseconds) {
        writeHeader(buf, DURATION);

        buf.writeInt(months).writeInt(days).writeInt(seconds).writeInt(nanoseconds);
    }

    /**
     * Decodes a date value from a given buffer.
     *
     * @param buf a buffer.
     * @return a date value.
     * @throws LimitExceededException     when a value exceeds its designated limits.
     * @throws UnexpectedTypeException    when an unexpected value type is encountered.
     * @throws UnexpectedStructException  when an unexpected struct type is encountered.
     * @throws IllegalStructSizeException when a struct provides insufficient or additional data.
     */
    public static DateValue readDate(PackstreamBuf buf)
            throws LimitExceededException, UnexpectedTypeException, UnexpectedStructException,
                    IllegalStructSizeException {
        var epochDays = buf.readInt();

        return DateValue.epochDate(epochDays);
    }

    /**
     * Writes an arbitrary local date to the given buffer.
     *
     * @param buf  a buffer.
     * @param date a date.
     */
    public static void writeDate(PackstreamBuf buf, LocalDate date) {
        requireNonNull(date, "date cannot be null");

        writeHeader(buf, DATE);

        buf.writeInt(date.toEpochDay());
    }

    /**
     * Decodes a local time value from a given buffer.
     *
     * @param buf a buffer.
     * @return a local time value.
     * @throws LimitExceededException     when a value exceeds its designated limits.
     * @throws UnexpectedTypeException    when an unexpected value type is encountered.
     * @throws UnexpectedStructException  when an unexpected struct type is encountered.
     * @throws IllegalStructSizeException when a struct argument provides insufficient or extra data.
     */
    public static LocalTimeValue readLocalTime(PackstreamBuf buf)
            throws LimitExceededException, UnexpectedTypeException, UnexpectedStructException,
                    IllegalStructSizeException {
        var nanoOfDay = buf.readInt();

        return LocalTimeValue.localTime(nanoOfDay);
    }

    /**
     * Writes a local time to the given buffer.
     *
     * @param buf  a buffer.
     * @param time a local time.
     */
    public static void writeLocalTime(PackstreamBuf buf, LocalTime time) {
        requireNonNull(time, "time cannot be null");

        writeHeader(buf, LOCAL_TIME);

        buf.writeInt(time.toNanoOfDay());
    }

    /**
     * Decodes a date time value from a given buffer.
     *
     * @param buf a buffer.
     * @return a date time value.
     * @throws UnexpectedTypeException        when an unexpected value type is encountered.
     * @throws IllegalStructArgumentException when a struct argument is outside its designated bounds.
     */
    public static DateTimeValue readDateTime(PackstreamBuf buf)
            throws UnexpectedTypeException, IllegalStructArgumentException {
        var epochSecond = buf.readInt();
        var nanos = buf.readInt();
        var offsetSeconds = buf.readInt();

        if (nanos > Integer.MAX_VALUE || nanos < Integer.MIN_VALUE) {
            throw new IllegalStructArgumentException("nanoseconds", "Value is out of bounds");
        }
        if (offsetSeconds > Integer.MAX_VALUE || offsetSeconds < Integer.MIN_VALUE) {
            throw new IllegalStructArgumentException("tz_offset_seconds", "Value is out of bounds");
        }

        ZoneOffset offset;
        Instant instant;
        LocalDateTime localDateTime;

        try {
            offset = ZoneOffset.ofTotalSeconds((int) offsetSeconds);
            instant = Instant.ofEpochSecond(epochSecond, nanos);
            localDateTime = LocalDateTime.ofInstant(instant, UTC);
        } catch (DateTimeException | ArithmeticException ex) {
            throw new IllegalStructArgumentException(
                    "seconds", format("Illegal epoch adjustment epoch seconds: %d+%d", epochSecond, nanos), ex);
        }

        return DateTimeValue.datetime(OffsetDateTime.of(localDateTime, offset));
    }

    /**
     * Writes a date time value to the given buffer.
     *
     * @param buf      a buffer.
     * @param dateTime a date time value.
     */
    public static void writeDateTime(PackstreamBuf buf, OffsetDateTime dateTime) {
        requireNonNull(dateTime, "dateTime cannot be null");

        writeHeader(buf, DATE_TIME);

        buf.writeInt(dateTime.toEpochSecond())
                .writeInt(dateTime.getNano())
                .writeInt(dateTime.getOffset().getTotalSeconds());
    }

    /**
     * Decodes a time value from a given buffer.
     *
     * @param buf a buffer.
     * @return a time value.
     * @throws UnexpectedStructException      when an unexpected struct type is encountered.
     * @throws LimitExceededException         when a value exceeds its designated bounds.
     * @throws UnexpectedTypeException        when an unexpected value type is encountered.
     * @throws IllegalStructSizeException     when a struct provides insufficient or extra data.
     * @throws IllegalStructArgumentException when a struct argument is outside of its designated bounds.
     */
    public static TimeValue readTime(PackstreamBuf buf)
            throws UnexpectedStructException, LimitExceededException, UnexpectedTypeException,
                    IllegalStructSizeException, IllegalStructArgumentException {
        var nanoOfDay = buf.readInt();
        var offsetSeconds = buf.readInt();

        if (offsetSeconds > Integer.MAX_VALUE || offsetSeconds < Integer.MIN_VALUE) {
            throw new IllegalStructArgumentException("tz_offset_seconds", "Value is out of bounds");
        }

        return TimeValue.time(
                TemporalUtil.nanosOfDayToUTC(nanoOfDay, (int) offsetSeconds),
                ZoneOffset.ofTotalSeconds((int) offsetSeconds));
    }

    /**
     * Writes an arbitrary time as well as its offset to the given buffer.
     *
     * @param buf  a buffer.
     * @param time an offset based time.
     */
    public static void writeTime(PackstreamBuf buf, OffsetTime time) {
        requireNonNull(time, "date cannot be null");

        writeHeader(buf, TIME);

        buf.writeInt(time.toLocalTime().toNanoOfDay()).writeInt(time.getOffset().getTotalSeconds());
    }

    /**
     * Decodes a local date time value from a given buffer.
     *
     * @param buf a buffer.
     * @return a local date time value.
     * @throws LimitExceededException     when a value exceeds its designated limit.
     * @throws UnexpectedTypeException    when an expected value type is encountered.
     * @throws UnexpectedStructException  when an unexpected struct type is encountered.
     * @throws IllegalStructSizeException when a struct provides insufficient or extra data.
     */
    public static LocalDateTimeValue readLocalDateTime(PackstreamBuf buf)
            throws LimitExceededException, UnexpectedTypeException, UnexpectedStructException,
                    IllegalStructSizeException {
        var epochSecond = buf.readInt();
        var nanos = buf.readInt();

        return LocalDateTimeValue.localDateTime(epochSecond, nanos);
    }

    /**
     * Writes an arbitrary local date and time to the given buffer.
     *
     * @param buf      a buffer.
     * @param dateTime a local date and time.
     */
    public static void writeLocalDateTime(PackstreamBuf buf, LocalDateTime dateTime) {
        requireNonNull(dateTime, "dateTime cannot be null");

        writeHeader(buf, LOCAL_DATE_TIME);

        buf.writeInt(dateTime.toEpochSecond(UTC)).writeInt(dateTime.getNano());
    }

    /**
     * Decodes a date time value from a given buffer.
     *
     * @param buf a buffer.
     * @return a date time value.
     * @throws LimitExceededException         when one of the given values exceeds its limit.
     * @throws UnexpectedTypeException        when an unexpected value type is encountered.
     * @throws UnexpectedStructException      when an unexpected struct type is encountered.
     * @throws IllegalStructSizeException     when the struct contains insufficient or too much data.
     * @throws IllegalStructArgumentException when a struct argument is outside of its bounds.
     */
    public static DateTimeValue readDateTimeZoneId(PackstreamBuf buf) throws PackstreamReaderException {
        var epochSecond = buf.readInt();
        var nanos = buf.readInt();
        var zoneName = buf.readString();

        if (nanos > Integer.MAX_VALUE || nanos < Integer.MIN_VALUE) {
            throw new IllegalStructArgumentException("nanoseconds", "Value exceeds bounds");
        }

        Instant instant;
        ZoneId zoneId;
        LocalDateTime localDateTime;
        try {
            instant = Instant.ofEpochSecond(epochSecond, nanos);
            zoneId = ZoneId.of(zoneName);
            localDateTime = LocalDateTime.ofInstant(instant, UTC);
        } catch (ZoneRulesException ex) {
            throw new IllegalStructArgumentException("tz_id", format("Illegal zone identifier: \"%s\"", zoneName), ex);
        } catch (DateTimeException | ArithmeticException ex) {
            throw new IllegalStructArgumentException(
                    "seconds", format("Illegal epoch adjustment epoch seconds: %d+%d", epochSecond, nanos), ex);
        }

        return DateTimeValue.datetime(ZonedDateTime.of(localDateTime, zoneId));
    }

    /**
     * Writes an arbitrary date and time along with its canonical zone identifier to the given buffer.
     *
     * @param buf      a buffer.
     * @param dateTime a zoned date and time.
     */
    public static void writeDateTimeZoneId(PackstreamBuf buf, ZonedDateTime dateTime) {
        requireNonNull(dateTime, "dateTime cannot be null");
        var epochSecondLocal = dateTime.toLocalDateTime().toEpochSecond(UTC);

        var zone = dateTime.getZone();

        if (zone instanceof ZoneOffset) {
            writeHeader(buf, DATE_TIME);
            buf.writeInt(epochSecondLocal)
                    .writeInt(dateTime.getNano())
                    .writeInt(dateTime.getOffset().getTotalSeconds());
            return;
        }

        writeHeader(buf, DATE_TIME_ZONE_ID);
        buf.writeInt(epochSecondLocal).writeInt(dateTime.getNano()).writeString(zone.getId());
    }
}
