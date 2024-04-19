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
package org.neo4j.values.storable;

import static java.time.ZoneOffset.UTC;
import static org.neo4j.util.Preconditions.checkArgument;
import static org.neo4j.util.Preconditions.checkState;
import static org.neo4j.values.storable.DateValue.epochDate;
import static org.neo4j.values.storable.Values.booleanArray;
import static org.neo4j.values.storable.Values.booleanValue;
import static org.neo4j.values.storable.Values.byteArray;
import static org.neo4j.values.storable.Values.byteValue;
import static org.neo4j.values.storable.Values.charArray;
import static org.neo4j.values.storable.Values.charValue;
import static org.neo4j.values.storable.Values.dateArray;
import static org.neo4j.values.storable.Values.dateTimeArray;
import static org.neo4j.values.storable.Values.doubleArray;
import static org.neo4j.values.storable.Values.doubleValue;
import static org.neo4j.values.storable.Values.durationArray;
import static org.neo4j.values.storable.Values.floatArray;
import static org.neo4j.values.storable.Values.floatValue;
import static org.neo4j.values.storable.Values.intArray;
import static org.neo4j.values.storable.Values.intValue;
import static org.neo4j.values.storable.Values.localDateTimeArray;
import static org.neo4j.values.storable.Values.localTimeArray;
import static org.neo4j.values.storable.Values.longArray;
import static org.neo4j.values.storable.Values.longValue;
import static org.neo4j.values.storable.Values.pointArray;
import static org.neo4j.values.storable.Values.pointValue;
import static org.neo4j.values.storable.Values.shortArray;
import static org.neo4j.values.storable.Values.shortValue;
import static org.neo4j.values.storable.Values.stringArray;
import static org.neo4j.values.storable.Values.stringValue;
import static org.neo4j.values.storable.Values.timeArray;

import java.nio.BufferOverflowException;
import java.nio.ByteBuffer;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.OffsetTime;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import org.apache.commons.lang3.StringUtils;
import org.neo4j.graphdb.Resource;
import org.neo4j.internal.helpers.ArrayUtil;
import org.neo4j.values.utils.TemporalUtil;

public final class ValueByteBufferCodec {
    private ValueByteBufferCodec() {}

    public interface ByteBufferAllocator {
        ByteBuffer allocate(long capacity);

        void free();
    }

    public static class Writer implements ValueWriter<RuntimeException>, Resource {
        private final ByteBufferAllocator allocation;
        private ByteBuffer buf;

        public Writer(int chunkSize, ByteBufferAllocator allocation) {
            this.allocation = allocation;
            this.buf = allocation.allocate(chunkSize);
        }

        @Override
        public void close() {
            allocation.free();
            buf = null;
        }

        public ByteBuffer write(Value value) {
            checkState(buf != null, "Writer is closed");
            try {
                buf.clear();
                buf.put((byte) ValueByteBufferCodec.ValueType.forValue(value).ordinal());
                value.writeTo(this);
                buf.flip();
                return buf;
            } catch (BufferOverflowException e) {
                final int newSize = newSizeGrow();
                allocation.free();
                buf = allocation.allocate(newSize);
                return write(value);
            }
        }

        private int newSizeGrow() {
            long old = buf.capacity();
            if (old == ArrayUtil.MAX_ARRAY_SIZE) {
                throw new RuntimeException("Unable to allocate array bigger than " + ArrayUtil.MAX_ARRAY_SIZE);
            }
            return Math.toIntExact(Math.min(old * 2, ArrayUtil.MAX_ARRAY_SIZE));
        }

        @Override
        public void writeNull() {
            // nop
        }

        @Override
        public void writeBoolean(boolean value) {
            buf.put((byte) (value ? 1 : 0));
        }

        @Override
        public void writeInteger(byte value) {
            buf.put(value);
        }

        @Override
        public void writeInteger(short value) {
            buf.putShort(value);
        }

        @Override
        public void writeInteger(int value) {
            buf.putInt(value);
        }

        @Override
        public void writeInteger(long value) {
            buf.putLong(value);
        }

        @Override
        public void writeFloatingPoint(float value) {
            buf.putFloat(value);
        }

        @Override
        public void writeFloatingPoint(double value) {
            buf.putDouble(value);
        }

        @Override
        public void writeString(String value) {
            final int len = value.length();
            buf.putInt(value.length());
            for (int i = 0; i < len; i++) {
                final char c = value.charAt(i);
                buf.putChar(c);
            }
        }

        @Override
        public void writeString(char value) {
            buf.putChar(value);
        }

        @Override
        public void beginArray(int size, ArrayType arrayType) {
            buf.putInt(size);
        }

        @Override
        public void endArray() {
            // nop
        }

        @Override
        public void writeByteArray(byte[] value) {
            buf.putInt(value.length);
            buf.put(value);
        }

        @Override
        public void writePoint(CoordinateReferenceSystem crs, double[] coordinate) {
            checkArgument(
                    coordinate.length == crs.getDimension(),
                    "Dimension for %s is %d, got %d",
                    crs.getName(),
                    crs.getDimension(),
                    coordinate.length);
            buf.putInt(crs.getCode());
            for (int i = 0; i < crs.getDimension(); i++) {
                buf.putDouble(coordinate[i]);
            }
        }

        @Override
        public void writeDuration(long months, long days, long seconds, int nanos) {
            buf.putLong(months);
            buf.putLong(days);
            buf.putLong(seconds);
            buf.putInt(nanos);
        }

        @Override
        public void writeDate(LocalDate localDate) {
            buf.putLong(localDate.toEpochDay());
        }

        @Override
        public void writeLocalTime(LocalTime localTime) {
            buf.putLong(localTime.toNanoOfDay());
        }

        @Override
        public void writeTime(OffsetTime offsetTime) {
            buf.putLong(TemporalUtil.getNanosOfDayUTC(offsetTime));
            buf.putInt(offsetTime.getOffset().getTotalSeconds());
        }

        @Override
        public void writeLocalDateTime(LocalDateTime localDateTime) {
            buf.putLong(localDateTime.toEpochSecond(UTC));
            buf.putInt(localDateTime.getNano());
        }

        @Override
        public void writeDateTime(ZonedDateTime zonedDateTime) {
            buf.putLong(zonedDateTime.toEpochSecond());
            buf.putInt(zonedDateTime.getNano());

            final ZoneId zone = zonedDateTime.getZone();
            if (zone instanceof ZoneOffset) {
                final int offsetSeconds = ((ZoneOffset) zone).getTotalSeconds();
                // lowest bit set to 0: it's a zone offset in seconds
                buf.putInt(offsetSeconds << 1);
            } else {
                // lowest bit set to 1: it's a zone id
                final int zoneId = (TimeZones.map(zone.getId()) << 1) | 1;
                buf.putInt(zoneId);
            }
        }
    }

    private static BooleanValue readBoolean(ByteBuffer chunk, int offset) {
        return booleanValue(chunk.get(offset) != 0);
    }

    private static BooleanArray readBooleanArray(ByteBuffer bb, int offset) {
        final int len = bb.getInt(offset);
        offset += Integer.BYTES;
        final boolean[] array = new boolean[len];
        for (int i = 0; i < len; i++) {
            array[i] = bb.get(offset) != 0;
            ++offset;
        }
        return booleanArray(array);
    }

    private static ByteValue readByte(ByteBuffer chunk, int offset) {
        return byteValue(chunk.get(offset));
    }

    private static ByteArray readByteArray(ByteBuffer bb, int offset) {
        final int len = bb.getInt(offset);
        offset += Integer.BYTES;
        final byte[] array = new byte[len];
        for (int i = 0; i < len; i++) {
            array[i] = bb.get(offset);
            ++offset;
        }
        return byteArray(array);
    }

    private static CharValue readChar(ByteBuffer chunk, int offset) {
        return charValue(chunk.getChar(offset));
    }

    private static CharArray readCharArray(ByteBuffer bb, int offset) {
        final int len = bb.getInt(offset);
        offset += Integer.BYTES;
        final char[] array = new char[len];
        for (int i = 0; i < len; i++) {
            array[i] = bb.getChar(offset);
            offset += Character.BYTES;
        }
        return charArray(array);
    }

    private static DateValue readDate(ByteBuffer chunk, int offset) {
        return epochDate(chunk.getLong(offset));
    }

    private static ArrayValue readDateArray(ByteBuffer bb, int offset) {
        final int len = bb.getInt(offset);
        offset += Integer.BYTES;
        final LocalDate[] array = new LocalDate[len];
        for (int i = 0; i < len; i++) {
            array[i] = LocalDate.ofEpochDay(bb.getLong(offset));
            offset += Long.BYTES;
        }
        return dateArray(array);
    }

    private static DoubleValue readDouble(ByteBuffer chunk, int offset) {
        return doubleValue(chunk.getDouble(offset));
    }

    private static DoubleArray readDoubleArray(ByteBuffer bb, int offset) {
        final int len = bb.getInt(offset);
        offset += Integer.BYTES;
        final double[] array = new double[len];
        for (int i = 0; i < len; i++) {
            array[i] = bb.getDouble(offset);
            offset += Long.BYTES;
        }
        return doubleArray(array);
    }

    private static DurationValue readDuration(ByteBuffer bb, int offset) {
        final long months = bb.getLong(offset);
        offset += Long.BYTES;
        final long days = bb.getLong(offset);
        offset += Long.BYTES;
        final long seconds = bb.getLong(offset);
        offset += Long.BYTES;
        final int nanos = bb.getInt(offset);
        return DurationValue.duration(months, days, seconds, nanos);
    }

    private static ArrayValue readDurationArray(ByteBuffer bb, int offset) {
        final int len = bb.getInt(offset);
        offset += Integer.BYTES;
        final DurationValue[] array = new DurationValue[len];
        for (int i = 0; i < len; i++) {
            array[i] = readDuration(bb, offset);
            offset += 3 * Long.BYTES + Integer.BYTES;
        }
        return durationArray(array);
    }

    private static FloatValue readFloat(ByteBuffer chunk, int offset) {
        return floatValue(chunk.getFloat(offset));
    }

    private static FloatArray readFloatArray(ByteBuffer bb, int offset) {
        final int len = bb.getInt(offset);
        offset += Integer.BYTES;
        float[] array = new float[len];
        for (int i = 0; i < len; i++) {
            array[i] = bb.getFloat(offset);
            offset += Float.BYTES;
        }
        return floatArray(array);
    }

    private static IntValue readInt(ByteBuffer chunk, int offset) {
        return intValue(chunk.getInt(offset));
    }

    private static IntArray readIntArray(ByteBuffer bb, int offset) {
        final int len = bb.getInt(offset);
        offset += Integer.BYTES;
        final int[] array = new int[len];
        for (int i = 0; i < len; i++) {
            array[i] = bb.getInt(offset);
            offset += Integer.BYTES;
        }
        return intArray(array);
    }

    private static LocalDateTimeValue readLocalDateTime(ByteBuffer bb, int offset) {
        final long epochSecond = bb.getLong(offset);
        offset += Long.BYTES;
        final int nanos = bb.getInt(offset);
        return LocalDateTimeValue.localDateTime(epochSecond, nanos);
    }

    private static ArrayValue readLocalDateTimeArray(ByteBuffer bb, int offset) {
        final int len = bb.getInt(offset);
        offset += Integer.BYTES;
        final LocalDateTime[] array = new LocalDateTime[len];
        for (int i = 0; i < len; i++) {
            final long epochSecond = bb.getLong(offset);
            offset += Long.BYTES;
            final int nanos = bb.getInt(offset);
            offset += Integer.BYTES;
            array[i] = LocalDateTime.ofEpochSecond(epochSecond, nanos, UTC);
        }
        return localDateTimeArray(array);
    }

    private static LocalTimeValue readLocalTime(ByteBuffer chunk, int offset) {
        return LocalTimeValue.localTime(chunk.getLong(offset));
    }

    private static ArrayValue readLocalTimeArray(ByteBuffer bb, int offset) {
        final int len = bb.getInt(offset);
        offset += Integer.BYTES;
        final LocalTime[] array = new LocalTime[len];
        for (int i = 0; i < len; i++) {
            array[i] = LocalTime.ofNanoOfDay(bb.getLong(offset));
            offset += Long.BYTES;
        }
        return localTimeArray(array);
    }

    private static LongValue readLong(ByteBuffer chunk, int offset) {
        return longValue(chunk.getLong(offset));
    }

    private static LongArray readLongArray(ByteBuffer bb, int offset) {
        final int len = bb.getInt(offset);
        offset += Integer.BYTES;
        final long[] array = new long[len];
        for (int i = 0; i < len; i++) {
            array[i] = bb.getLong(offset);
            offset += Long.BYTES;
        }
        return longArray(array);
    }

    private static PointValue readPoint(ByteBuffer chunk, int offset) {
        final int crsCode = chunk.getInt(offset);
        offset += Integer.BYTES;
        final CoordinateReferenceSystem crs = CoordinateReferenceSystem.get(crsCode);
        final double[] coordinate = new double[crs.getDimension()];
        for (int i = 0; i < coordinate.length; i++) {
            coordinate[i] = chunk.getDouble(offset);
            offset += Double.BYTES;
        }
        return pointValue(crs, coordinate);
    }

    private static PointArray readPointArray(ByteBuffer bb, int offset) {
        final int len = bb.getInt(offset);
        offset += Integer.BYTES;
        final PointValue[] array = new PointValue[len];
        for (int i = 0; i < len; i++) {
            final PointValue point = readPoint(bb, offset);
            array[i] = point;
            offset += Integer.BYTES + point.getCoordinateReferenceSystem().getDimension() * Double.BYTES;
        }
        return pointArray(array);
    }

    private static String readRawString(ByteBuffer chunk, int offset) {
        final int len = chunk.getInt(offset);
        if (len == 0) {
            return StringUtils.EMPTY;
        }
        offset += Integer.BYTES;

        final char[] chars = new char[len];
        for (int i = 0; i < len; i++) {
            chars[i] = chunk.getChar(offset);
            offset += Character.BYTES;
        }
        return new String(chars);
    }

    private static ShortValue readShort(ByteBuffer chunk, int offset) {
        return shortValue(chunk.getShort(offset));
    }

    private static ShortArray readShortArray(ByteBuffer bb, int offset) {
        final int len = bb.getInt(offset);
        offset += Integer.BYTES;
        final short[] array = new short[len];
        for (int i = 0; i < len; i++) {
            array[i] = bb.getShort(offset);
            offset += Short.BYTES;
        }
        return shortArray(array);
    }

    private static TextValue readString(ByteBuffer chunk, int offset) {
        return stringValue(readRawString(chunk, offset));
    }

    private static ArrayValue readStringArray(ByteBuffer bb, int offset) {
        final int len = bb.getInt(offset);
        offset += Integer.BYTES;

        final String[] array = new String[len];
        for (int i = 0; i < len; i++) {
            final String str = readRawString(bb, offset);
            array[i] = str;
            offset += Integer.BYTES + str.length() * Character.BYTES;
        }
        return stringArray(array);
    }

    private static TimeValue readTime(ByteBuffer bb, int offset) {
        return TimeValue.time(readRawTime(bb, offset));
    }

    private static ArrayValue readTimeArray(ByteBuffer bb, int offset) {
        final int len = bb.getInt(offset);
        offset += Integer.BYTES;
        final OffsetTime[] array = new OffsetTime[len];
        for (int i = 0; i < len; i++) {
            array[i] = readRawTime(bb, offset);
            offset += Long.BYTES + Integer.BYTES;
        }
        return timeArray(array);
    }

    private static OffsetTime readRawTime(ByteBuffer bb, int offset) {
        final long nanosOfDayUTC = bb.getLong(offset);
        offset += Long.BYTES;
        final int offsetSeconds = bb.getInt(offset);
        return OffsetTime.ofInstant(Instant.ofEpochSecond(0, nanosOfDayUTC), ZoneOffset.ofTotalSeconds(offsetSeconds));
    }

    private static DateTimeValue readDateTime(ByteBuffer bb, int offset) {
        final long epocSeconds = bb.getLong(offset);
        offset += Long.BYTES;
        final int nanos = bb.getInt(offset);
        offset += Integer.BYTES;
        final int z = bb.getInt(offset);
        return DateTimeValue.datetime(epocSeconds, nanos, toZoneId(z));
    }

    private static ZoneId toZoneId(int z) {
        // if lowest bit is set to 1 then it's a shifted zone id
        if ((z & 1) != 0) {
            final String zoneId = TimeZones.map((short) (z >> 1));
            return ZoneId.of(zoneId);
        }
        // otherwise it's a shifted offset seconds value
        // preserve sign bit for negative offsets
        return ZoneOffset.ofTotalSeconds(z >> 1);
    }

    private static ArrayValue readDateTimeArray(ByteBuffer bb, int offset) {
        final int len = bb.getInt(offset);
        offset += Integer.BYTES;
        final ZonedDateTime[] array = new ZonedDateTime[len];
        for (int i = 0; i < len; i++) {
            final long epocSeconds = bb.getLong(offset);
            offset += Long.BYTES;
            final int nanos = bb.getInt(offset);
            offset += Integer.BYTES;
            final int z = bb.getInt(offset);
            offset += Integer.BYTES;
            array[i] = ZonedDateTime.ofInstant(Instant.ofEpochSecond(epocSeconds, nanos), toZoneId(z));
        }
        return dateTimeArray(array);
    }

    public enum ValueType {
        NO_VALUE(NoValue.class, (unused, unused2) -> Values.NO_VALUE),
        BOOLEAN(BooleanValue.class, ValueByteBufferCodec::readBoolean),
        BOOLEAN_ARRAY(BooleanArray.class, ValueByteBufferCodec::readBooleanArray),
        BYTE(ByteValue.class, ValueByteBufferCodec::readByte),
        BYTE_ARRAY(ByteArray.class, ValueByteBufferCodec::readByteArray),
        SHORT(ShortValue.class, ValueByteBufferCodec::readShort),
        SHORT_ARRAY(ShortArray.class, ValueByteBufferCodec::readShortArray),
        INT(IntValue.class, ValueByteBufferCodec::readInt),
        INT_ARRAY(IntArray.class, ValueByteBufferCodec::readIntArray),
        LONG(LongValue.class, ValueByteBufferCodec::readLong),
        LONG_ARRAY(LongArray.class, ValueByteBufferCodec::readLongArray),
        FLOAT(FloatValue.class, ValueByteBufferCodec::readFloat),
        FLOAT_ARRAY(FloatArray.class, ValueByteBufferCodec::readFloatArray),
        DOUBLE(DoubleValue.class, ValueByteBufferCodec::readDouble),
        DOUBLE_ARRAY(DoubleArray.class, ValueByteBufferCodec::readDoubleArray),
        STRING(StringValue.class, ValueByteBufferCodec::readString),
        STRING_ARRAY(StringArray.class, ValueByteBufferCodec::readStringArray),
        CHAR(CharValue.class, ValueByteBufferCodec::readChar),
        CHAR_ARRAY(CharArray.class, ValueByteBufferCodec::readCharArray),
        POINT(PointValue.class, ValueByteBufferCodec::readPoint),
        POINT_ARRAY(PointArray.class, ValueByteBufferCodec::readPointArray),
        DURATION(DurationValue.class, ValueByteBufferCodec::readDuration),
        DURATION_ARRAY(DurationArray.class, ValueByteBufferCodec::readDurationArray),
        DATE(DateValue.class, ValueByteBufferCodec::readDate),
        DATE_ARRAY(DateArray.class, ValueByteBufferCodec::readDateArray),
        TIME(TimeValue.class, ValueByteBufferCodec::readTime),
        TIME_ARRAY(TimeArray.class, ValueByteBufferCodec::readTimeArray),
        DATE_TIME(DateTimeValue.class, ValueByteBufferCodec::readDateTime),
        DATE_TIME_ARRAY(DateTimeArray.class, ValueByteBufferCodec::readDateTimeArray),
        LOCAL_TIME(LocalTimeValue.class, ValueByteBufferCodec::readLocalTime),
        LOCAL_TIME_ARRAY(LocalTimeArray.class, ValueByteBufferCodec::readLocalTimeArray),
        LOCAL_DATE_TIME(LocalDateTimeValue.class, ValueByteBufferCodec::readLocalDateTime),
        LOCAL_DATE_TIME_ARRAY(LocalDateTimeArray.class, ValueByteBufferCodec::readLocalDateTimeArray),
        ;

        private final Class<?> valueClass;

        private final ValueReader reader;

        <T extends Value> ValueType(Class<? extends T> valueClass, ValueReader<T> reader) {
            this.valueClass = valueClass;
            this.reader = reader;
        }

        private static ValueType forValue(Value value) {
            for (ValueType valueType : VALUE_TYPES) {
                if (valueType.valueClass.isAssignableFrom(value.getClass())) {
                    return valueType;
                }
            }
            throw new IllegalArgumentException("Unsupported value type: " + value.getClass());
        }

        public ValueReader getReader() {
            return reader;
        }
    }

    @FunctionalInterface
    public interface ValueReader<T extends Value> {
        T read(ByteBuffer bb, int offset);
    }

    public static final ValueType[] VALUE_TYPES = ValueType.values();

    public static Value readValue(ByteBuffer buffer) {
        byte valueTypeOrdinal = buffer.get();
        var type = VALUE_TYPES[valueTypeOrdinal];
        return type.getReader().read(buffer, buffer.position());
    }
}
