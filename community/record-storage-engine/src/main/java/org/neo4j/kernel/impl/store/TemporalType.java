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
package org.neo4j.kernel.impl.store;

import static java.time.ZoneOffset.UTC;

import java.nio.ByteBuffer;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.OffsetTime;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.time.temporal.ChronoUnit;
import org.neo4j.kernel.impl.store.format.standard.StandardFormatSettings;
import org.neo4j.kernel.impl.store.record.PropertyBlock;
import org.neo4j.values.storable.ArrayValue;
import org.neo4j.values.storable.DateTimeValue;
import org.neo4j.values.storable.DateValue;
import org.neo4j.values.storable.DurationValue;
import org.neo4j.values.storable.LocalDateTimeValue;
import org.neo4j.values.storable.LocalTimeValue;
import org.neo4j.values.storable.LongArray;
import org.neo4j.values.storable.TimeValue;
import org.neo4j.values.storable.TimeZones;
import org.neo4j.values.storable.Value;
import org.neo4j.values.storable.Values;
import org.neo4j.values.utils.TemporalUtil;

/**
 * For the PropertyStore format, check {@link PropertyStore}.
 * For the array format, check {@link DynamicArrayStore}.
 */
public enum TemporalType {
    TEMPORAL_INVALID(0, "Invalid") {
        @Override
        public Value decodeForTemporal(long[] valueBlocks, int offset) {
            throw new UnsupportedOperationException("Cannot decode invalid temporal");
        }

        @Override
        public int calculateNumberOfBlocksUsedForTemporal(long firstBlock) {
            return PropertyType.BLOCKS_USED_FOR_BAD_TYPE_OR_ENCODING;
        }

        @Override
        public ArrayValue decodeArray(Value dataValue) {
            throw new UnsupportedOperationException("Cannot decode invalid temporal array");
        }
    },
    TEMPORAL_DATE(1, "Date") {
        @Override
        public Value decodeForTemporal(long[] valueBlocks, int offset) {
            long epochDay = valueIsInlined(valueBlocks[offset]) ? valueBlocks[offset] >>> 33 : valueBlocks[1 + offset];
            return DateValue.epochDate(epochDay);
        }

        @Override
        public int calculateNumberOfBlocksUsedForTemporal(long firstBlock) {
            return valueIsInlined(firstBlock) ? BLOCKS_LONG_INLINED : BLOCKS_LONG_NOT_INLINED;
        }

        @Override
        public ArrayValue decodeArray(Value dataValue) {
            if (dataValue instanceof LongArray numbers) {
                LocalDate[] dates = new LocalDate[numbers.intSize()];
                for (int i = 0; i < dates.length; i++) {
                    dates[i] = LocalDate.ofEpochDay(numbers.longValue(i));
                }
                return Values.dateArray(dates);
            } else {
                throw new InvalidRecordException("Array with unexpected type. Actual:"
                        + dataValue.getClass().getSimpleName() + ". Expected: LongArray.");
            }
        }

        private boolean valueIsInlined(long firstBlock) {
            // [][][][i][ssss,tttt][kkkk,kkkk][kkkk,kkkk][kkkk,kkkk]
            return (firstBlock & 0x100000000L) > 0;
        }
    },
    TEMPORAL_LOCAL_TIME(2, "LocalTime") {
        @Override
        public Value decodeForTemporal(long[] valueBlocks, int offset) {
            long nanoOfDay = valueIsInlined(valueBlocks[offset]) ? valueBlocks[offset] >>> 33 : valueBlocks[1 + offset];
            checkValidNanoOfDay(nanoOfDay);
            return LocalTimeValue.localTime(nanoOfDay);
        }

        @Override
        public int calculateNumberOfBlocksUsedForTemporal(long firstBlock) {
            return valueIsInlined(firstBlock) ? BLOCKS_LONG_INLINED : BLOCKS_LONG_NOT_INLINED;
        }

        @Override
        public ArrayValue decodeArray(Value dataValue) {
            if (dataValue instanceof LongArray numbers) {
                LocalTime[] times = new LocalTime[numbers.intSize()];
                for (int i = 0; i < times.length; i++) {
                    long nanoOfDay = numbers.longValue(i);
                    checkValidNanoOfDay(nanoOfDay);
                    times[i] = LocalTime.ofNanoOfDay(nanoOfDay);
                }
                return Values.localTimeArray(times);
            } else {
                throw new InvalidRecordException("Array with unexpected type. Actual:"
                        + dataValue.getClass().getSimpleName() + ". Expected: LongArray.");
            }
        }

        private boolean valueIsInlined(long firstBlock) {
            // [][][][i][ssss,tttt][kkkk,kkkk][kkkk,kkkk][kkkk,kkkk]
            return (firstBlock & 0x100000000L) > 0;
        }
    },
    TEMPORAL_LOCAL_DATE_TIME(3, "LocalDateTime") {
        @Override
        public Value decodeForTemporal(long[] valueBlocks, int offset) {
            long nanoOfSecond = valueBlocks[offset] >>> 32;
            checkValidNanoOfSecond(nanoOfSecond);
            long epochSecond = valueBlocks[1 + offset];
            return LocalDateTimeValue.localDateTime(epochSecond, nanoOfSecond);
        }

        @Override
        public int calculateNumberOfBlocksUsedForTemporal(long firstBlock) {
            return BLOCKS_LOCAL_DATETIME;
        }

        @Override
        public ArrayValue decodeArray(Value dataValue) {
            if (dataValue instanceof LongArray numbers) {
                LocalDateTime[] dateTimes = new LocalDateTime[numbers.intSize() / BLOCKS_LOCAL_DATETIME];
                for (int i = 0; i < dateTimes.length; i++) {
                    long epochSecond = numbers.longValue(i * BLOCKS_LOCAL_DATETIME);
                    long nanoOfSecond = numbers.longValue(i * BLOCKS_LOCAL_DATETIME + 1);
                    checkValidNanoOfSecond(nanoOfSecond);
                    dateTimes[i] = LocalDateTime.ofInstant(Instant.ofEpochSecond(epochSecond, nanoOfSecond), UTC);
                }
                return Values.localDateTimeArray(dateTimes);
            } else {
                throw new InvalidRecordException("Array with unexpected type. Actual:"
                        + dataValue.getClass().getSimpleName() + ". Expected: LongArray.");
            }
        }
    },
    TEMPORAL_TIME(4, "Time") {
        @Override
        public Value decodeForTemporal(long[] valueBlocks, int offset) {
            int secondOffset = (int) (valueBlocks[offset] >>> 32);
            long nanoOfDay = valueBlocks[1 + offset];
            checkValidNanoOfDayWithOffset(nanoOfDay, secondOffset);
            return TimeValue.time(nanoOfDay, ZoneOffset.ofTotalSeconds(secondOffset));
        }

        @Override
        public int calculateNumberOfBlocksUsedForTemporal(long firstBlock) {
            return BLOCKS_TIME;
        }

        @Override
        public ArrayValue decodeArray(Value dataValue) {
            if (dataValue instanceof LongArray numbers) {
                OffsetTime[] times = new OffsetTime[numbers.intSize() / BLOCKS_TIME];
                for (int i = 0; i < times.length; i++) {
                    long nanoOfDay = numbers.longValue(i * BLOCKS_TIME);
                    int secondOffset = (int) numbers.longValue(i * BLOCKS_TIME + 1);
                    checkValidNanoOfDay(nanoOfDay);
                    times[i] = OffsetTime.of(LocalTime.ofNanoOfDay(nanoOfDay), ZoneOffset.ofTotalSeconds(secondOffset));
                }
                return Values.timeArray(times);
            } else {
                throw new InvalidRecordException("Array with unexpected type. Actual:"
                        + dataValue.getClass().getSimpleName() + ". Expected: LongArray.");
            }
        }
    },
    TEMPORAL_DATE_TIME(5, "DateTime") {
        @Override
        public Value decodeForTemporal(long[] valueBlocks, int offset) {
            if (storingZoneOffset(valueBlocks[offset])) {
                int nanoOfSecond = (int) (valueBlocks[offset] >>> 33);
                checkValidNanoOfSecond(nanoOfSecond);
                long epochSecond = valueBlocks[1 + offset];
                int secondOffset = (int) valueBlocks[2 + offset];
                return DateTimeValue.datetime(epochSecond, nanoOfSecond, ZoneOffset.ofTotalSeconds(secondOffset));
            } else {
                int nanoOfSecond = (int) (valueBlocks[offset] >>> 33);
                checkValidNanoOfSecond(nanoOfSecond);
                long epochSecond = valueBlocks[1 + offset];
                short zoneNumber = (short) valueBlocks[2 + offset];
                return DateTimeValue.datetime(epochSecond, nanoOfSecond, ZoneId.of(TimeZones.map(zoneNumber)));
            }
        }

        @Override
        public int calculateNumberOfBlocksUsedForTemporal(long firstBlock) {
            return BLOCKS_DATETIME;
        }

        @Override
        public ArrayValue decodeArray(Value dataValue) {
            if (dataValue instanceof LongArray numbers) {
                ZonedDateTime[] dateTimes = new ZonedDateTime[numbers.intSize() / BLOCKS_DATETIME];
                for (int i = 0; i < dateTimes.length; i++) {
                    long epochSecond = numbers.longValue(i * BLOCKS_DATETIME);
                    long nanoOfSecond = numbers.longValue(i * BLOCKS_DATETIME + 1);
                    checkValidNanoOfSecond(nanoOfSecond);
                    long zoneValue = numbers.longValue(i * BLOCKS_DATETIME + 2);
                    if ((zoneValue & 1) == 1) {
                        int secondOffset = (int) (zoneValue >>> 1);
                        dateTimes[i] = ZonedDateTime.ofInstant(
                                Instant.ofEpochSecond(epochSecond, nanoOfSecond),
                                ZoneOffset.ofTotalSeconds(secondOffset));
                    } else {
                        short zoneNumber = (short) (zoneValue >>> 1);
                        dateTimes[i] = ZonedDateTime.ofInstant(
                                Instant.ofEpochSecond(epochSecond, nanoOfSecond), ZoneId.of(TimeZones.map(zoneNumber)));
                    }
                }
                return Values.dateTimeArray(dateTimes);
            } else {
                throw new InvalidRecordException("LocalTime array with unexpected type. Actual:"
                        + dataValue.getClass().getSimpleName() + ". Expected: LongArray.");
            }
        }

        private boolean storingZoneOffset(long firstBlock) {
            // [][][][i][ssss,tttt][kkkk,kkkk][kkkk,kkkk][kkkk,kkkk]
            return (firstBlock & 0x100000000L) > 0;
        }
    },
    TEMPORAL_DURATION(6, "Duration") {
        @Override
        public Value decodeForTemporal(long[] valueBlocks, int offset) {
            int nanos = (int) (valueBlocks[offset] >>> 32);
            long months = valueBlocks[1 + offset];
            long days = valueBlocks[2 + offset];
            long seconds = valueBlocks[3 + offset];
            return DurationValue.duration(months, days, seconds, nanos);
        }

        @Override
        public int calculateNumberOfBlocksUsedForTemporal(long firstBlock) {
            return BLOCKS_DURATION;
        }

        @Override
        public ArrayValue decodeArray(Value dataValue) {
            if (dataValue instanceof LongArray numbers) {
                DurationValue[] durations = new DurationValue[numbers.intSize() / BLOCKS_DURATION];
                for (int i = 0; i < durations.length; i++) {
                    durations[i] = DurationValue.duration(
                            numbers.longValue(i * BLOCKS_DURATION),
                            numbers.longValue(i * BLOCKS_DURATION + 1),
                            numbers.longValue(i * BLOCKS_DURATION + 2),
                            numbers.longValue(i * BLOCKS_DURATION + 3));
                }
                return Values.durationArray(durations);
            } else {
                throw new InvalidRecordException("Array with unexpected type. Actual:"
                        + dataValue.getClass().getSimpleName() + ". Expected: LongArray.");
            }
        }
    };

    private static final int BLOCKS_LONG_INLINED = 1;
    private static final int BLOCKS_LONG_NOT_INLINED = 2;
    private static final int BLOCKS_LOCAL_DATETIME = 2;
    private static final int BLOCKS_TIME = 2;
    private static final int BLOCKS_DATETIME = 3;
    private static final int BLOCKS_DURATION = 4;

    /**
     * Handler for header information for Temporal objects and arrays of Temporal objects
     */
    public static class TemporalHeader {
        private final int temporalType;

        private TemporalHeader(int temporalType) {
            this.temporalType = temporalType;
        }

        private void writeArrayHeaderTo(byte[] bytes) {
            bytes[0] = (byte) PropertyType.TEMPORAL.intValue();
            bytes[1] = (byte) temporalType;
        }

        static TemporalHeader fromArrayHeaderBytes(byte[] header) {
            int temporalType = Byte.toUnsignedInt(header[1]);
            return new TemporalHeader(temporalType);
        }

        public static TemporalHeader fromArrayHeaderByteBuffer(ByteBuffer buffer) {
            int temporalType = Byte.toUnsignedInt(buffer.get());
            return new TemporalHeader(temporalType);
        }
    }

    private static final TemporalType[] TYPES = TemporalType.values();
    private static final long TEMPORAL_TYPE_MASK = 0x00000000F0000000L;

    private static int getTemporalType(long firstBlock) {
        return (int) ((firstBlock & TEMPORAL_TYPE_MASK) >> 28);
    }

    public static int calculateNumberOfBlocksUsed(long firstBlock) {
        TemporalType geometryType = find(getTemporalType(firstBlock));
        return geometryType.calculateNumberOfBlocksUsedForTemporal(firstBlock);
    }

    private static TemporalType find(int temporalType) {
        if (temporalType < TYPES.length && temporalType >= 0) {
            return TYPES[temporalType];
        } else {
            // Kernel code requires no exceptions in deeper PropertyChain processing of corrupt/invalid data
            return TEMPORAL_INVALID;
        }
    }

    /**
     * Any out of range value means a store corruption
     */
    private static void checkValidNanoOfDay(long nanoOfDay) {
        if (nanoOfDay > LocalTime.MAX.toNanoOfDay() || nanoOfDay < LocalTime.MIN.toNanoOfDay()) {
            throw new InvalidRecordException("Nanosecond of day out of range:" + nanoOfDay);
        }
    }

    /**
     * Any out of range value means a store corruption
     */
    private static void checkValidNanoOfDayWithOffset(long nanoOfDayUTC, int secondOffset) {
        long nanoOfDay = TemporalUtil.nanosOfDayToLocal(nanoOfDayUTC, secondOffset);
        checkValidNanoOfDay(nanoOfDay);
    }

    /**
     * Any out of range value means a store corruption
     */
    private static void checkValidNanoOfSecond(long nanoOfSecond) {
        if (nanoOfSecond > 999_999_999 || nanoOfSecond < 0) {
            throw new InvalidRecordException("Nanosecond of second out of range:" + nanoOfSecond);
        }
    }

    public static Value decode(PropertyBlock block) {
        return decode(block.getValueBlocks(), 0);
    }

    public static Value decode(long[] valueBlocks, int offset) {
        long firstBlock = valueBlocks[offset];
        int temporalType = getTemporalType(firstBlock);
        return find(temporalType).decodeForTemporal(valueBlocks, offset);
    }

    public static long[] encodeDate(int keyId, long epochDay) {
        return encodeLong(keyId, epochDay, TemporalType.TEMPORAL_DATE.temporalType);
    }

    public static long[] encodeLocalTime(int keyId, long nanoOfDay) {
        return encodeLong(keyId, nanoOfDay, TemporalType.TEMPORAL_LOCAL_TIME.temporalType);
    }

    private static long[] encodeLong(int keyId, long val, int temporalType) {
        int idBits = StandardFormatSettings.PROPERTY_TOKEN_MAXIMUM_ID_BITS;

        long keyAndType = keyId | (((long) (PropertyType.TEMPORAL.intValue()) << idBits));
        long temporalTypeBits = temporalType << (idBits + 4);

        long[] data;
        if (ShortArray.LONG.getRequiredBits(val) <= 64 - 33) { // We only need one block for this value
            data = new long[BLOCKS_LONG_INLINED];
            data[0] = keyAndType | temporalTypeBits | (1L << 32) | (val << 33);
        } else { // We need two blocks for this value
            data = new long[BLOCKS_LONG_NOT_INLINED];
            data[0] = keyAndType | temporalTypeBits;
            data[1] = val;
        }

        return data;
    }

    public static long[] encodeLocalDateTime(int keyId, long epochSecond, long nanoOfSecond) {
        int idBits = StandardFormatSettings.PROPERTY_TOKEN_MAXIMUM_ID_BITS;

        long keyAndType = keyId | (((long) (PropertyType.TEMPORAL.intValue()) << idBits));
        long temporalTypeBits = TemporalType.TEMPORAL_LOCAL_DATE_TIME.temporalType << (idBits + 4);

        long[] data = new long[BLOCKS_LOCAL_DATETIME];
        // nanoOfSecond will never require more than 30 bits
        data[0] = keyAndType | temporalTypeBits | (nanoOfSecond << 32);
        data[1] = epochSecond;

        return data;
    }

    public static long[] encodeDateTime(int keyId, long epochSecond, long nanoOfSecond, String zoneId) {
        int idBits = StandardFormatSettings.PROPERTY_TOKEN_MAXIMUM_ID_BITS;
        short zoneNumber = TimeZones.map(zoneId);

        long keyAndType = keyId | (((long) (PropertyType.TEMPORAL.intValue()) << idBits));
        long temporalTypeBits = TemporalType.TEMPORAL_DATE_TIME.temporalType << (idBits + 4);

        long[] data = new long[BLOCKS_DATETIME];
        // nanoOfSecond will never require more than 30 bits
        data[0] = keyAndType | temporalTypeBits | (nanoOfSecond << 33);
        data[1] = epochSecond;
        data[2] = zoneNumber;

        return data;
    }

    public static long[] encodeDateTime(int keyId, long epochSecond, long nanoOfSecond, int secondOffset) {
        int idBits = StandardFormatSettings.PROPERTY_TOKEN_MAXIMUM_ID_BITS;

        long keyAndType = keyId | (((long) (PropertyType.TEMPORAL.intValue()) << idBits));
        long temporalTypeBits = TemporalType.TEMPORAL_DATE_TIME.temporalType << (idBits + 4);

        long[] data = new long[BLOCKS_DATETIME];
        // nanoOfSecond will never require more than 30 bits
        data[0] = keyAndType | temporalTypeBits | (1L << 32) | (nanoOfSecond << 33);
        data[1] = epochSecond;
        data[2] = secondOffset;

        return data;
    }

    public static long[] encodeTime(int keyId, long nanoOfDayUTC, int secondOffset) {
        int idBits = StandardFormatSettings.PROPERTY_TOKEN_MAXIMUM_ID_BITS;

        long keyAndType = keyId | (((long) (PropertyType.TEMPORAL.intValue()) << idBits));
        long temporalTypeBits = TemporalType.TEMPORAL_TIME.temporalType << (idBits + 4);

        long[] data = new long[BLOCKS_TIME];
        // Offset are always in the range +-18:00, so secondOffset will never require more than 17 bits
        data[0] = keyAndType | temporalTypeBits | ((long) secondOffset << 32);
        data[1] = nanoOfDayUTC;

        return data;
    }

    public static long[] encodeDuration(int keyId, long months, long days, long seconds, int nanos) {
        int idBits = StandardFormatSettings.PROPERTY_TOKEN_MAXIMUM_ID_BITS;

        long keyAndType = keyId | (((long) (PropertyType.TEMPORAL.intValue()) << idBits));
        long temporalTypeBits = TemporalType.TEMPORAL_DURATION.temporalType << (idBits + 4);

        long[] data = new long[BLOCKS_DURATION];
        data[0] = keyAndType | temporalTypeBits | ((long) nanos << 32);
        data[1] = months;
        data[2] = days;
        data[3] = seconds;

        return data;
    }

    public static byte[] encodeDateArray(LocalDate[] dates) {
        long[] data = new long[dates.length];
        for (int i = 0; i < data.length; i++) {
            data[i] = dates[i].toEpochDay();
        }
        TemporalHeader header = new TemporalHeader(TemporalType.TEMPORAL_DATE.temporalType);
        byte[] bytes = DynamicArrayStore.encodeFromNumbers(data, DynamicArrayStore.TEMPORAL_HEADER_SIZE);
        header.writeArrayHeaderTo(bytes);
        return bytes;
    }

    public static byte[] encodeLocalTimeArray(LocalTime[] times) {
        long[] data = new long[times.length];
        for (int i = 0; i < data.length; i++) {
            data[i] = times[i].toNanoOfDay();
        }
        TemporalHeader header = new TemporalHeader(TemporalType.TEMPORAL_LOCAL_TIME.temporalType);
        byte[] bytes = DynamicArrayStore.encodeFromNumbers(data, DynamicArrayStore.TEMPORAL_HEADER_SIZE);
        header.writeArrayHeaderTo(bytes);
        return bytes;
    }

    public static byte[] encodeLocalDateTimeArray(LocalDateTime[] dateTimes) {
        long[] data = new long[dateTimes.length * BLOCKS_LOCAL_DATETIME];
        for (int i = 0; i < dateTimes.length; i++) {
            data[i * BLOCKS_LOCAL_DATETIME] = dateTimes[i].toEpochSecond(UTC);
            data[i * BLOCKS_LOCAL_DATETIME + 1] = dateTimes[i].getNano();
        }
        TemporalHeader header = new TemporalHeader(TemporalType.TEMPORAL_LOCAL_DATE_TIME.temporalType);
        byte[] bytes = DynamicArrayStore.encodeFromNumbers(data, DynamicArrayStore.TEMPORAL_HEADER_SIZE);
        header.writeArrayHeaderTo(bytes);
        return bytes;
    }

    public static byte[] encodeTimeArray(OffsetTime[] times) {
        // We could store this in dateTimes.length * 1.5 if we wanted
        long[] data = new long[(int) (Math.ceil(times.length * BLOCKS_TIME))];
        int i;
        for (i = 0; i < times.length; i++) {
            data[i * BLOCKS_TIME] = times[i].toLocalTime().toNanoOfDay();
            data[i * BLOCKS_TIME + 1] = times[i].getOffset().getTotalSeconds();
        }

        TemporalHeader header = new TemporalHeader(TemporalType.TEMPORAL_TIME.temporalType);
        byte[] bytes = DynamicArrayStore.encodeFromNumbers(data, DynamicArrayStore.TEMPORAL_HEADER_SIZE);
        header.writeArrayHeaderTo(bytes);
        return bytes;
    }

    public static byte[] encodeDateTimeArray(ZonedDateTime[] dateTimes) {
        // We could store this in dateTimes.length * 2.5 if we wanted
        long[] data = new long[dateTimes.length * BLOCKS_DATETIME];

        int i;
        for (i = 0; i < dateTimes.length; i++) {
            data[i * BLOCKS_DATETIME] = dateTimes[i].toEpochSecond();
            data[i * BLOCKS_DATETIME + 1] = dateTimes[i].getNano();
            if (dateTimes[i].getZone() instanceof ZoneOffset offset) {
                int secondOffset = offset.getTotalSeconds();
                // Set lowest bit to 1 means offset
                data[i * BLOCKS_DATETIME + 2] = secondOffset << 1 | 1L;
            } else {
                String timeZoneId = dateTimes[i].getZone().getId();
                short zoneNumber = TimeZones.map(timeZoneId);
                // Set lowest bit to 0 means zone id
                data[i * BLOCKS_DATETIME + 2] = zoneNumber << 1;
            }
        }

        TemporalHeader header = new TemporalHeader(TemporalType.TEMPORAL_DATE_TIME.temporalType);
        byte[] bytes = DynamicArrayStore.encodeFromNumbers(data, DynamicArrayStore.TEMPORAL_HEADER_SIZE);
        header.writeArrayHeaderTo(bytes);
        return bytes;
    }

    public static byte[] encodeDurationArray(DurationValue[] durations) {
        long[] data = new long[durations.length * BLOCKS_DURATION];
        for (int i = 0; i < durations.length; i++) {
            data[i * BLOCKS_DURATION] = durations[i].get(ChronoUnit.MONTHS);
            data[i * BLOCKS_DURATION + 1] = durations[i].get(ChronoUnit.DAYS);
            data[i * BLOCKS_DURATION + 2] = durations[i].get(ChronoUnit.SECONDS);
            data[i * BLOCKS_DURATION + 3] = durations[i].get(ChronoUnit.NANOS);
        }
        TemporalHeader header = new TemporalHeader(TemporalType.TEMPORAL_DURATION.temporalType);
        byte[] bytes = DynamicArrayStore.encodeFromNumbers(data, DynamicArrayStore.TEMPORAL_HEADER_SIZE);
        header.writeArrayHeaderTo(bytes);
        return bytes;
    }

    public static ArrayValue decodeTemporalArray(TemporalHeader header, byte[] data) {
        byte[] dataHeader = PropertyType.ARRAY.readDynamicRecordHeader(data);
        byte[] dataBody = new byte[data.length - dataHeader.length];
        System.arraycopy(data, dataHeader.length, dataBody, 0, dataBody.length);
        Value dataValue = DynamicArrayStore.getRightArray(dataHeader, dataBody);
        return find(header.temporalType).decodeArray(dataValue);
    }

    private final int temporalType;
    private final String name;

    TemporalType(int temporalType, String name) {
        this.temporalType = temporalType;
        this.name = name;
    }

    public abstract Value decodeForTemporal(long[] valueBlocks, int offset);

    public abstract int calculateNumberOfBlocksUsedForTemporal(long firstBlock);

    public abstract ArrayValue decodeArray(Value dataValue);

    public String getName() {
        return name;
    }
}
