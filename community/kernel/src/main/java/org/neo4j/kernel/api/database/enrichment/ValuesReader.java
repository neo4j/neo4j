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
package org.neo4j.kernel.api.database.enrichment;

import static java.time.ZoneOffset.UTC;
import static org.neo4j.values.storable.Values.dateTimeArray;
import static org.neo4j.values.storable.Values.localDateTimeArray;
import static org.neo4j.values.storable.Values.localTimeArray;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.OffsetTime;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.util.List;
import org.eclipse.collections.api.map.primitive.ImmutableByteObjectMap;
import org.eclipse.collections.impl.factory.primitive.ByteObjectMaps;
import org.neo4j.values.AnyValue;
import org.neo4j.values.storable.BooleanArray;
import org.neo4j.values.storable.BooleanValue;
import org.neo4j.values.storable.ByteArray;
import org.neo4j.values.storable.ByteValue;
import org.neo4j.values.storable.CharArray;
import org.neo4j.values.storable.CharValue;
import org.neo4j.values.storable.CoordinateReferenceSystem;
import org.neo4j.values.storable.DateArray;
import org.neo4j.values.storable.DateTimeArray;
import org.neo4j.values.storable.DateTimeValue;
import org.neo4j.values.storable.DateValue;
import org.neo4j.values.storable.DoubleArray;
import org.neo4j.values.storable.DoubleValue;
import org.neo4j.values.storable.DurationArray;
import org.neo4j.values.storable.DurationValue;
import org.neo4j.values.storable.FloatArray;
import org.neo4j.values.storable.FloatValue;
import org.neo4j.values.storable.IntArray;
import org.neo4j.values.storable.IntValue;
import org.neo4j.values.storable.LocalDateTimeArray;
import org.neo4j.values.storable.LocalDateTimeValue;
import org.neo4j.values.storable.LocalTimeArray;
import org.neo4j.values.storable.LocalTimeValue;
import org.neo4j.values.storable.LongArray;
import org.neo4j.values.storable.LongValue;
import org.neo4j.values.storable.NoValue;
import org.neo4j.values.storable.PointArray;
import org.neo4j.values.storable.PointValue;
import org.neo4j.values.storable.ShortArray;
import org.neo4j.values.storable.ShortValue;
import org.neo4j.values.storable.StringArray;
import org.neo4j.values.storable.StringValue;
import org.neo4j.values.storable.TextArray;
import org.neo4j.values.storable.TextValue;
import org.neo4j.values.storable.TimeArray;
import org.neo4j.values.storable.TimeValue;
import org.neo4j.values.storable.TimeZones;
import org.neo4j.values.storable.Value;
import org.neo4j.values.storable.Values;
import org.neo4j.values.virtual.ListValue;
import org.neo4j.values.virtual.MapValue;
import org.neo4j.values.virtual.NodeValue;
import org.neo4j.values.virtual.RelationshipValue;
import org.neo4j.values.virtual.VirtualNodeValue;
import org.neo4j.values.virtual.VirtualPathValue;
import org.neo4j.values.virtual.VirtualRelationshipValue;
import org.neo4j.values.virtual.VirtualValues;

/**
 * Factory for reading {@link Value} objects from some {@link ByteBuffer}
 */
public enum ValuesReader {
    // STORAGE VALUES
    NO_VALUE((byte) 1, NoValue.class, (unused) -> Values.NO_VALUE),
    BOOLEAN((byte) 2, BooleanValue.class, ValuesReader::readBoolean),
    BOOLEAN_ARRAY((byte) 3, BooleanArray.class, ValuesReader::readBooleanArray),
    BYTE((byte) 4, ByteValue.class, ValuesReader::readByte),
    BYTE_ARRAY((byte) 5, ByteArray.class, ValuesReader::readByteArray),
    SHORT((byte) 6, ShortValue.class, ValuesReader::readShort),
    SHORT_ARRAY((byte) 7, ShortArray.class, ValuesReader::readShortArray),
    INT((byte) 8, IntValue.class, ValuesReader::readInt),
    INT_ARRAY((byte) 9, IntArray.class, ValuesReader::readIntArray),
    LONG((byte) 10, LongValue.class, ValuesReader::readLong),
    LONG_ARRAY((byte) 11, LongArray.class, ValuesReader::readLongArray),
    FLOAT((byte) 12, FloatValue.class, ValuesReader::readFloat),
    FLOAT_ARRAY((byte) 13, FloatArray.class, ValuesReader::readFloatArray),
    DOUBLE((byte) 14, DoubleValue.class, ValuesReader::readDouble),
    DOUBLE_ARRAY((byte) 15, DoubleArray.class, ValuesReader::readDoubleArray),
    STRING((byte) 16, StringValue.class, ValuesReader::readString),
    STRING_ARRAY((byte) 17, StringArray.class, ValuesReader::readStringArray),
    CHAR((byte) 18, CharValue.class, ValuesReader::readChar),
    CHAR_ARRAY((byte) 19, CharArray.class, ValuesReader::readCharArray),
    POINT((byte) 20, PointValue.class, ValuesReader::readPoint),
    POINT_ARRAY((byte) 21, PointArray.class, ValuesReader::readPointArray),
    DURATION((byte) 22, DurationValue.class, ValuesReader::readDuration),
    DURATION_ARRAY((byte) 23, DurationArray.class, ValuesReader::readDurationArray),
    DATE((byte) 24, DateValue.class, ValuesReader::readDate),
    DATE_ARRAY((byte) 25, DateArray.class, ValuesReader::readDateArray),
    TIME((byte) 26, TimeValue.class, ValuesReader::readTime),
    TIME_ARRAY((byte) 27, TimeArray.class, ValuesReader::readTimeArray),
    DATE_TIME((byte) 28, DateTimeValue.class, ValuesReader::readDateTime),
    DATE_TIME_ARRAY((byte) 29, DateTimeArray.class, ValuesReader::readDateTimeArray),
    LOCAL_TIME((byte) 30, LocalTimeValue.class, ValuesReader::readLocalTime),
    LOCAL_TIME_ARRAY((byte) 31, LocalTimeArray.class, ValuesReader::readLocalTimeArray),
    LOCAL_DATE_TIME((byte) 32, LocalDateTimeValue.class, ValuesReader::readLocalDateTime),
    LOCAL_DATE_TIME_ARRAY((byte) 33, LocalDateTimeArray.class, ValuesReader::readLocalDateTimeArray),
    // ANY VALUES - see also PackStreamValueWriter/PackStreamValueReader in bolt module
    PATH((byte) 34, VirtualPathValue.class, ValuesReader::readPath),
    NODE((byte) 35, VirtualNodeValue.class, ValuesReader::readNode),
    RELATIONSHIP((byte) 36, VirtualRelationshipValue.class, ValuesReader::readRelationship),
    LIST((byte) 37, ListValue.class, ValuesReader::readList),
    MAP((byte) 38, MapValue.class, ValuesReader::readMap);

    public static final ImmutableByteObjectMap<ValuesReader> BY_ID =
            ByteObjectMaps.immutable.from(List.of(ValuesReader.values()), ValuesReader::id, v -> v);

    private final byte id;
    private final Class<?> valueClass;
    private final ValueReader<?> reader;

    <T extends AnyValue> ValuesReader(byte id, Class<? extends T> valueClass, ValueReader<T> reader) {
        this.id = id;
        this.valueClass = valueClass;
        this.reader = reader;
    }

    public AnyValue read(ByteBuffer buffer) {
        return reader.read(buffer);
    }

    public static ValuesReader forValueClass(Class<? extends AnyValue> type) {
        for (ValuesReader valuesReader : BY_ID) {
            if (valuesReader.valueClass.isAssignableFrom(type)) {
                return valuesReader;
            }
        }
        throw new IllegalArgumentException("Unsupported value type: " + type);
    }

    public static AnyValue from(ByteBuffer buffer) {
        final var type = buffer.get();
        final var reader = BY_ID.get(type);
        if (reader == null) {
            throw new IllegalArgumentException("Unsupported value type: " + type);
        }

        return reader.read(buffer);
    }

    public static String readJavaString(ByteBuffer buffer) {
        final var bytes = new byte[buffer.getInt()];
        buffer.get(bytes, 0, bytes.length);
        return new String(bytes, StandardCharsets.UTF_8);
    }

    private static ByteValue readByte(ByteBuffer buffer) {
        return Values.byteValue(buffer.get());
    }

    private static ByteArray readByteArray(ByteBuffer buffer) {
        final var length = buffer.getInt();
        final var values = new byte[length];
        for (var i = 0; i < length; i++) {
            values[i] = buffer.get();
        }
        return Values.byteArray(values);
    }

    private static BooleanValue readBoolean(ByteBuffer buffer) {
        return Values.booleanValue(readJavaBoolean(buffer));
    }

    private static BooleanArray readBooleanArray(ByteBuffer buffer) {
        final var length = buffer.getInt();
        final var values = new boolean[length];
        for (var i = 0; i < length; i++) {
            values[i] = readJavaBoolean(buffer);
        }
        return Values.booleanArray(values);
    }

    private static CharValue readChar(ByteBuffer buffer) {
        return Values.charValue(buffer.getChar());
    }

    private static CharArray readCharArray(ByteBuffer buffer) {
        final var length = buffer.getInt();
        final var values = new char[length];
        for (var i = 0; i < length; i++) {
            values[i] = buffer.getChar();
        }
        return Values.charArray(values);
    }

    private static ShortValue readShort(ByteBuffer buffer) {
        return Values.shortValue(buffer.getShort());
    }

    private static ShortArray readShortArray(ByteBuffer buffer) {
        final var length = buffer.getInt();
        final var values = new short[length];
        for (var i = 0; i < length; i++) {
            values[i] = buffer.getShort();
        }
        return Values.shortArray(values);
    }

    private static IntValue readInt(ByteBuffer buffer) {
        return Values.intValue(buffer.getInt());
    }

    private static IntArray readIntArray(ByteBuffer buffer) {
        final var length = buffer.getInt();
        final var values = new int[length];
        for (var i = 0; i < length; i++) {
            values[i] = buffer.getInt();
        }
        return Values.intArray(values);
    }

    private static LongValue readLong(ByteBuffer buffer) {
        return Values.longValue(buffer.getLong());
    }

    private static LongArray readLongArray(ByteBuffer buffer) {
        final var length = buffer.getInt();
        final var values = new long[length];
        for (var i = 0; i < length; i++) {
            values[i] = buffer.getLong();
        }
        return Values.longArray(values);
    }

    private static FloatValue readFloat(ByteBuffer buffer) {
        return Values.floatValue(buffer.getFloat());
    }

    private static FloatArray readFloatArray(ByteBuffer buffer) {
        final var length = buffer.getInt();
        final var values = new float[length];
        for (var i = 0; i < length; i++) {
            values[i] = buffer.getFloat();
        }
        return Values.floatArray(values);
    }

    private static DoubleValue readDouble(ByteBuffer buffer) {
        return Values.doubleValue(buffer.getDouble());
    }

    private static DoubleArray readDoubleArray(ByteBuffer buffer) {
        final var length = buffer.getInt();
        final var values = new double[length];
        for (var i = 0; i < length; i++) {
            values[i] = buffer.getDouble();
        }
        return Values.doubleArray(values);
    }

    private static TextValue readString(ByteBuffer buffer) {
        return Values.stringValue(readJavaString(buffer));
    }

    private static TextArray readStringArray(ByteBuffer buffer) {
        final var length = buffer.getInt();
        final var values = new String[length];
        for (var i = 0; i < length; i++) {
            values[i] = readJavaString(buffer);
        }
        return Values.stringArray(values);
    }

    private static PointValue readPoint(ByteBuffer buffer) {
        final var crs = CoordinateReferenceSystem.get(buffer.getInt());
        final var coordinates = new double[crs.getDimension()];
        for (var i = 0; i < coordinates.length; i++) {
            coordinates[i] = buffer.getDouble();
        }
        return Values.pointValue(crs, coordinates);
    }

    private static PointArray readPointArray(ByteBuffer buffer) {
        final var length = buffer.getInt();
        final var values = new PointValue[length];
        for (var i = 0; i < length; i++) {
            values[i] = readPoint(buffer);
        }
        return Values.pointArray(values);
    }

    private static DurationValue readDuration(ByteBuffer buffer) {
        final var months = buffer.getLong();
        final var days = buffer.getLong();
        final var seconds = buffer.getLong();
        final var nanos = buffer.getInt();
        return DurationValue.duration(months, days, seconds, nanos);
    }

    private static DurationArray readDurationArray(ByteBuffer buffer) {
        final var length = buffer.getInt();
        final var values = new DurationValue[length];
        for (var i = 0; i < length; i++) {
            values[i] = readDuration(buffer);
        }
        return Values.durationArray(values);
    }

    private static DateValue readDate(ByteBuffer buffer) {
        return DateValue.epochDate(buffer.getLong());
    }

    private static DateArray readDateArray(ByteBuffer buffer) {
        final var length = buffer.getInt();
        final var values = new LocalDate[length];
        for (var i = 0; i < length; i++) {
            values[i] = LocalDate.ofEpochDay(buffer.getLong());
        }
        return Values.dateArray(values);
    }

    private static TimeValue readTime(ByteBuffer buffer) {
        return TimeValue.time(readRawTime(buffer));
    }

    private static TimeArray readTimeArray(ByteBuffer buffer) {
        final var length = buffer.getInt();
        final var values = new OffsetTime[length];
        for (var i = 0; i < length; i++) {
            values[i] = readRawTime(buffer);
        }
        return Values.timeArray(values);
    }

    private static DateTimeValue readDateTime(ByteBuffer buffer) {
        final long epocSeconds = buffer.getLong();
        final int nanos = buffer.getInt();
        return DateTimeValue.datetime(epocSeconds, nanos, toZoneId(buffer.getInt()));
    }

    private static DateTimeArray readDateTimeArray(ByteBuffer buffer) {
        final var length = buffer.getInt();
        final var array = new ZonedDateTime[length];
        for (var i = 0; i < length; i++) {
            final var epocSeconds = buffer.getLong();
            final var nanos = buffer.getInt();
            final var z = buffer.getInt();
            array[i] = ZonedDateTime.ofInstant(Instant.ofEpochSecond(epocSeconds, nanos), toZoneId(z));
        }
        return dateTimeArray(array);
    }

    private static LocalTimeValue readLocalTime(ByteBuffer buffer) {
        return LocalTimeValue.localTime(buffer.getLong());
    }

    private static LocalTimeArray readLocalTimeArray(ByteBuffer buffer) {
        final var length = buffer.getInt();
        final var array = new LocalTime[length];
        for (var i = 0; i < length; i++) {
            array[i] = LocalTime.ofNanoOfDay(buffer.getLong());
        }
        return localTimeArray(array);
    }

    private static LocalDateTimeValue readLocalDateTime(ByteBuffer buffer) {
        final var epochSecond = buffer.getLong();
        final var nanos = buffer.getInt();
        return LocalDateTimeValue.localDateTime(epochSecond, nanos);
    }

    private static LocalDateTimeArray readLocalDateTimeArray(ByteBuffer buffer) {
        final var length = buffer.getInt();
        final var array = new LocalDateTime[length];
        for (var i = 0; i < length; i++) {
            final var epochSecond = buffer.getLong();
            final var nanos = buffer.getInt();
            array[i] = LocalDateTime.ofEpochSecond(epochSecond, nanos, UTC);
        }
        return localDateTimeArray(array);
    }

    private static VirtualPathValue readPath(ByteBuffer buffer) {
        final var isDirect = readJavaBoolean(buffer);
        final var nodeCount = buffer.getInt();
        if (isDirect) {
            final var beforePos = buffer.position();
            final var nodes = new NodeValue[nodeCount];
            for (int i = 0; i < nodes.length; i++) {
                nodes[i] = (NodeValue) readNode(buffer);
            }

            final var relationships = new RelationshipValue[nodeCount - 1];
            for (int i = 0; i < relationships.length; i++) {
                relationships[i] = (RelationshipValue) readRelationship(buffer);
            }

            final var payloadSize = buffer.position() - beforePos;
            return VirtualValues.path(nodes, relationships, payloadSize);
        } else {
            final var nodes = new long[nodeCount];
            for (int i = 0; i < nodes.length; i++) {
                nodes[i] = buffer.getLong();
            }

            final var relationships = new long[nodeCount - 1];
            for (int i = 0; i < relationships.length; i++) {
                relationships[i] = buffer.getLong();
            }

            return VirtualValues.pathReference(nodes, relationships);
        }
    }

    private static VirtualNodeValue readNode(ByteBuffer buffer) {
        final var isDirect = readJavaBoolean(buffer);
        final var nodeId = buffer.getLong();
        if (isDirect) {
            final var elementId = readJavaString(buffer);
            final var isDeleted = readJavaBoolean(buffer);
            final var labels = readStringArray(buffer);
            final var properties = readMap(buffer);
            return VirtualValues.nodeValue(nodeId, elementId, labels, properties, isDeleted);
        } else {
            return VirtualValues.node(nodeId);
        }
    }

    private static VirtualRelationshipValue readRelationship(ByteBuffer buffer) {
        final var isDirect = readJavaBoolean(buffer);
        final var relId = buffer.getLong();
        if (isDirect) {
            final var elementId = readJavaString(buffer);
            final var type = readString(buffer);
            final var isDeleted = readJavaBoolean(buffer);

            final var startId = buffer.getLong();
            final var startElementId = readJavaString(buffer);
            final var endId = buffer.getLong();
            final var endElementId = readJavaString(buffer);

            final var properties = readMap(buffer);
            return VirtualValues.relationshipValue(
                    relId,
                    elementId,
                    VirtualValues.node(startId, startElementId),
                    VirtualValues.node(endId, endElementId),
                    type,
                    properties,
                    isDeleted);
        } else {
            return VirtualValues.relationship(relId);
        }
    }

    private static ListValue readList(ByteBuffer buffer) {
        final var values = new AnyValue[buffer.getInt()];
        for (int i = 0; i < values.length; i++) {
            values[i] = from(buffer);
        }
        return VirtualValues.list(values);
    }

    private static MapValue readMap(ByteBuffer buffer) {
        final var entryCount = buffer.getInt();
        final var keys = new String[entryCount];
        final var values = new AnyValue[entryCount];
        for (int i = 0; i < entryCount; i++) {
            keys[i] = readJavaString(buffer);
            values[i] = from(buffer);
        }
        return VirtualValues.map(keys, values);
    }

    private static boolean readJavaBoolean(ByteBuffer buffer) {
        return buffer.get() == 1;
    }

    private static OffsetTime readRawTime(ByteBuffer buffer) {
        final var nanosOfDayUTC = buffer.getLong();
        final var offsetSeconds = buffer.getInt();
        return OffsetTime.ofInstant(Instant.ofEpochSecond(0, nanosOfDayUTC), ZoneOffset.ofTotalSeconds(offsetSeconds));
    }

    private static ZoneId toZoneId(int z) {
        // if lowest bit is set to 1 then it's a shifted zone id
        if ((z & 1) != 0) {
            final String zoneId = TimeZones.map((short) (z >> 1));
            return ZoneId.of(zoneId);
        }
        // otherwise it's a shifted offset seconds value
        // preserve sign-bit for negative offsets
        return ZoneOffset.ofTotalSeconds(z >> 1);
    }

    public byte id() {
        return id;
    }

    @FunctionalInterface
    public interface ValueReader<T extends AnyValue> {
        T read(ByteBuffer buffer);
    }
}
