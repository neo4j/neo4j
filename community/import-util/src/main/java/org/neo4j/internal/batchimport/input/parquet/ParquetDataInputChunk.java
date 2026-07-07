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
package org.neo4j.internal.batchimport.input.parquet;

import java.io.IOException;
import java.lang.reflect.Array;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.OffsetDateTime;
import java.time.OffsetTime;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.function.IntFunction;
import java.util.function.Supplier;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import org.apache.parquet.schema.LogicalTypeAnnotation;
import org.neo4j.batchimport.api.input.Group;
import org.neo4j.batchimport.api.input.IdType;
import org.neo4j.batchimport.api.input.InputEntityVisitor;
import org.neo4j.csv.reader.VectorExtractor;
import org.neo4j.exceptions.TemporalParseException;
import org.neo4j.graphdb.Vector;
import org.neo4j.internal.batchimport.input.Groups;
import org.neo4j.internal.batchimport.input.InputException;
import org.neo4j.internal.helpers.Numbers;
import org.neo4j.values.storable.ArrayValue;
import org.neo4j.values.storable.DateTimeValue;
import org.neo4j.values.storable.DateValue;
import org.neo4j.values.storable.DurationValue;
import org.neo4j.values.storable.LocalDateTimeValue;
import org.neo4j.values.storable.LocalTimeValue;
import org.neo4j.values.storable.PointValue;
import org.neo4j.values.storable.TimeValue;
import org.neo4j.values.storable.Value;
import org.neo4j.values.storable.Values;
import org.neo4j.values.storable.VectorValue;

/**
 * The data chunk to be stuck to a Parquet reader.
 * One chunk equals one file.
 */
class ParquetDataInputChunk implements ParquetInputChunk {

    private ParquetData parquetDataFile;
    private Groups groups;
    private Supplier<ZoneId> defaultTimezoneSupplier;
    private String arrayDelimiter;
    private String vectorDelimiter;
    private IdType idType;
    private Iterator<List<Object>> iterator;
    private Collection<String> filteredLabelsOrTypes;
    private final Map<Object, Collection<String>> labelCache = new HashMap<>();
    private Group nodeIdGroup;
    private Group relationshipStartIdGroup;
    private Group relationshipEndIdGroup;

    private static final ArrayValue EMPTY_POINT_ARRAY = Values.arrayValue(new PointValue[0], false);
    private static final ArrayValue EMPTY_DATE_ARRAY = Values.arrayValue(new LocalDate[0], false);
    private static final ArrayValue EMPTY_TIME_ARRAY = Values.arrayValue(new OffsetTime[0], false);
    private static final ArrayValue EMPTY_DATETIME_ARRAY = Values.arrayValue(new ZonedDateTime[0], false);
    private static final ArrayValue EMPTY_LOCALTIME_ARRAY = Values.arrayValue(new LocalTime[0], false);
    private static final ArrayValue EMPTY_LOCALDATETIME_ARRAY = Values.arrayValue(new LocalDateTime[0], false);
    private static final ArrayValue EMPTY_DURATION_ARRAY = Values.arrayValue(new DurationValue[0], false);

    @Override
    public boolean readWith(ParquetDataReader reader) {
        try {
            iterator = reader.next();
            if (iterator == null) {
                return false;
            }
            // set up metadata for this reader to avoid repeated parsing those in the reading step
            parquetDataFile = reader.getParquetDataFile();
            groups = reader.getGroups();
            defaultTimezoneSupplier = reader.getDefaultTimezoneSupplier();
            arrayDelimiter = Pattern.quote(reader.getArrayDelimiter());
            vectorDelimiter = Pattern.quote(reader.getVectorDelimiter());
            idType = reader.getIdType();
            filteredLabelsOrTypes = filterEmptyLabelsAndTrim(parquetDataFile.labelsOrType());
            if (parquetDataFile.entityType() == EntityType.NODE) {
                nodeIdGroup = groups.get(parquetDataFile.groupName());
                relationshipStartIdGroup = null;
                relationshipEndIdGroup = null;
            } else {
                nodeIdGroup = null;
                relationshipStartIdGroup = groups.get(parquetDataFile.relationshipStartIdGroupName());
                relationshipEndIdGroup = groups.get(parquetDataFile.relationshipEndIdGroupName());
            }
            return true;
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void close() throws IOException {}

    @Override
    public boolean next(InputEntityVisitor entityToHydrate) throws IOException {
        if (iterator == null || !iterator.hasNext()) {
            return false;
        }

        List<ParquetColumn> columns = parquetDataFile.columns();
        List<Object> readData = iterator.next();
        List<String> labels = new ArrayList<>(filteredLabelsOrTypes);

        List<Object> idValues = new ArrayList<>();
        List<Object> startIdValues = new ArrayList<>();
        List<Object> endIdValues = new ArrayList<>();

        int startIdTypeIndex = 0;
        int endIdTypeIndex = 0;

        String type = filteredLabelsOrTypes.isEmpty()
                ? ""
                : filteredLabelsOrTypes.iterator().next();
        boolean isRelationshipEntity = false;
        for (int i = 0; i < readData.size(); i++) {
            var parquetColumn = columns.get(i);
            Object readDatum = readData.get(i);
            if (readDatum == null || isEmptyString(readDatum) || parquetColumn.isIgnoredColumn()) {
                continue;
            }
            // node
            if (parquetColumn.isIdColumn()) {
                idValues.add(resolveIdByType(readDatum, parquetColumn.columnIdType(), idType));
                if (idType != IdType.ACTUAL && parquetColumn.hasPropertyName()) {
                    entityToHydrate.property(parquetColumn.propertyName(), convertType(readDatum, parquetColumn), true);
                }
            }
            if (parquetColumn.isLabelColumn()) {
                labels.addAll(readLabelsFromEntry(readDatum));
            }
            // common
            if (parquetColumn.hasPropertyName()
                    && parquetColumn.logicalColumnType() == ParquetLogicalColumnType.PROPERTY) {
                if (readDatum instanceof Map rawDataMap) {
                    Map<String, Object> dataMap = (Map<String, Object>) rawDataMap;
                    for (Map.Entry<String, Object> entry : dataMap.entrySet()) {
                        var converted = convertType(entry.getValue(), parquetColumn);
                        if (converted == null) {
                            // Skip null entries, as those should not be added as properties
                            continue;
                        }
                        entityToHydrate.property(entry.getKey(), converted, parquetColumn.isIdentifier());
                    }
                } else {
                    var converted = convertType(readDatum, parquetColumn);
                    if (converted == null) {
                        // Skip null entries, as those should not be added as properties
                        continue;
                    }
                    entityToHydrate.property(parquetColumn.propertyName(), converted, parquetColumn.isIdentifier());
                }
            }
            // relationship
            if (parquetColumn.isStartId()) {
                startIdValues.add(resolveIdByType(
                        readDatum, parquetColumn.relationshipColumnIdType(groups, startIdTypeIndex++), idType));
                isRelationshipEntity = true;
            }
            if (parquetColumn.isEndId()) {
                endIdValues.add(resolveIdByType(
                        readDatum, parquetColumn.relationshipColumnIdType(groups, endIdTypeIndex++), idType));
                isRelationshipEntity = true;
            }
            if (parquetColumn.isType()) {
                if (readDatum instanceof String typeColumnData && !typeColumnData.isBlank()) {
                    type = typeColumnData;
                }
            }
        }
        if (!isRelationshipEntity && !labels.isEmpty()) {
            entityToHydrate.labels(labels.toArray(new String[] {}));
        }
        if (isRelationshipEntity && type != null && !type.isBlank()) {
            entityToHydrate.type(type);
        }
        if (!idValues.isEmpty()) {
            var id = convertIdValues(idValues);
            if (idType == IdType.ACTUAL) {
                entityToHydrate.id((Long) id);
            } else {
                entityToHydrate.id(id, nodeIdGroup);
            }
        }
        if (!startIdValues.isEmpty()) {
            entityToHydrate.startId(convertIdValues(startIdValues), relationshipStartIdGroup);
        }
        if (!endIdValues.isEmpty()) {
            entityToHydrate.endId(convertIdValues(endIdValues), relationshipEndIdGroup);
        }
        entityToHydrate.endOfEntity();
        return true;
    }

    private static Object convertIdValues(List<Object> idValues) {
        if (idValues.size() == 1) {
            return idValues.getFirst();
        } else {
            var idValue = new StringBuilder();
            for (var value : idValues) {
                if (!idValue.isEmpty()) {
                    idValue.append(ParquetInput.DELIMITER);
                }
                idValue.append(value);
            }
            return idValue.toString();
        }
    }

    private Object convertType(Object object, ParquetColumn parquetColumn) {
        try {
            if (parquetColumn.isRaw() && parquetColumn.primitiveType().getLogicalTypeAnnotation() == null) {
                return object;
            }

            // for now there is only support for String-based arrays
            if (parquetColumn.isArray() && !(object instanceof List)) {
                String[] parts = object.toString().split(arrayDelimiter);
                ParquetColumn nonArrayType = parquetColumn.withoutArray();
                return toArrayValue(parts, nonArrayType);
            } else if (parquetColumn.columnType() == ParquetColumnType.VECTOR) {
                return convertVectorType(object, parquetColumn);
            } else if (object instanceof List<?> listValue) {
                return toArrayValue(listValue, parquetColumn.withoutArray());
            }

            return switch (parquetColumn.columnType()) {
                case POINT -> {
                    if (parquetColumn.hasConfiguration()) {
                        yield PointValue.parse(
                                object.toString(), PointValue.parseHeaderInformation(parquetColumn.rawConfiguration()));
                    }
                    yield PointValue.parse(object.toString());
                }
                // Temporal cases short-circuit when the element already is the target java.time type (from
                // list/struct reading) or its Value wrapper (scalar reading). Anything else - including a local
                // temporal that must be promoted to a zoned one using the configured timezone - falls through to
                // the original epoch/string parsing.
                case DATE ->
                    switch (object) {
                        case DateValue dateValue -> dateValue;
                        case LocalDate localDate -> DateValue.date(localDate);
                        case Number epochDay -> DateValue.epochDate(epochDay.intValue());
                        default -> DateValue.parse(object.toString());
                    };
                case TIME ->
                    switch (object) {
                        case TimeValue timeValue -> timeValue;
                        case OffsetTime offsetTime -> TimeValue.time(offsetTime);
                        case Number nanosOfDay -> TimeValue.time(nanosOfDay.longValue(), ZoneOffset.UTC);
                        default ->
                            TimeValue.parse(
                                    object.toString(), parquetColumn.getTimezone(defaultTimezoneSupplier), null);
                    };
                case DATE_TIME ->
                    switch (object) {
                        case DateTimeValue dateTimeValue -> dateTimeValue;
                        case OffsetDateTime offsetDateTime -> DateTimeValue.datetime(offsetDateTime);
                        case ZonedDateTime zonedDateTime -> DateTimeValue.datetime(zonedDateTime);
                        case Number epochMicros ->
                            DateTimeValue.datetime(OffsetDateTime.ofInstant(
                                    Instant.ofEpochSecond(
                                            epochMicros.longValue() / 1_000_000L,
                                            (epochMicros.longValue() % 1_000_000L) * 1_000L),
                                    ZoneOffset.UTC));
                        default ->
                            DateTimeValue.parse(
                                    object.toString(), parquetColumn.getTimezone(defaultTimezoneSupplier), null);
                    };
                case LOCAL_TIME ->
                    switch (object) {
                        case LocalTimeValue localTimeValue -> localTimeValue;
                        case LocalTime localTime -> LocalTimeValue.localTime(localTime);
                        case Number nanoOfDay -> LocalTimeValue.localTime(nanoOfDay.longValue());
                        default -> LocalTimeValue.parse(object.toString());
                    };
                case LOCAL_DATE_TIME ->
                    switch (object) {
                        case LocalDateTimeValue localDateTimeValue -> localDateTimeValue;
                        case LocalDateTime localDateTime -> LocalDateTimeValue.localDateTime(localDateTime);
                        case Number epochMicros ->
                            LocalDateTimeValue.localDateTime(
                                    epochMicros.longValue() / 1_000_000L,
                                    (epochMicros.longValue() % 1_000_000L) * 1_000L);
                        default -> {
                            try {
                                yield LocalDateTimeValue.parse(object.toString());
                            } catch (TemporalParseException e) {
                                // this could happen if the column type is adjusted to UTC (with zone) but the column
                                // header defines this just as a localdatetime
                                yield LocalDateTimeValue.localDateTime(
                                        DateTimeValue.parse(object.toString(), () -> ZoneId.of(ZoneOffset.UTC.getId()))
                                                .asObjectCopy()
                                                .toLocalDateTime());
                            }
                        }
                    };
                case DURATION ->
                    object instanceof DurationValue durationValue
                            ? durationValue
                            : DurationValue.parse(object.toString());
                case INT ->
                    object instanceof Number number
                            ? Numbers.safeCastLongToInt(number.longValue())
                            : Integer.valueOf(object.toString());
                case SHORT ->
                    object instanceof Number number
                            ? Numbers.safeCastLongToShort(number.longValue())
                            : Short.valueOf(object.toString());
                case STRING -> object.toString();
                case LONG -> object instanceof Number number ? number.longValue() : Long.valueOf(object.toString());
                case BYTE ->
                    object instanceof Number number
                            ? Numbers.safeCastLongToByte(number.longValue())
                            : Byte.parseByte(object.toString());
                // FLOAT/DOUBLE only short-circuit on the matching Java type. Cross-type widening (Float -> :double)
                // goes through the toString roundtrip so the decimal representation is preserved
                // (1.01f -> "1.01" -> 1.01d, not (double) 1.01f = 1.00999999...).
                case DOUBLE ->
                    object instanceof Double doubleValue ? doubleValue : Double.parseDouble(object.toString());
                case FLOAT -> object instanceof Float floatValue ? floatValue : Float.parseFloat(object.toString());
                default -> object;
            };
        } catch (RuntimeException e) {
            throw new InputException(
                    "could not convert %s to %s: %s"
                            .formatted(object.toString(), parquetColumn.columnType(), e.getMessage()),
                    e);
        }
    }

    private ArrayValue toArrayValue(String[] parts, ParquetColumn nonArrayType) {
        if (parts.length == 0) {
            return null;
        }
        return createTypedArrayValue(parts.length, i -> convertType(parts[i], nonArrayType));
    }

    private ArrayValue toArrayValue(List<?> listValue, ParquetColumn parquetColumn) {
        if (listValue.isEmpty()) {
            return getEmptyArrayValue(parquetColumn);
        }
        return createTypedArrayValue(listValue.size(), i -> convertType(listValue.get(i), parquetColumn));
    }

    private static ArrayValue getEmptyArrayValue(ParquetColumn parquetColumn) {
        // For RAW or unspecified column types, infer from logical type annotation or primitive type
        if (parquetColumn.columnType() == ParquetColumnType.RAW || parquetColumn.columnType() == null) {
            return getEmptyArrayValueFromSchema(parquetColumn);
        }

        return switch (parquetColumn.columnType()) {
            case BYTE -> Values.EMPTY_BYTE_ARRAY;
            case SHORT -> Values.EMPTY_SHORT_ARRAY;
            case INT -> Values.EMPTY_INT_ARRAY;
            case LONG -> Values.EMPTY_LONG_ARRAY;
            case FLOAT -> Values.EMPTY_FLOAT_ARRAY;
            case DOUBLE -> Values.EMPTY_DOUBLE_ARRAY;
            case BOOLEAN -> Values.EMPTY_BOOLEAN_ARRAY;
            case CHAR -> Values.EMPTY_CHAR_ARRAY;
            case STRING -> Values.EMPTY_TEXT_ARRAY;
            case POINT -> EMPTY_POINT_ARRAY;
            case DATE -> EMPTY_DATE_ARRAY;
            case TIME -> EMPTY_TIME_ARRAY;
            case DATE_TIME -> EMPTY_DATETIME_ARRAY;
            case LOCAL_TIME -> EMPTY_LOCALTIME_ARRAY;
            case LOCAL_DATE_TIME -> EMPTY_LOCALDATETIME_ARRAY;
            case DURATION -> EMPTY_DURATION_ARRAY;
            default -> getEmptyArrayValueFromSchema(parquetColumn);
        };
    }

    private static ArrayValue getEmptyArrayValueFromSchema(ParquetColumn parquetColumn) {
        // Try to infer from logical type annotation first
        var logicalType = parquetColumn.logicalTypeAnnotation();
        if (logicalType != null) {
            if (LogicalTypeAnnotation.stringType().equals(logicalType)) {
                return Values.EMPTY_TEXT_ARRAY;
            }
            if (logicalType instanceof LogicalTypeAnnotation.IntLogicalTypeAnnotation) {
                return Values.EMPTY_INT_ARRAY;
            }
            if (LogicalTypeAnnotation.dateType().equals(logicalType)) {
                return EMPTY_DATE_ARRAY;
            }
            if (logicalType instanceof LogicalTypeAnnotation.TimeLogicalTypeAnnotation ts) {
                return ts.isAdjustedToUTC() ? EMPTY_TIME_ARRAY : EMPTY_LOCALTIME_ARRAY;
            }
            if (logicalType instanceof LogicalTypeAnnotation.TimestampLogicalTypeAnnotation ts) {
                return ts.isAdjustedToUTC() ? EMPTY_DATETIME_ARRAY : EMPTY_LOCALDATETIME_ARRAY;
            }
            if (logicalType instanceof LogicalTypeAnnotation.IntervalLogicalTypeAnnotation) {
                return EMPTY_DURATION_ARRAY;
            }
        }

        // Fall back to primitive type
        if (parquetColumn.primitiveType() == null) {
            return null;
        }
        return switch (parquetColumn.primitiveType().getPrimitiveTypeName()) {
            case INT64, INT96 -> Values.EMPTY_LONG_ARRAY;
            case INT32 -> Values.EMPTY_INT_ARRAY;
            case BOOLEAN -> Values.EMPTY_BOOLEAN_ARRAY;
            case BINARY, FIXED_LEN_BYTE_ARRAY -> Values.EMPTY_BYTE_ARRAY;
            case FLOAT -> Values.EMPTY_FLOAT_ARRAY;
            case DOUBLE -> Values.EMPTY_DOUBLE_ARRAY;
        };
    }

    private ArrayValue createTypedArrayValue(int size, IntFunction<Object> valueMapper) {
        // Explicitly typed elements (e.g. a `:date[]` column) are converted to storable Values, but
        // Values.arrayValue only assembles arrays of the raw representations (LocalDate[], Point[], ...),
        // so unwrap each element back to its underlying object before building the array.
        IntFunction<Object> rawValueMapper = i -> unwrapStorableValue(valueMapper.apply(i));
        var probeConversion = rawValueMapper.apply(0).getClass();
        Object[] values = (Object[]) Array.newInstance(probeConversion, size);
        for (int i = 0; i < size; i++) {
            values[i] = rawValueMapper.apply(i);
        }
        return Values.arrayValue(values, true);
    }

    private static Object unwrapStorableValue(Object value) {
        return value instanceof Value storableValue ? storableValue.asObjectCopy() : value;
    }

    private VectorValue convertVectorType(Object object, ParquetColumn parquetColumn) {
        final List<?> parts = object instanceof List<?> listValue
                ? listValue
                : Arrays.asList(object.toString().split(vectorDelimiter));

        final var headerInformation = VectorExtractor.parseHeaderInformation(parquetColumn.configuration());
        final var dimensions = headerInformation.getDimensions();
        final var coordinateType = headerInformation.getCoordinateType();

        if (dimensions != parts.size()) {
            throw new IllegalArgumentException("Header specified %d dimensions, but vector has %d dimensions: %s"
                    .formatted(dimensions, parts.size(), object));
        }

        return switch (coordinateType) {
            case Vector.CoordinateType.INTEGER8 -> {
                final var innerColumn = parquetColumn.withColumnType(ParquetColumnType.resolve("byte"));
                final byte[] values = new byte[dimensions];
                for (int i = 0; i < dimensions; i++) {
                    values[i] = (byte) convertType(parts.get(i), innerColumn);
                }
                yield Values.int8Vector(values);
            }
            case Vector.CoordinateType.INTEGER16 -> {
                final var innerColumn = parquetColumn.withColumnType(ParquetColumnType.resolve("short"));
                short[] values = new short[dimensions];
                for (int i = 0; i < dimensions; i++) {
                    values[i] = (short) convertType(parts.get(i), innerColumn);
                }
                yield Values.int16Vector(values);
            }
            case Vector.CoordinateType.INTEGER32 -> {
                final var innerColumn = parquetColumn.withColumnType(ParquetColumnType.resolve("int"));
                int[] values = new int[dimensions];
                for (int i = 0; i < dimensions; i++) {
                    values[i] = (int) convertType(parts.get(i), innerColumn);
                }
                yield Values.int32Vector(values);
            }
            case Vector.CoordinateType.INTEGER64 -> {
                final var innerColumn = parquetColumn.withColumnType(ParquetColumnType.resolve("long"));
                long[] values = new long[dimensions];
                for (int i = 0; i < dimensions; i++) {
                    values[i] = (long) convertType(parts.get(i), innerColumn);
                }
                yield Values.int64Vector(values);
            }
            case Vector.CoordinateType.FLOAT32 -> {
                final var innerColumn = parquetColumn.withColumnType(ParquetColumnType.resolve("float"));
                float[] values = new float[dimensions];
                for (int i = 0; i < dimensions; i++) {
                    values[i] = (float) convertType(parts.get(i), innerColumn);
                }
                yield Values.float32Vector(values);
            }
            case Vector.CoordinateType.FLOAT64 -> {
                final var innerColumn = parquetColumn.withColumnType(ParquetColumnType.resolve("double"));
                double[] values = new double[dimensions];
                for (int i = 0; i < dimensions; i++) {
                    values[i] = (double) convertType(parts.get(i), innerColumn);
                }
                yield Values.float64Vector(values);
            }
        };
    }

    private Collection<String> readLabelsFromEntry(Object readDatum) {
        return labelCache.computeIfAbsent(
                readDatum,
                (read) -> filterEmptyLabelsAndTrim(Arrays.asList(read.toString().split(arrayDelimiter))));
    }

    private static boolean isEmptyString(Object object) {
        return object instanceof String stringValue && stringValue.isEmpty();
    }

    private static Object resolveIdByType(Object id, IdType columnIdType, IdType globalIdType) {
        boolean targetIsString =
                columnIdType == IdType.STRING || (columnIdType == null && globalIdType == IdType.STRING);
        if (id instanceof String stringId) {
            return stringId;
        } else if (id instanceof Long longId) {
            if (targetIsString) {
                return String.valueOf(longId);
            } else {
                return longId;
            }
        } else if (id instanceof Integer intId) {
            if (targetIsString) {
                return String.valueOf(intId);
            } else if (columnIdType == IdType.INTEGER) {
                return intId;
            } else {
                return intId.longValue();
            }
        }

        throw new IllegalArgumentException("Cannot convert id of type " + id.getClass());
    }

    private static Collection<String> filterEmptyLabelsAndTrim(Collection<String> labels) {
        return labels.stream().filter(s -> !s.isEmpty()).map(String::trim).collect(Collectors.toSet());
    }
}
