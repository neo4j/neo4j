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
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
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
import org.neo4j.exceptions.TemporalParseException;
import org.neo4j.internal.batchimport.input.Groups;
import org.neo4j.internal.batchimport.input.InputException;
import org.neo4j.values.storable.ArrayValue;
import org.neo4j.values.storable.DateTimeValue;
import org.neo4j.values.storable.DateValue;
import org.neo4j.values.storable.DurationValue;
import org.neo4j.values.storable.LocalDateTimeValue;
import org.neo4j.values.storable.LocalTimeValue;
import org.neo4j.values.storable.PointValue;
import org.neo4j.values.storable.TimeValue;
import org.neo4j.values.storable.Values;

/**
 * The data chunk to be stuck to a Parquet reader.
 * One chunk equals one file.
 */
class ParquetDataInputChunk implements ParquetInputChunk {
    private ParquetData parquetDataFile;
    private Groups groups;
    private Supplier<ZoneId> defaultTimezoneSupplier;
    private String arrayDelimiter;
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

        var columns = parquetDataFile.columns();
        List<Object> readData = iterator.next();
        List<String> labels = new ArrayList<>(filteredLabelsOrTypes);
        StringBuilder idValue = new StringBuilder();
        StringBuilder startIdValue = new StringBuilder();
        StringBuilder endIdValue = new StringBuilder();
        var type = filteredLabelsOrTypes.isEmpty()
                ? ""
                : filteredLabelsOrTypes.iterator().next();
        var isRelationshipEntity = false;
        for (int i = 0; i < readData.size(); i++) {
            var parquetColumn = columns.get(i);
            Object readDatum = readData.get(i);
            if (readDatum == null || isEmptyString(readDatum) || parquetColumn.isIgnoredColumn()) {
                continue;
            }
            // node
            if (parquetColumn.isIdColumn()) {
                if (idType == IdType.STRING
                        && (parquetColumn.columnIdType() == IdType.STRING || parquetColumn.columnIdType() == null)) {
                    if (!idValue.isEmpty()) {
                        idValue.append(ParquetInput.DELIMITER);
                    }
                    idValue.append(resolveIdByType(readDatum, null));
                } else if (idType == IdType.INTEGER || parquetColumn.columnIdType() == IdType.INTEGER) {
                    entityToHydrate.id(resolveIdByType(readDatum, parquetColumn.columnIdType()), nodeIdGroup);
                } else {
                    entityToHydrate.id((Long) resolveIdByType(readDatum, parquetColumn.columnIdType()));
                }
                boolean isActualIdColumn = idType == IdType.ACTUAL && parquetColumn.isIdColumn();
                if (!isActualIdColumn && parquetColumn.hasPropertyName()) {
                    entityToHydrate.property(parquetColumn.propertyName(), convertType(readDatum, parquetColumn));
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
                        entityToHydrate.property(entry.getKey(), converted);
                    }
                } else {
                    var converted = convertType(readDatum, parquetColumn);
                    if (converted == null) {
                        // Skip null entries, as those should not be added as properties
                        continue;
                    }
                    entityToHydrate.property(parquetColumn.propertyName(), converted);
                }
            }
            // relationship
            if (parquetColumn.isStartId()) {
                if (idType == IdType.STRING
                        && (parquetColumn.relationshipColumnIdType(groups) == IdType.STRING
                                || parquetColumn.relationshipColumnIdType(groups) == null)) {
                    if (!startIdValue.isEmpty()) {
                        startIdValue.append(ParquetInput.DELIMITER);
                    }
                    startIdValue.append(resolveIdByType(readDatum, null));
                } else {
                    entityToHydrate.startId(resolveIdByType(readDatum, null), relationshipStartIdGroup);
                }
                isRelationshipEntity = true;
            }
            if (parquetColumn.isEndId()) {
                if (idType == IdType.STRING
                        && (parquetColumn.relationshipColumnIdType(groups) == IdType.STRING
                                || parquetColumn.relationshipColumnIdType(groups) == null)) {
                    if (!endIdValue.isEmpty()) {
                        endIdValue.append(ParquetInput.DELIMITER);
                    }
                    endIdValue.append(resolveIdByType(readDatum, null));
                } else {
                    entityToHydrate.endId(resolveIdByType(readDatum, null), relationshipEndIdGroup);
                }
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
        if (idType == IdType.STRING && !idValue.isEmpty()) {
            entityToHydrate.id(idValue.toString(), nodeIdGroup);
        }
        if (idType == IdType.STRING && !startIdValue.isEmpty()) {
            entityToHydrate.startId(resolveIdByType(startIdValue.toString(), null), relationshipStartIdGroup);
        }
        if (idType == IdType.STRING && !endIdValue.isEmpty()) {
            entityToHydrate.endId(resolveIdByType(endIdValue.toString(), null), relationshipEndIdGroup);
        }
        entityToHydrate.endOfEntity();
        return true;
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
                case DATE -> object instanceof Number number
                        ? DateValue.epochDate(number.intValue())
                        : DateValue.parse(object.toString());
                case TIME -> object instanceof Number number
                        ? TimeValue.time(number.longValue(), ZoneOffset.UTC)
                        : TimeValue.parse(object.toString(), parquetColumn.getTimezone(defaultTimezoneSupplier), null);
                case DATE_TIME -> DateTimeValue.parse(
                        object.toString(), parquetColumn.getTimezone(defaultTimezoneSupplier), null);
                case LOCAL_TIME -> LocalTimeValue.parse(object.toString());
                case LOCAL_DATE_TIME -> {
                    if (object instanceof Long) {
                        yield LocalDateTimeValue.localDateTime((Long) object / 1000000L, 0);
                    } else {
                        try {
                            yield LocalDateTimeValue.parse(object.toString());
                        } catch (TemporalParseException e) {
                            // this could happen if the column type is adjusted to UTC (with zone) but the column header
                            // defines this just as a localdatetime
                            yield LocalDateTimeValue.localDateTime(
                                    DateTimeValue.parse(object.toString(), () -> ZoneId.of(ZoneOffset.UTC.getId()))
                                            .asObjectCopy()
                                            .toLocalDateTime());
                        }
                    }
                }
                case DURATION -> DurationValue.parse(object.toString());
                case INT -> Integer.valueOf(object.toString());
                case SHORT -> Short.valueOf(object.toString());
                case STRING -> object.toString();
                case LONG -> Long.valueOf(object.toString());
                case BYTE -> Byte.parseByte(object.toString());
                case DOUBLE -> Double.parseDouble(object.toString());
                case FLOAT -> Float.parseFloat(object.toString());
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
        var probeConversion = valueMapper.apply(0).getClass();
        Object[] values = (Object[]) Array.newInstance(probeConversion, size);
        for (int i = 0; i < size; i++) {
            values[i] = valueMapper.apply(i);
        }
        return Values.arrayValue(values, true);
    }

    private boolean isEmptyString(Object object) {
        return object instanceof String stringValue && stringValue.isEmpty();
    }

    private Object resolveIdByType(Object id, IdType columnIdType) {
        if (id instanceof String stringId) {
            return stringId;
        } else if (id instanceof Long longId) {
            return longId;
        } else if (id instanceof Integer intId) {
            if (columnIdType == IdType.INTEGER) {
                return intId;
            }
            return intId.longValue();
        }

        throw new IllegalArgumentException("Cannot convert id of type " + id.getClass());
    }

    private Collection<String> filterEmptyLabelsAndTrim(Collection<String> labels) {
        return labels.stream().filter(s -> !s.isEmpty()).map(String::trim).collect(Collectors.toSet());
    }

    private Collection<String> readLabelsFromEntry(Object readDatum) {
        return labelCache.computeIfAbsent(
                readDatum,
                (read) -> filterEmptyLabelsAndTrim(Arrays.asList(read.toString().split(arrayDelimiter))));
    }
}
