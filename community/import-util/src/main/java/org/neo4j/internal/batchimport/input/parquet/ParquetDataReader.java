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

import java.io.Closeable;
import java.io.IOException;
import java.nio.ByteOrder;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Supplier;
import org.apache.parquet.ParquetReadOptions;
import org.apache.parquet.column.ColumnDescriptor;
import org.apache.parquet.column.ColumnReader;
import org.apache.parquet.column.impl.ColumnReadStoreImpl;
import org.apache.parquet.example.DummyRecordConverter;
import org.apache.parquet.hadoop.ParquetEmptyBlockException;
import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.hadoop.metadata.ParquetMetadata;
import org.apache.parquet.io.api.GroupConverter;
import org.apache.parquet.schema.GroupType;
import org.apache.parquet.schema.LogicalTypeAnnotation;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.PrimitiveType;
import org.apache.parquet.schema.Type;
import org.neo4j.batchimport.api.input.IdType;
import org.neo4j.internal.batchimport.input.Groups;
import org.neo4j.internal.batchimport.input.InputException;
import org.neo4j.values.storable.DateTimeValue;
import org.neo4j.values.storable.DateValue;
import org.neo4j.values.storable.DurationValue;
import org.neo4j.values.storable.LocalDateTimeValue;
import org.neo4j.values.storable.LocalTimeValue;
import org.neo4j.values.storable.TimeValue;
import org.neo4j.values.storable.Value;

/**
 * There should be a 1:1 match between Reader and file for now.
 */
class ParquetDataReader implements Closeable {

    private final ParquetData parquetDataFile;
    private final Groups groups;
    private final IdType idType;
    private final Supplier<ZoneId> defaultTimezoneSupplier;
    private final String arrayDelimiter;
    private final String vectorDelimiter;
    private final ParquetFileReader metadataReader;
    private final AtomicInteger blockCounter;
    private final MessageType schema;
    private final GroupConverter recordConverter;
    private final String createdBy;
    private final List<ColumnDescriptor> parquetColumns;
    private final ParquetMetadata footer;

    ParquetDataReader(
            ParquetData parquetDataFile,
            Groups groups,
            IdType idType,
            Supplier<ZoneId> defaultTimezoneSupplier,
            String arrayDelimiter,
            String vectorDelimiter) {
        this.parquetDataFile = parquetDataFile;
        this.groups = groups;
        this.idType = idType;
        this.defaultTimezoneSupplier = defaultTimezoneSupplier;
        this.arrayDelimiter = arrayDelimiter;
        this.vectorDelimiter = vectorDelimiter;
        var path = parquetDataFile.file();

        try {
            this.metadataReader = ParquetFileReader.open(
                    ParquetInput.ParquetImportInputFile.of(path),
                    ParquetReadOptions.builder().build());
            var metadata = this.metadataReader.getFileMetaData();
            this.footer = this.metadataReader.getFooter();
            this.schema = metadata.getSchema();
            this.recordConverter = new DummyRecordConverter(this.schema).getRootConverter();
            this.createdBy = metadata.getCreatedBy();

            var columnsToRead = parquetDataFile.columns().stream()
                    .map(column -> column.headerDefinition().parquetColumnName())
                    .toList();
            this.parquetColumns = schema.getColumns().stream()
                    .filter(c -> columnsToRead.contains(c.getPath()[0]))
                    .toList();

            this.blockCounter = new AtomicInteger(0);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public Iterator<List<Object>> next() throws IOException {
        var nextRowGroupIndex = blockCounter.getAndIncrement();
        if (nextRowGroupIndex >= metadataReader.getRowGroups().size()) {
            return null;
        }
        try {
            return new ParquetRowGroupReader(nextRowGroupIndex);
        } catch (ParquetEmptyBlockException e) {
            // This row group does not contain any records, so let's just return an empty iterator
            return Collections.emptyIterator();
        }
    }

    public ParquetData getParquetDataFile() {
        return parquetDataFile;
    }

    public Groups getGroups() {
        return groups;
    }

    public IdType getIdType() {
        return idType;
    }

    public Supplier<ZoneId> getDefaultTimezoneSupplier() {
        return defaultTimezoneSupplier;
    }

    public String getArrayDelimiter() {
        return arrayDelimiter;
    }

    public String getVectorDelimiter() {
        return vectorDelimiter;
    }

    private class ParquetRowGroupReader implements Iterator<List<Object>> {
        private final ParquetFileReader reader;
        private final List<ColumnReader> columnReaders;
        private final long rowCount;
        private long rowIndex;

        ParquetRowGroupReader(int rowGroupIndex) throws IOException {
            var file = ParquetInput.ParquetImportInputFile.of(parquetDataFile.file());
            this.reader = ParquetFileReader.open(
                    file,
                    ParquetDataReader.this.footer,
                    ParquetReadOptions.builder().build(),
                    file.newStream());
            var store = this.reader.readRowGroup(rowGroupIndex);
            this.rowCount = store.getRowCount();

            var columnReadStore = new ColumnReadStoreImpl(
                    store,
                    ParquetDataReader.this.recordConverter,
                    ParquetDataReader.this.schema,
                    ParquetDataReader.this.createdBy);
            this.columnReaders = parquetColumns.stream()
                    .map(columnReadStore::getColumnReader)
                    .toList();
        }

        @Override
        public boolean hasNext() {
            var result = rowIndex < rowCount;
            if (!result) {
                closeUnderlyingReader();
            }
            return result;
        }

        void closeUnderlyingReader() {
            try {
                this.reader.close();
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }

        // Wrapper class for representing maps / structs and their
        // content.
        private static class MapLikeRecord {

            private final String fieldName;
            private Keys keys;
            private Values values;

            private MapLikeRecord(String fieldName) {
                this.fieldName = fieldName;
            }

            private MapLikeRecord(String fieldName, Keys keys, Values values) {
                this.fieldName = fieldName;
                this.keys = keys;
                this.values = values;
            }

            // Avoid passing around a custom type to the ParquetDataInputChunk
            // by merging it down to a simple map.
            private Map<String, Object> asMap() {
                Map<String, Object> renderedMap = new HashMap<>();
                for (int i = 0; i < keys.keys().size(); i++) {
                    renderedMap.put(keys.keys.get(i), values.values().get(i));
                }

                return renderedMap;
            }

            // marker classes for map keys to be picked up by the next function
            private record Keys(String fieldName, List<String> keys) {}

            // marker classes for map values to be picked up by the next function
            private record Values(String fieldName, List<Object> values) {}
        }

        @Override
        public List<Object> next() {
            var result = new ArrayList<>();
            var processedEmptyMaps = new HashSet<String>();
            MapLikeRecord mapLikeRecord = null;
            for (ColumnReader columnReader : this.columnReaders) {
                Object readValue = readValue(columnReader);
                // we only get a direct Map value if it's empty
                if (readValue instanceof Map emptyRecord) {
                    if (!processedEmptyMaps.contains(
                            columnReader.getDescriptor().getPath()[0])) {
                        result.add(emptyRecord);
                        processedEmptyMaps.add(columnReader.getDescriptor().getPath()[0]);
                    }
                } else if (readValue instanceof MapLikeRecord.Keys keys) {
                    if (mapLikeRecord == null) {
                        mapLikeRecord =
                                new MapLikeRecord(columnReader.getDescriptor().getPath()[0]);
                    } else if (!mapLikeRecord.fieldName.equals(keys.fieldName)) {
                        result.add(mapLikeRecord.asMap());
                        mapLikeRecord = new MapLikeRecord(keys.fieldName);
                    }
                    mapLikeRecord.keys = keys;
                } else if (readValue instanceof MapLikeRecord.Values values) {
                    if (mapLikeRecord == null) {
                        mapLikeRecord =
                                new MapLikeRecord(columnReader.getDescriptor().getPath()[0]);
                    } else if (!mapLikeRecord.fieldName.equals(values.fieldName)) {
                        result.add(mapLikeRecord.asMap());
                        mapLikeRecord = new MapLikeRecord(values.fieldName);
                    }
                    mapLikeRecord.values = values;
                } else if (readValue instanceof MapLikeRecord struct) {
                    if (mapLikeRecord != null && mapLikeRecord.keys != null) {
                        result.add(mapLikeRecord.asMap());
                        mapLikeRecord = null;
                    }
                    result.add(struct.asMap());

                } else {
                    // when switching to a non-map-related column, we can be sure that
                    // if there was a map before, it was read completely.
                    // add mapRecord if there is something upfront setting the next value
                    if (mapLikeRecord != null && mapLikeRecord.keys != null) {
                        result.add(mapLikeRecord.asMap());
                        mapLikeRecord = null;
                    }
                    result.add(readValue);
                }
            }
            // add if map record is last
            if (mapLikeRecord != null && mapLikeRecord.keys != null) {
                result.add(mapLikeRecord.asMap());
            }

            this.rowIndex++;

            return result;
        }

        /* This method does the `consume` call on the columnReader because the list
         * processing requires it for every single value.
         */
        private Object readValue(ColumnReader columnReader) {
            ColumnDescriptor column = columnReader.getDescriptor();
            PrimitiveType primitiveType = column.getPrimitiveType();
            String fieldName = column.getPath()[0];
            Type type = schema.getType(fieldName);
            LogicalTypeAnnotation logicalType = type.getLogicalTypeAnnotation();
            // reference:
            // https://github.com/apache/parquet-format/blob/master/LogicalTypes.md#parquet-logical-type-definitions

            // Handle lists and maps before the general null check, as they have separate null semantics (empty vs null)
            if (logicalType != null && logicalType.equals(LogicalTypeAnnotation.listType())) {
                return readListValues(columnReader, column, primitiveType);
            }

            if (logicalType != null && logicalType.equals(LogicalTypeAnnotation.mapType())) {
                return readMapValues(columnReader, column, fieldName, primitiveType);
            }

            if (type instanceof GroupType groupType) {
                return convertGroupValue(columnReader, groupType, fieldName, column, primitiveType);
            }

            // Check for null before reading temporal or primitive values
            if (isPrimitiveValueNull(columnReader)) {
                columnReader.consume();
                return null;
            }

            if (isInterval(logicalType)) {
                var duration = readIntervalAsDuration(columnReader);
                columnReader.consume();
                return duration;
            }

            var object = readTemporalValues(columnReader, logicalType, primitiveType);
            if (object != null) {
                columnReader.consume();
                return object;
            }

            // Primitives
            Object readValue = readPrimitiveType(columnReader, primitiveType);

            columnReader.consume();
            return readValue;
        }

        private MapLikeRecord convertGroupValue(
                ColumnReader columnReader,
                GroupType groupType,
                String fieldName,
                ColumnDescriptor column,
                PrimitiveType primitiveType) {
            // The only groupType supported right now is a struct.
            // This could also represent _any_ complex type.
            // Check if the struct field is null using definition level (same pattern as lists/maps)
            if (columnReader.getCurrentDefinitionLevel() == 0) {
                columnReader.consume();
                return null;
            }
            var mapKeys = new MapLikeRecord.Keys(fieldName, new ArrayList<>());
            String nestedFieldName = column.getPath()[1];
            String propertyName = fieldName + "." + nestedFieldName;
            mapKeys.keys().add(propertyName);
            var mapValues = new MapLikeRecord.Values(fieldName, new ArrayList<>());

            // Check if the nested field is a list type
            Type nestedType = groupType.getType(nestedFieldName);
            LogicalTypeAnnotation nestedLogicalType = nestedType.getLogicalTypeAnnotation();

            if (nestedLogicalType != null && nestedLogicalType.equals(LogicalTypeAnnotation.listType())) {
                // Handle list within struct - use repetition level pattern
                rejectNestedList(column);
                mapValues.values().add(collectRepeatedValues(columnReader, column, primitiveType));
            } else {
                // Only read value if we're at max definition level (field value is present)
                if (columnReader.getCurrentDefinitionLevel() == column.getMaxDefinitionLevel()) {
                    mapValues.values().add(readPrimitiveType(columnReader, primitiveType));
                } else {
                    // Field exists but value is null
                    mapValues.values().add(null);
                }
                columnReader.consume();
            }
            return new MapLikeRecord(fieldName, mapKeys, mapValues);
        }

        private ArrayList<Object> readListValues(
                ColumnReader columnReader, ColumnDescriptor column, PrimitiveType primitiveType) {
            rejectNestedList(column);
            // Check if the list itself is null (definition level 0)
            if (columnReader.getCurrentDefinitionLevel() == 0) {
                columnReader.consume();
                return null;
            }

            // Return empty list instead of null for empty lists
            return collectRepeatedValues(columnReader, column, primitiveType);
        }

        private Record readMapValues(
                ColumnReader columnReader, ColumnDescriptor column, String fieldName, PrimitiveType primitiveType) {
            // Check if the map itself is null (definition level 0)
            if (columnReader.getCurrentDefinitionLevel() == 0) {
                columnReader.consume();
                return null;
            }

            // Initialize data structure for a map by fieldName
            // the data will come in as "all keys" and then "all values".
            // As a consequence, we need to return them separately and merge them
            // later into a Java map.
            if (column.getPath()[2].equals("key")) {
                var mapKeys = new MapLikeRecord.Keys(fieldName, new ArrayList<>());
                var keys = collectRepeatedValues(columnReader, column, primitiveType);
                for (Object key : keys) {
                    mapKeys.keys().add(fieldName + "." + key);
                }
                return mapKeys;
            }

            var mapValues = new MapLikeRecord.Values(fieldName, new ArrayList<>());
            var values = collectRepeatedValuesWithNulls(columnReader, column, primitiveType);
            mapValues.values().addAll(values);
            return mapValues;
        }

        /**
         * Rejects nested list columns (arrays of arrays). A standard 3-level LIST encodes its element at
         * max repetition level 1; a higher value means the element is itself repeated. Neo4j properties
         * cannot hold arrays of arrays, so fail fast with a clear message instead.
         */
        private static void rejectNestedList(ColumnDescriptor column) {
            if (column.getMaxRepetitionLevel() > 1) {
                throw new InputException(
                        "Nested list columns are not supported (column '%s'): Neo4j properties cannot hold arrays of arrays."
                                .formatted(column.getPath()[0]));
            }
        }

        /**
         * Collects values from a repeated group (list elements, map keys, etc.) using repetition levels.
         * Only collects non-null values where definition level equals max definition level.
         */
        private ArrayList<Object> collectRepeatedValues(
                ColumnReader columnReader, ColumnDescriptor column, PrimitiveType primitiveType) {
            var values = new ArrayList<>();
            do {
                if (columnReader.getCurrentDefinitionLevel() == column.getMaxDefinitionLevel()) {
                    values.add(readRepeatedElement(columnReader, primitiveType));
                }
                columnReader.consume();
            } while (columnReader.getCurrentRepetitionLevel() > 0);
            return values;
        }

        /**
         * Collects values from a repeated group, including null values for entries where the
         * definition level indicates the entry exists but the value is null.
         * Used for map values where null values need to be preserved.
         */
        private ArrayList<Object> collectRepeatedValuesWithNulls(
                ColumnReader columnReader, ColumnDescriptor column, PrimitiveType primitiveType) {
            var values = new ArrayList<>();
            do {
                if (columnReader.getCurrentDefinitionLevel() == column.getMaxDefinitionLevel()) {
                    values.add(readRepeatedElement(columnReader, primitiveType));
                } else if (columnReader.getCurrentDefinitionLevel() > 0) {
                    // Entry exists but value is null
                    values.add(null);
                }
                columnReader.consume();
            } while (columnReader.getCurrentRepetitionLevel() > 0);
            return values;
        }

        /**
         * Reads a single element of a repeated group (a list/map member). Unlike scalar columns, the logical
         * type annotation of a list element lives on the element's primitive type rather than on the top-level
         * field, so we resolve it from there. Temporal elements are converted to their underlying {@code java.time}
         * representation so that they end up in a properly typed array; everything else is read as-is.
         */
        private Object readRepeatedElement(ColumnReader columnReader, PrimitiveType primitiveType) {
            var logicalType = primitiveType.getLogicalTypeAnnotation();
            if (isInterval(logicalType)) {
                return readIntervalAsDuration(columnReader);
            }
            var value = readPrimitiveType(columnReader, primitiveType);
            var temporal = toTemporalValue(value, logicalType);
            return temporal != null ? temporal : value;
        }

        private static boolean isInterval(LogicalTypeAnnotation logicalType) {
            return logicalType != null && logicalType.equals(LogicalTypeAnnotation.intervalType());
        }

        /**
         * Reads a Parquet {@code INTERVAL} value (a 12-byte little-endian triple of unsigned ints holding months, days
         * and milliseconds) into a Neo4j {@link DurationValue}. Does not advance the reader; the caller is responsible
         * for {@link ColumnReader#consume()}, matching how the other read methods are driven.
         * See https://github.com/apache/parquet-format/blob/master/LogicalTypes.md#interval
         */
        private static DurationValue readIntervalAsDuration(ColumnReader columnReader) {
            var buffer = columnReader.getBinary().toByteBuffer().order(ByteOrder.LITTLE_ENDIAN);
            int pos = buffer.position();
            long months = Integer.toUnsignedLong(buffer.getInt(pos));
            long days = Integer.toUnsignedLong(buffer.getInt(pos + 4));
            long millis = Integer.toUnsignedLong(buffer.getInt(pos + 8));
            return DurationValue.duration(months, days, millis / 1000, (millis % 1000) * 1_000_000);
        }

        private static boolean isPrimitiveValueNull(ColumnReader columnReader) {
            ColumnDescriptor column = columnReader.getDescriptor();

            return columnReader.getCurrentDefinitionLevel() != column.getMaxDefinitionLevel();
        }

        private Object readTemporalValues(
                ColumnReader columnReader, LogicalTypeAnnotation logicalType, PrimitiveType primitiveType) {
            var object = readPrimitiveType(columnReader, primitiveType);
            return toTemporalValue(object, logicalType);
        }

        /**
         * Maps an already-read primitive value to its temporal {@link Value} based on the logical type annotation,
         * or returns {@code null} when the logical type is not a (supported) temporal one. This is the pure
         * conversion shared by scalar columns and by list/map elements; it does not advance the column reader.
         * See https://github.com/apache/parquet-format/blob/master/LogicalTypes.md#parquet-logical-type-definitions
         */
        private static Value toTemporalValue(Object object, LogicalTypeAnnotation logicalType) {
            if (object == null || logicalType == null) {
                return null;
            }
            // Dates
            if (LogicalTypeAnnotation.dateType().equals(logicalType)) {
                return DateValue.date(LocalDate.ofEpochDay((int) object));
            }
            // Time UTC true
            if (LogicalTypeAnnotation.timeType(true, LogicalTypeAnnotation.TimeUnit.MILLIS)
                    .equals(logicalType)) {
                return TimeValue.time(
                        LocalTime.ofNanoOfDay(((int) object) * 1_000_000L).atOffset(ZoneOffset.UTC));
            }
            if (LogicalTypeAnnotation.timeType(true, LogicalTypeAnnotation.TimeUnit.MICROS)
                    .equals(logicalType)) {
                return TimeValue.time(
                        LocalTime.ofNanoOfDay(((long) object) * 1_000L).atOffset(ZoneOffset.UTC));
            }
            if (LogicalTypeAnnotation.timeType(true, LogicalTypeAnnotation.TimeUnit.NANOS)
                    .equals(logicalType)) {
                return TimeValue.time(LocalTime.ofNanoOfDay((long) object).atOffset(ZoneOffset.UTC));
            }
            // Time UTC false
            if (LogicalTypeAnnotation.timeType(false, LogicalTypeAnnotation.TimeUnit.MILLIS)
                    .equals(logicalType)) {
                var seconds = ((int) object) * 1_000_000L / 1_000_000_000L;
                var nanos = ((int) object) * 1_000_000L % 1_000_000_000L;
                return LocalTimeValue.localTime(LocalTime.ofSecondOfDay(seconds).plusNanos(nanos));
            }
            if (LogicalTypeAnnotation.timeType(false, LogicalTypeAnnotation.TimeUnit.MICROS)
                    .equals(logicalType)) {
                var seconds = ((long) object) * 1_000L / 1_000_000_000L;
                var nanos = ((long) object) * 1_000L % 1_000_000_000L;
                return LocalTimeValue.localTime(LocalTime.ofSecondOfDay(seconds).plusNanos(nanos));
            }
            if (LogicalTypeAnnotation.timeType(false, LogicalTypeAnnotation.TimeUnit.NANOS)
                    .equals(logicalType)) {
                var seconds = ((long) object) / 1_000_000_000L;
                var nanos = ((long) object) % 1_000_000_000L;
                return LocalTimeValue.localTime(LocalTime.ofSecondOfDay(seconds).plusNanos(nanos));
            }
            // Timestamp UTC true
            if (LogicalTypeAnnotation.timestampType(true, LogicalTypeAnnotation.TimeUnit.MILLIS)
                    .equals(logicalType)) {
                var seconds = ((long) object) / 1_000L;
                var millis = ((long) object) % 1_000L;
                return DateTimeValue.datetime(ZonedDateTime.of(
                        LocalDateTime.ofEpochSecond(seconds, (int) millis * 1_000_000, ZoneOffset.UTC),
                        ZoneId.of(ZoneOffset.UTC.getId())));
            }
            if (LogicalTypeAnnotation.timestampType(true, LogicalTypeAnnotation.TimeUnit.MICROS)
                    .equals(logicalType)) {
                var seconds = ((long) object) / 1_000_000L;
                var micros = ((long) object) % 1_000_000L;
                return DateTimeValue.datetime(ZonedDateTime.of(
                        LocalDateTime.ofEpochSecond(seconds, (int) micros * 1_000, ZoneOffset.UTC),
                        ZoneId.of(ZoneOffset.UTC.getId())));
            }
            if (LogicalTypeAnnotation.timestampType(true, LogicalTypeAnnotation.TimeUnit.NANOS)
                    .equals(logicalType)) {
                var seconds = ((long) object) / 1_000_000_000L;
                var nanos = ((long) object) % 1_000_000_000L;
                return DateTimeValue.datetime(ZonedDateTime.of(
                        LocalDateTime.ofEpochSecond(seconds, (int) nanos, ZoneOffset.UTC),
                        ZoneId.of(ZoneOffset.UTC.getId())));
            }
            // Timestamp UTC false
            if (LogicalTypeAnnotation.timestampType(false, LogicalTypeAnnotation.TimeUnit.MILLIS)
                    .equals(logicalType)) {
                var seconds = ((long) object) / 1_000L;
                var millis = ((long) object) % 1_000L;
                return LocalDateTimeValue.localDateTime(
                        LocalDateTime.ofEpochSecond(seconds, (int) millis * 1_000_000, ZoneOffset.UTC));
            }
            if (LogicalTypeAnnotation.timestampType(false, LogicalTypeAnnotation.TimeUnit.MICROS)
                    .equals(logicalType)) {
                var seconds = ((long) object) / 1_000_000L;
                var micros = ((long) object) % 1_000_000L;
                return LocalDateTimeValue.localDateTime(
                        LocalDateTime.ofEpochSecond(seconds, (int) (micros * 1_000), ZoneOffset.UTC));
            }
            if (LogicalTypeAnnotation.timestampType(false, LogicalTypeAnnotation.TimeUnit.NANOS)
                    .equals(logicalType)) {
                var seconds = ((long) object) / 1_000_000_000L;
                var nanos = ((long) object) % 1_000_000_000L;
                return LocalDateTimeValue.localDateTime(
                        LocalDateTime.ofEpochSecond(seconds, (int) nanos, ZoneOffset.UTC));
            }
            return null;
        }

        private static Object readPrimitiveType(ColumnReader columnReader, PrimitiveType primitiveType) {
            return switch (primitiveType.getPrimitiveTypeName()) {
                case BINARY, FIXED_LEN_BYTE_ARRAY, INT96 ->
                    primitiveType.stringifier().stringify(columnReader.getBinary());
                case BOOLEAN -> columnReader.getBoolean();
                case DOUBLE -> columnReader.getDouble();
                case FLOAT -> columnReader.getFloat();
                case INT32 -> columnReader.getInteger();
                case INT64 -> columnReader.getLong();
            };
        }
    }

    @Override
    public void close() throws IOException {
        this.metadataReader.close();
    }
}
