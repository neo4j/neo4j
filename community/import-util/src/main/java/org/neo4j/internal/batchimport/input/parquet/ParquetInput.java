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

import static org.neo4j.util.Preconditions.checkState;

import blue.strategic.parquet.ParquetReader;
import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import org.apache.parquet.column.ColumnDescriptor;
import org.apache.parquet.hadoop.metadata.BlockMetaData;
import org.apache.parquet.hadoop.metadata.ColumnChunkMetaData;
import org.apache.parquet.hadoop.metadata.ParquetMetadata;
import org.apache.parquet.io.DelegatingSeekableInputStream;
import org.apache.parquet.io.InputFile;
import org.apache.parquet.io.SeekableInputStream;
import org.eclipse.collections.api.factory.Lists;
import org.neo4j.batchimport.api.InputIterable;
import org.neo4j.batchimport.api.input.Collector;
import org.neo4j.batchimport.api.input.FileGroup;
import org.neo4j.batchimport.api.input.IdType;
import org.neo4j.batchimport.api.input.Input;
import org.neo4j.batchimport.api.input.PropertySizeCalculator;
import org.neo4j.batchimport.api.input.ReadableGroups;
import org.neo4j.cloud.storage.io.ReadableChannel;
import org.neo4j.csv.reader.Configuration;
import org.neo4j.importer.SchemaCommandSource;
import org.neo4j.internal.batchimport.input.Groups;
import org.neo4j.internal.batchimport.input.HeaderException;
import org.neo4j.internal.batchimport.input.InputException;
import org.neo4j.internal.batchimport.input.csv.DataFactories;
import org.neo4j.internal.schema.SchemaDescriptor;
import org.neo4j.internal.schema.SchemaDescriptors;
import org.neo4j.token.TokenHolders;
import org.neo4j.token.api.TokenConstants;

/**
 * Provides {@link Input} from data contained in Parquet files.
 */
public class ParquetInput implements Input {
    public static final char DELIMITER =
            '\u0007'; // BEL char as in {@link org.neo4j.internal.batchimport.input.csv.IdValueBuilder}
    private static final Supplier<ZoneId> defaultTimezoneSupplier = () -> ZoneOffset.UTC;

    private final List<ParquetData> nodeDatas;
    private final List<ParquetData> relationshipDatas;
    private final SchemaCommandSource schemaCommandSource;
    private final IdType idType;
    private final Groups groups;
    private final ParquetMonitor monitor;
    // this is used for header mapping parsing only
    private final Configuration csvConfig;
    private final Map<Set<String>, List<FileGroup>> nodeFiles;
    private final Map<String, List<FileGroup>> relationshipFiles;
    private final List<ParquetColumnMetadata> verifiedColumns;
    private final boolean containsVectorData;

    public ParquetInput(
            Map<Set<String>, List<FileGroup>> nodeFiles,
            Map<String, List<FileGroup>> relationshipFiles,
            SchemaCommandSource schemaCommandSource,
            IdType defaultIdType,
            Configuration csvConfig,
            Groups groups,
            ParquetMonitor monitor) {
        this.groups = groups;
        this.monitor = monitor;
        this.csvConfig = csvConfig;
        this.nodeFiles = nodeFiles;
        this.relationshipFiles = relationshipFiles;
        this.schemaCommandSource = schemaCommandSource;
        this.verifiedColumns = verifyColumns(nodeFiles, relationshipFiles);
        this.idType = autoDetectIdType(defaultIdType, verifiedColumns);
        this.containsVectorData = containsVectorData(verifiedColumns);
        this.nodeDatas = nodeData(verifiedColumns, nodeFiles);
        this.relationshipDatas = relationshipData(verifiedColumns, relationshipFiles);
    }

    @Override
    public InputIterable nodes(Collector badCollector) {
        return () -> new ParquetGroupInputIterator(
                nodeDatas,
                groups,
                idType,
                String.valueOf(csvConfig.arrayDelimiter()),
                String.valueOf(csvConfig.vectorDelimiter()));
    }

    @Override
    public InputIterable relationships(Collector badCollector) {
        return () -> new ParquetGroupInputIterator(
                relationshipDatas,
                groups,
                idType,
                String.valueOf(csvConfig.arrayDelimiter()),
                String.valueOf(csvConfig.vectorDelimiter()));
    }

    @Override
    public IdType idType() {
        return idType;
    }

    @Override
    public ReadableGroups groups() {
        return groups;
    }

    /**
     * @return the {@link IdType} of the {@code IdMapper} that suits the parsed parquet node columns.
     * Returns {@link IdType#INTEGER} only if every node file metadata contains at most one ID column
     * and every ID column has an effective type that is long-castable.
     * Returns {@link IdType#ACTUAL} when the input is configured with that mode.
     * Falls back to {@code defaultIdType} when no node ID columns are present at all.
     */
    private static IdType autoDetectIdType(IdType defaultIdType, List<ParquetColumnMetadata> verifiedColumns) {
        if (defaultIdType == IdType.ACTUAL) {
            return IdType.ACTUAL;
        }
        int totalIdColumns = 0;
        for (ParquetColumnMetadata metadata : verifiedColumns) {
            if (metadata.entityType() != EntityType.NODE) {
                continue;
            }
            int idColumnsInMetadata = 0;
            for (ParquetColumn column : metadata.columns()) {
                if (!column.isIdColumn()) {
                    continue;
                }
                idColumnsInMetadata++;
                IdType columnIdType = column.columnIdType() != null ? column.columnIdType() : defaultIdType;
                if (columnIdType != IdType.INTEGER) {
                    return IdType.STRING;
                }
            }
            if (idColumnsInMetadata > 1) {
                return IdType.STRING;
            }
            totalIdColumns += idColumnsInMetadata;
        }
        return totalIdColumns == 0 ? defaultIdType : IdType.INTEGER;
    }

    @Override
    public SchemaCommandSource schemaCommandSource() {
        return schemaCommandSource;
    }

    @Override
    public boolean containsVectorData() {
        return containsVectorData;
    }

    private static List<ParquetData> nodeData(
            List<ParquetColumnMetadata> verifiedColumns, Map<Set<String>, List<FileGroup>> nodeFiles) {
        return parquetData(verifiedColumns, EntityType.NODE, nodeFiles, ParquetInput::extractExternalLabels);
    }

    private static List<ParquetData> relationshipData(
            List<ParquetColumnMetadata> verifiedColumns, Map<String, List<FileGroup>> relationshipFiles) {
        return parquetData(
                verifiedColumns, EntityType.RELATIONSHIP, relationshipFiles, ParquetInput::extractExternalRelType);
    }

    private static <KEY> List<ParquetData> parquetData(
            List<ParquetColumnMetadata> verifiedColumns,
            EntityType entityType,
            Map<KEY, List<FileGroup>> files,
            Function<KEY, Set<String>> keyExtractor) {

        var indexedMetadata = verifiedColumns.stream()
                .filter(verifiedColumn -> verifiedColumn.entityType == entityType)
                // **not** using toMap (which rejects key duplicates)
                // labels (or reltypes) can reuse the same data files with different mappings
                // this also means we need to deduplicate parquet data in such cases
                .collect(Collectors.groupingBy(ParquetColumnMetadata::key));

        var deduplicatedColumnData = new LinkedHashSet<ParquetData>();
        files.forEach((rawLabelsOrType, fileGroups) -> {
            for (FileGroup fileGroup : fileGroups) {
                for (FileGroup.NumberedFile path : fileGroup.files()) {
                    if (!isHeaderFile(path.path())) {
                        var labelsOrType = keyExtractor.apply(rawLabelsOrType);
                        var metadataKey = new ParquetColumnMetadataKey(path.path(), labelsOrType);
                        var allMetadata = indexedMetadata.get(metadataKey);
                        for (ParquetColumnMetadata metadata : allMetadata) {
                            deduplicatedColumnData.add(new ParquetData(
                                    entityType,
                                    labelsOrType,
                                    path.path(),
                                    metadata.columns(),
                                    defaultTimezoneSupplier));
                        }
                    }
                }
            }
        });
        final var columnData = Lists.mutable.<ParquetData>empty();
        columnData.addAll(deduplicatedColumnData);
        return columnData;
    }

    private static class HeaderContext {

        private final Map<List<Path>, Map<String, ParquetColumn.HeaderDefinition>> headerColumnNameDefinitions =
                new HashMap<>();
        private final Map<List<Path>, Map<Integer, ParquetColumn.HeaderDefinition>> headerColumnIndexDefinitions =
                new HashMap<>();

        private void addHeaderDefinition(
                List<Path> files, String columnName, ParquetColumn.HeaderDefinition headerDefinition) {
            headerColumnNameDefinitions.putIfAbsent(files, new HashMap<>());
            headerColumnNameDefinitions.get(files).put(columnName, headerDefinition);
        }

        private void addHeaderDefinition(
                List<Path> files, Integer index, ParquetColumn.HeaderDefinition headerDefinition) {
            headerColumnIndexDefinitions.putIfAbsent(files, new HashMap<>());
            headerColumnIndexDefinitions.get(files).put(index, headerDefinition);
        }

        private ParquetColumn.HeaderDefinition getHeaderDefinition(List<Path> files, Integer index, String columnName) {
            if (headerColumnNameDefinitions.get(files) != null) {
                return headerColumnNameDefinitions.get(files).get(columnName);
            }

            if (headerColumnIndexDefinitions.get(files) != null) {
                return headerColumnIndexDefinitions.get(files).get(index).addParquetColumnName(columnName);
            }

            return new ParquetColumn.SingleRowHeaderDefinition(columnName);
        }

        private boolean isNotIncludedInHeaderDefinition(List<Path> value, Integer index, String columnName) {
            return (headerColumnNameDefinitions.containsKey(value)
                            && !headerColumnNameDefinitions.get(value).containsKey(columnName))
                    || (headerColumnIndexDefinitions.containsKey(value)
                            && !headerColumnIndexDefinitions.get(value).containsKey(index));
        }

        private boolean columnShouldBeSkipped(
                boolean fileGroupHasHeader, List<Path> fileGroupPaths, int index, String columnName) {
            return fileGroupHasHeader && isNotIncludedInHeaderDefinition(fileGroupPaths, index, columnName);
        }

        private void ensureAllColumnsExist(Path dataFile, List<Path> fileGroupPaths, Set<String> columnNames) {
            var headers = headerColumnNameDefinitions.entrySet().stream()
                    .filter(entry -> entry.getKey().equals(fileGroupPaths))
                    .flatMap(entry -> entry.getValue().keySet().stream())
                    .collect(Collectors.toSet());

            if (!columnNames.containsAll(headers)) {
                var errorHeaders = new ArrayList<>(headers);
                errorHeaders.removeAll(columnNames);
                throw new InputException("Target column(s) '%s' from header cannot be found in %s."
                        .formatted(errorHeaders, dataFile.toString()));
            }
        }
    }

    private List<ParquetColumnMetadata> verifyColumns(
            Map<Set<String>, List<FileGroup>> labelsAndNodeFiles,
            Map<String, List<FileGroup>> typeAndRelationshipFiles) {

        var headerContext = new HeaderContext();

        List<ParquetColumnMetadata> columnInfo = new ArrayList<>();
        try {
            var numberOfIdsPerGroup = verifyNodes(labelsAndNodeFiles, headerContext, columnInfo);
            verifyRelationships(typeAndRelationshipFiles, headerContext, columnInfo, numberOfIdsPerGroup);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }

        return columnInfo;
    }

    private Map<String, Integer> verifyNodes(
            Map<Set<String>, List<FileGroup>> labelsAndNodeFiles,
            HeaderContext headerContext,
            List<ParquetColumnMetadata> columnInfo)
            throws IOException {
        var numberOfIdsPerGroup = new HashMap<String, Integer>();
        for (Map.Entry<Set<String>, List<FileGroup>> labelsAndNodeFilesEntry : labelsAndNodeFiles.entrySet()) {
            var labels = labelsAndNodeFilesEntry.getKey();
            var hasLabelColumn = !labels.isEmpty() && labels.stream().anyMatch(label -> !label.isBlank());

            for (FileGroup fileGroup : labelsAndNodeFilesEntry.getValue()) {
                var fileGroupPaths = fileGroup.streamPaths().collect(Collectors.toList());
                boolean fileGroupHasHeader = false;

                for (Path nodeFile : fileGroupPaths) {
                    if (processPotentialHeaderFile(nodeFile, fileGroupPaths, headerContext)) {
                        fileGroupHasHeader = true;
                        continue;
                    }
                    ParquetMetadata metadata;
                    try {
                        metadata = ParquetReader.readMetadata(ParquetImportInputFile.of(nodeFile));
                    } catch (RuntimeException e) {
                        throw new RuntimeException(
                                "Could not read parquet file %s".formatted(nodeFile.toAbsolutePath()), e);
                    }
                    var currentColumnInfo = new ArrayList<ParquetColumn>();
                    var propertyNames = new HashSet<String>();
                    String previousGroupName = null;
                    int idColumnCount = 0;
                    var schema = metadata.getFileMetaData().getSchema();
                    var columns = schema.getColumns();
                    if (fileGroupHasHeader) {
                        headerContext.ensureAllColumnsExist(
                                nodeFile,
                                fileGroupPaths,
                                columns.stream().map(cd -> cd.getPath()[0]).collect(Collectors.toSet()));
                    }
                    Set<String> mapColumns = new HashSet<>();
                    Set<String> structColumns = new HashSet<>();
                    // check for possible group / ID space definitions and register them
                    String fileName = nodeFile.getFileName().toString();
                    for (int i = 0; i < columns.size(); i++) {
                        ColumnDescriptor columnDescriptor = columns.get(i);
                        String[] namePath = columnDescriptor.getPath();
                        var columnName = namePath[0];
                        var type = schema.getType(columnName);

                        if (columnName.isBlank()) {
                            throw new InputException("column name must not be blank");
                        }
                        try {
                            // ignore missing column in header definition
                            if (headerContext.columnShouldBeSkipped(
                                    fileGroupHasHeader, fileGroupPaths, i, columnName)) {
                                continue;
                            }
                            var parquetColumn = ParquetColumn.from(
                                    headerContext.getHeaderDefinition(fileGroupPaths, i, columnName),
                                    EntityType.NODE,
                                    columnDescriptor.getPrimitiveType(),
                                    type.getLogicalTypeAnnotation());
                            if (parquetColumn.isIgnoredColumn()) {
                                continue;
                            }
                            String propertyName = parquetColumn.propertyName() != null
                                    ? parquetColumn.propertyName()
                                    : parquetColumn.logicalColumnType().name();
                            if (parquetColumn.isIdColumn() && parquetColumn.groupName() != null) {
                                if (previousGroupName != null && !previousGroupName.equals(parquetColumn.groupName())) {
                                    throw new IllegalStateException(
                                            "There are multiple :ID columns, but they are referring to different groups");
                                }
                                previousGroupName = parquetColumn.groupName();
                            }
                            if (parquetColumn.isIdColumn()) {
                                idColumnCount++;
                            }
                            if (propertyNames.contains(propertyName) && parquetColumn.isIdColumn()) {
                                throw new DuplicatedColumnException(
                                        "Multiple :ID columns share the same property name '%s' in file '%s'. Each part of a composite ID must have a unique property name."
                                                .formatted(propertyName, fileName));
                            }
                            var firstPropertyNamePart =
                                    propertyName.contains(".") ? propertyName.split("\\.")[0] : propertyName;

                            if ((propertyNames.contains(propertyName) || propertyNames.contains(firstPropertyNamePart))
                                    && !mapColumns.contains(propertyName)
                                    && !structColumns.contains(propertyName)) {
                                throw new DuplicatedColumnException("Duplicated header property %s found in file %s."
                                        .formatted(propertyName, fileName));
                            }
                            propertyNames.add(propertyName);

                            if (parquetColumn.logicalColumnType() == ParquetLogicalColumnType.ID) {
                                var group = groups.getOrCreate(parquetColumn.groupName());
                                var columnIdType = parquetColumn.columnIdType();
                                if (columnIdType != null) {
                                    groups.bindIdType(group, parquetColumn.propertyName(), columnIdType.name());
                                }
                            }
                            if (parquetColumn.columnType().needsConversion()) {
                                monitor.typeNormalized(
                                        fileName,
                                        propertyName,
                                        parquetColumn.columnType().name(),
                                        parquetColumn
                                                .columnType()
                                                .convertedType()
                                                .name());
                            }
                            if (parquetColumn.logicalColumnType() == ParquetLogicalColumnType.LABEL) {
                                hasLabelColumn = true;
                            }
                            // Avoid duplicate columns
                            if (!mapColumns.contains(propertyName)) {
                                currentColumnInfo.add(parquetColumn);
                            }
                            if (namePath.length > 1 && "key_value".equals(namePath[1])) {
                                mapColumns.add(propertyName);
                            } else if (namePath.length > 1) {
                                structColumns.add(propertyName);
                            }
                        } catch (IllegalArgumentException e) {
                            throw new InputException(
                                    "Column '%s' in node file '%s' is not a recognised type. Expected one of: %s"
                                            .formatted(
                                                    columnName,
                                                    fileName,
                                                    ParquetColumn.getReservedColumns(EntityType.NODE)),
                                    e);
                        }
                    }
                    if (!hasLabelColumn) {
                        monitor.noNodeLabelsSpecified(fileName);
                    }
                    if (previousGroupName != null && idColumnCount > 0) {
                        final int count = idColumnCount;
                        numberOfIdsPerGroup.merge(
                                previousGroupName,
                                count,
                                (existing, newCount) -> existing.equals(newCount) ? existing : -1);
                    }
                    var metadataKey = new ParquetColumnMetadataKey(nodeFile, extractExternalLabels(labels));
                    columnInfo.add(new ParquetColumnMetadata(metadataKey, EntityType.NODE, currentColumnInfo));
                }
            }
        }
        return numberOfIdsPerGroup;
    }

    private void verifyRelationships(
            Map<String, List<FileGroup>> typeAndRelationshipFiles,
            HeaderContext headerContext,
            List<ParquetColumnMetadata> columnInfo,
            Map<String, Integer> numberOfIdsPerGroup)
            throws IOException {
        for (Map.Entry<String, List<FileGroup>> typeAndRelationshipFilesEntry : typeAndRelationshipFiles.entrySet()) {
            var relType = typeAndRelationshipFilesEntry.getKey();
            var hasTypeColumn = relType != null && !relType.isBlank();
            // parse all relationship headers and verify all ID spaces
            for (FileGroup fileGroup : typeAndRelationshipFilesEntry.getValue()) {
                var relationshipFileList = fileGroup.streamPaths().collect(Collectors.toList());
                boolean fileGroupHasHeader = false;
                Set<String> mapColumns = new HashSet<>();
                Set<String> structColumns = new HashSet<>();
                for (Path relationshipFile : relationshipFileList) {
                    if (processPotentialHeaderFile(relationshipFile, relationshipFileList, headerContext)) {
                        fileGroupHasHeader = true;
                        continue;
                    }
                    ParquetMetadata metadata;
                    try {
                        metadata = ParquetReader.readMetadata(ParquetImportInputFile.of(relationshipFile));
                    } catch (RuntimeException e) {
                        throw new RuntimeException(
                                "Could not read parquet file %s".formatted(relationshipFile.toAbsolutePath()), e);
                    }
                    var currentColumnInfo = new ArrayList<ParquetColumn>();
                    var propertyNames = new HashSet<String>();
                    var schema = metadata.getFileMetaData().getSchema();
                    var columns = schema.getColumns();
                    String fileName = relationshipFile.getFileName().toString();
                    String startIdGroup = null;
                    String endIdGroup = null;
                    int numStartIds = 0;
                    int numEndIds = 0;
                    for (int i = 0; i < columns.size(); i++) {
                        ColumnDescriptor columnDescriptor = columns.get(i);
                        String[] namePath = columnDescriptor.getPath();
                        var columnName = namePath[0];

                        var type = schema.getType(columnName);

                        try {
                            if (headerContext.columnShouldBeSkipped(
                                    fileGroupHasHeader, relationshipFileList, i, columnName)) {
                                continue;
                            }
                            var parquetColumn = ParquetColumn.from(
                                    headerContext.getHeaderDefinition(relationshipFileList, i, columnName),
                                    EntityType.RELATIONSHIP,
                                    columnDescriptor.getPrimitiveType(),
                                    type.getLogicalTypeAnnotation());
                            if (parquetColumn.isIgnoredColumn()) {
                                continue;
                            }
                            String propertyName = parquetColumn.propertyName() != null
                                    ? parquetColumn.propertyName()
                                    : parquetColumn.logicalColumnType().name();
                            if (propertyNames.contains(propertyName)
                                    && !mapColumns.contains(propertyName)
                                    && !structColumns.contains(propertyName)
                                    && !parquetColumn.isStartId()
                                    && !parquetColumn.isEndId()) {
                                throw new DuplicatedColumnException("Duplicated header property %s found in file %s."
                                        .formatted(propertyName, fileName));
                            }
                            propertyNames.add(propertyName);
                            if (parquetColumn.columnType().needsConversion()) {
                                monitor.typeNormalized(
                                        fileName,
                                        propertyName,
                                        parquetColumn.columnType().name(),
                                        parquetColumn
                                                .columnType()
                                                .convertedType()
                                                .name());
                            }
                            if (parquetColumn.isStartId()) {
                                try {
                                    groups.get(parquetColumn.groupName());
                                } catch (HeaderException e) {
                                    throw new InputException(e.getMessage());
                                }
                                if (startIdGroup != null && !startIdGroup.equals(parquetColumn.groupName())) {
                                    throw new IllegalStateException(
                                            "Relationship '%s' has multiple :START_ID columns referring to different groups: '%s' and '%s'. All :START_ID columns must belong to the same group."
                                                    .formatted(relType, startIdGroup, parquetColumn.groupName()));
                                }
                                startIdGroup = parquetColumn.groupName();
                                numStartIds++;
                            } else if (parquetColumn.isEndId()) {
                                try {
                                    groups.get(parquetColumn.groupName());
                                } catch (HeaderException e) {
                                    throw new InputException(e.getMessage());
                                }
                                if (endIdGroup != null && !endIdGroup.equals(parquetColumn.groupName())) {
                                    throw new IllegalStateException(
                                            "Relationship '%s' has multiple :END_ID columns referring to different groups: '%s' and '%s'. All :END_ID columns must belong to the same group."
                                                    .formatted(relType, endIdGroup, parquetColumn.groupName()));
                                }
                                endIdGroup = parquetColumn.groupName();
                                numEndIds++;
                            }
                            if (parquetColumn.logicalColumnType() == ParquetLogicalColumnType.TYPE) {
                                hasTypeColumn = true;
                            }
                            // Avoid duplicate columns
                            if (!mapColumns.contains(propertyName)) {
                                currentColumnInfo.add(parquetColumn);
                            }
                            if (namePath.length > 1 && "key_value".equals(namePath[1])) {
                                mapColumns.add(propertyName);
                            }
                            if (namePath.length > 1) {
                                structColumns.add(propertyName);
                            }
                        } catch (IllegalArgumentException e) {
                            throw new InputException(
                                    "Column '%s' in relationship file '%s' is not a recognised type. Expected one of: %s"
                                            .formatted(
                                                    columnName,
                                                    fileName,
                                                    ParquetColumn.getReservedColumns(EntityType.RELATIONSHIP)),
                                    e);
                        }
                    }
                    if (numStartIds > 1) {
                        var expectedCount = numberOfIdsPerGroup.get(startIdGroup);
                        if (expectedCount != null && expectedCount != -1 && expectedCount != numStartIds) {
                            throw new IllegalStateException(
                                    "Number of :START_ID columns (%d) does not match the number of :ID columns (%d) in node group '%s'"
                                            .formatted(numStartIds, expectedCount, startIdGroup));
                        }
                    }
                    if (numEndIds > 1) {
                        var expectedCount = numberOfIdsPerGroup.get(endIdGroup);
                        if (expectedCount != null && expectedCount != -1 && expectedCount != numEndIds) {
                            throw new IllegalStateException(
                                    "Number of :END_ID columns (%d) does not match the number of :ID columns (%d) in node group '%s'"
                                            .formatted(numEndIds, expectedCount, endIdGroup));
                        }
                    }
                    var metadataKey = new ParquetColumnMetadataKey(relationshipFile, extractExternalRelType(relType));
                    columnInfo.add(new ParquetColumnMetadata(metadataKey, EntityType.RELATIONSHIP, currentColumnInfo));

                    if (!hasTypeColumn) {
                        monitor.noRelationshipTypeSpecified(fileName);
                    }
                }
            }
        }
    }

    private static Set<String> extractExternalLabels(Set<String> labels) {
        return labels;
    }

    private static Set<String> extractExternalRelType(String typeGroupOrNull) {
        return typeGroupOrNull == null ? Set.of() : Set.of(typeGroupOrNull);
    }

    private static boolean containsVectorData(List<ParquetColumnMetadata> verifiedColumns) {
        return verifiedColumns.stream()
                .flatMap(metadata -> metadata.columns().stream())
                .anyMatch(column -> column.logicalColumnType() == ParquetLogicalColumnType.PROPERTY
                        && column.columnType() == ParquetColumnType.VECTOR);
    }

    record ParquetColumnMetadata(ParquetColumnMetadataKey key, EntityType entityType, List<ParquetColumn> columns) {}

    record ParquetColumnMetadataKey(Path path, Collection<String> labelsOrType) {}

    private boolean processPotentialHeaderFile(Path path, List<Path> paths, ParquetInput.HeaderContext headerContext)
            throws IOException {
        if (!isHeaderFile(path)) {
            return false;
        }
        try (var csvInput = new BufferedReader(new InputStreamReader(Files.newInputStream(path)))) {
            var lines = csvInput.lines().toList();
            if (lines.isEmpty() || lines.stream().allMatch(String::isBlank)) {
                throw new IllegalArgumentException("The header definition is empty");
            }
            if (lines.size() > 2) {
                throw new IllegalArgumentException("The header is expected to have one or two lines");
            }
            if (lines.size() == 1) { // CSV import style header
                var targetColumnNames = DataFactories.parseRawHeaderEntries(
                        path.toString(),
                        csvConfig,
                        defaultTimezoneSupplier,
                        lines.getFirst().toCharArray());
                for (int i = 0; i < targetColumnNames.size(); i++) {
                    headerContext.addHeaderDefinition(
                            paths, i, ParquetColumn.HeaderDefinition.from(targetColumnNames.get(i)));
                }
            } else { // Parquet import style header;
                var targetColumnNames = DataFactories.parseRawHeaderEntries(
                        path.toString(),
                        csvConfig,
                        defaultTimezoneSupplier,
                        lines.get(0).toCharArray());
                var existingParquetColumns = DataFactories.parseRawHeaderEntries(
                        path.toString(),
                        csvConfig,
                        defaultTimezoneSupplier,
                        lines.get(1).toCharArray());
                for (int i = 0; i < targetColumnNames.size(); i++) {
                    if (existingParquetColumns.size() > i) {
                        headerContext.addHeaderDefinition(
                                paths,
                                existingParquetColumns.get(i),
                                ParquetColumn.HeaderDefinition.from(
                                        targetColumnNames.get(i), existingParquetColumns.get(i)));
                    }
                }
            }
        }
        return true;
    }

    private static boolean isHeaderFile(Path path) {
        return path.toString().endsWith(".csv");
    }

    @Override
    public Map<String, SchemaDescriptor> referencedNodeSchema(TokenHolders tokenHolders) {
        List<ParquetColumn> idColumns = verifiedColumns.stream()
                .flatMap(metadata -> metadata.columns().stream())
                .filter(ParquetColumn::isIdColumn)
                .toList();
        HashMap<String, SchemaDescriptor> result = new HashMap<>();
        checkReferencedNodeSchema(idColumns, tokenHolders, result);
        return result;
    }

    private void checkReferencedNodeSchema(
            List<ParquetColumn> idColumns, TokenHolders tokenHolders, Map<String, SchemaDescriptor> result) {
        idColumns.forEach(column -> {
            var labelName = column.idLabel();
            checkState(labelName != null, "No label was specified for the node index in '%s'", column);
            var keyName = column.propertyName();
            checkState(keyName != null, "No property key was specified for node index in '%s'", column);
            var label = tokenHolders.labelTokens().getIdByName(labelName);
            var key = tokenHolders.propertyKeyTokens().getIdByName(keyName);
            checkState(
                    label != TokenConstants.NO_TOKEN,
                    "Label '%s' for node index specified in '%s' does not exist",
                    labelName,
                    column);
            checkState(
                    key != TokenConstants.NO_TOKEN,
                    "Property key '%s' for node index specified in '%s' does not exist",
                    keyName,
                    column);
            var schemaDescriptor = SchemaDescriptors.forLabel(label, key);
            var prev = result.put(column.groupName(), schemaDescriptor);
            checkState(
                    prev == null || prev.equals(schemaDescriptor),
                    "Multiple different indexes for group " + column.groupName());
        });
    }

    @Override
    public Estimates validateAndEstimate(PropertySizeCalculator valueSizeCalculator, int numberOfThreads)
            throws IOException {
        // fly over node files
        long numberOfNodes = 0;
        long numberOfNodeProperties = 0;
        long totalNodePropertiesSize = 0;
        Set<String> mergedLabels = new HashSet<>();
        for (Map.Entry<Set<String>, List<FileGroup>> nodePathEntries : nodeFiles.entrySet()) {
            mergedLabels.addAll(Collections.unmodifiableSet(nodePathEntries.getKey()));
            for (FileGroup nodeFileGroup : nodePathEntries.getValue()) {
                for (FileGroup.NumberedFile nodeFile : nodeFileGroup.files()) {
                    try {
                        // skip obvious csv head
                        if (isHeaderFile(nodeFile.path())) {
                            continue;
                        }
                        var metadata = ParquetReader.readMetadata(ParquetImportInputFile.of(nodeFile.path()));
                        List<BlockMetaData> blocks = metadata.getBlocks();
                        for (BlockMetaData block : blocks) {
                            numberOfNodes += block.getRowCount();
                            var currentColumnCount = block.getColumns().size();
                            // This needs to be separated by file/group, or?
                            if (currentColumnCount > numberOfNodeProperties) {
                                numberOfNodeProperties = currentColumnCount;
                            }
                            for (ColumnChunkMetaData column : block.getColumns()) {
                                totalNodePropertiesSize += column.getTotalUncompressedSize();
                            }
                        }
                    } catch (RuntimeException e) {
                        // Silently ignore if a file cannot be read.
                        // This is 99% caused by header CSVs being part of the import files.
                        // Unfortunately, we can only ignore at this level because csv files might not be named such
                    }
                }
            }
        }
        var numberOfNodeLabels = mergedLabels.size();

        // fly over relationship files
        long numberOfRelationships = 0;
        long numberOfRelationshipProperties = 0;
        long totalRelationshipPropertiesSize = 0;
        for (Map.Entry<String, List<FileGroup>> relationshipFileEntries : relationshipFiles.entrySet()) {
            for (FileGroup relationshipFileGroup : relationshipFileEntries.getValue()) {
                for (FileGroup.NumberedFile relationshipFile : relationshipFileGroup.files()) {
                    try {
                        // skip obvious csv headers
                        if (isHeaderFile(relationshipFile.path())) {
                            continue;
                        }
                        var metadata = ParquetReader.readMetadata(ParquetImportInputFile.of(relationshipFile.path()));
                        for (BlockMetaData block : metadata.getBlocks()) {
                            numberOfRelationships += block.getRowCount();
                            var currentColumnCount = block.getColumns().size();
                            if (currentColumnCount > numberOfNodeProperties) {
                                numberOfRelationshipProperties = currentColumnCount;
                            }
                            for (ColumnChunkMetaData column : block.getColumns()) {
                                totalRelationshipPropertiesSize += column.getTotalUncompressedSize();
                            }
                        }
                    } catch (RuntimeException e) {
                        // Silently ignore if a file cannot be read.
                        // This is 99% caused by header CSVs being part of the import files.
                        // Unfortunately, we can only ignore at this level because csv files might not be named such
                    }
                }
            }
        }

        return Input.knownEstimates(
                numberOfNodes,
                numberOfRelationships,
                numberOfNodeProperties,
                numberOfRelationshipProperties,
                totalNodePropertiesSize,
                totalRelationshipPropertiesSize,
                numberOfNodeLabels);
    }

    static class ParquetImportInputFile implements InputFile {

        static Map<Path, ParquetImportInputFile> importFileCache = new HashMap<>();

        static ParquetImportInputFile of(Path importFilePath) {
            return importFileCache.computeIfAbsent(importFilePath, (any) -> new ParquetImportInputFile(importFilePath));
        }

        private final Path filePath;

        private ParquetImportInputFile(Path lePath) {
            this.filePath = lePath;
        }

        @Override
        public long getLength() throws IOException {
            return Files.size(filePath);
        }

        @Override
        public SeekableInputStream newStream() throws IOException {
            InputStream inputStream = Files.newInputStream(filePath);
            if (inputStream instanceof ReadableChannel cloudFileChannel) {
                return new DelegatingSeekableInputStream(inputStream) {
                    private long position = 0;

                    @Override
                    public long getPos() {
                        return position;
                    }

                    @Override
                    public void seek(long newPos) throws IOException {
                        cloudFileChannel.position(newPos);
                        position = newPos;
                    }
                };
            } else { // assume we have a local file
                inputStream = new FileInputStream(filePath.toFile());
                FileInputStream fis = (FileInputStream) inputStream;
                return new DelegatingSeekableInputStream(fis) {
                    private long position = 0;

                    @Override
                    public long getPos() {
                        return position;
                    }

                    @Override
                    public void seek(long newPos) throws IOException {
                        //noinspection resource
                        fis.getChannel().position(newPos);
                        position = newPos;
                    }
                };
            }
        }

        @Override
        public String toString() {
            return "ParquetFile{" + "path=" + filePath + '}';
        }
    }
}
