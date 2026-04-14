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

import java.time.ZoneId;
import java.util.Arrays;
import java.util.Collections;
import java.util.Locale;
import java.util.Map;
import java.util.function.Supplier;
import org.apache.parquet.schema.LogicalTypeAnnotation;
import org.apache.parquet.schema.PrimitiveType;
import org.neo4j.batchimport.api.input.IdType;
import org.neo4j.internal.batchimport.input.Groups;
import org.neo4j.values.storable.Value;

/**
 * Represents the metadata of a Parquet file column.
 * Those metadata _should_ only get created once per file.
 */
record ParquetColumn(
        String columnName,
        HeaderDefinition headerDefinition,
        String propertyName,
        String groupName,
        PrimitiveType primitiveType,
        ParquetLogicalColumnType logicalColumnType,
        ParquetColumnType columnType,
        LogicalTypeAnnotation logicalTypeAnnotation,
        boolean isArray,
        String rawConfiguration,
        Map<String, String> configuration) {

    private static final String ID_TYPE_KEY = "id-type";

    interface HeaderDefinition {

        String targetColumnName();

        String parquetColumnName();

        default DefaultHeaderDefinition addParquetColumnName(String parquetColumnName) {
            return new DefaultHeaderDefinition(targetColumnName(), parquetColumnName);
        }

        static HeaderDefinition from(String columnName, String originalColumn) {
            return new DefaultHeaderDefinition(columnName, originalColumn);
        }

        static HeaderDefinition from(String columnName) {
            return new SingleRowHeaderDefinition(columnName);
        }
    }
    /**
     * This entry would represent the very same data as the ParquetColumn itself,
     * if there is no special header definition
     *
     * @param targetColumnName column name to be set as property name in Neo4j
     * @param parquetColumnName points to the column name in the parquet file
     */
    record DefaultHeaderDefinition(String targetColumnName, String parquetColumnName) implements HeaderDefinition {}

    record SingleRowHeaderDefinition(String targetColumnName) implements HeaderDefinition {
        @Override
        public String parquetColumnName() {
            return targetColumnName();
        }
    }

    static ParquetColumn from(
            HeaderDefinition headerDefinition,
            EntityType knownEntityType,
            PrimitiveType primitiveType,
            LogicalTypeAnnotation logicalTypeAnnotation) {
        String targetColumnName = headerDefinition.targetColumnName();
        boolean isArray = hasArrayDefinition(targetColumnName);
        // get rid of the array definition after we looked for its presence
        String columnName = targetColumnName.replace("[]", "");

        EnclosureMatch groupNameMatch = extractGroupName(columnName);
        EnclosureMatch configurationMatch = extractConfiguration(columnName);

        columnName = groupNameMatch.removeFrom(columnName);
        columnName = configurationMatch.adjustAfterRemovalOf(groupNameMatch).removeFrom(columnName);
        var propertyName = extractPropertyName(columnName);

        String rawConfiguration = configurationMatch.getMatch();
        Map<String, String> configuration = parseConfiguration(rawConfiguration);

        var logicalColumnType = ParquetLogicalColumnType.resolve(extractLogicalColumnType(columnName), knownEntityType);
        var columnType = ParquetColumnType.resolve(extractColumnType(logicalColumnType, columnName, configuration));

        return new ParquetColumn(
                columnName,
                headerDefinition,
                propertyName,
                groupNameMatch.getMatch(),
                primitiveType,
                logicalColumnType,
                columnType,
                logicalTypeAnnotation,
                isArray,
                rawConfiguration,
                configuration);
    }

    private static EnclosureMatch extractConfiguration(String columnNameValue) {
        return EnclosureMatch.parseEnclosure('{', '}', columnNameValue, true);
    }

    private static EnclosureMatch extractGroupName(String columnNameValue) {
        return EnclosureMatch.parseEnclosure('(', ')', columnNameValue, false);
    }

    private static Map<String, String> parseConfiguration(String rawConfiguration) {
        if (rawConfiguration == null) {
            return Collections.emptyMap();
        }
        return Value.parseStringMap(rawConfiguration);
    }

    String idLabel() {
        return configuration.get("label");
    }

    IdType relationshipColumnIdType(Groups groups) {
        IdType columnIdType = columnIdType();
        if (columnIdType != null) {
            return columnIdType;
        }
        var groupType = groups.get(groupName()).specificIdType();

        return groupType != null ? IdType.valueOf(groupType) : null;
    }

    // todo this and the following method should get merged
    private static String extractLogicalColumnType(String columnNameValue) {
        if (!columnNameValue.contains(":")) {
            return null;
        }
        var typeSplitPosition = columnNameValue.lastIndexOf(":");
        return columnNameValue.substring(typeSplitPosition + 1).trim();
    }

    private static String extractColumnType(
            ParquetLogicalColumnType logicalColumnType, String columnNameValue, Map<String, String> configuration) {
        // skip column type detection if there is no type definition to see or the logical type
        // is not a property (this includes also ignored fields)

        // ensure that if there is an id-type defined in the configuration, it is used for conversion
        if (logicalColumnType == ParquetLogicalColumnType.ID) {
            // if there is an id-type defined in the configuration, use that for column type detection
            String idTypeValue = configuration.get(ID_TYPE_KEY);
            if (idTypeValue != null
                    && !idTypeValue.isBlank()
                    && !columnIdType(idTypeValue).equals(IdType.ACTUAL)) {
                return idTypeValue;
            }
        }

        if (!columnNameValue.contains(":")
                || logicalColumnType
                        != org.neo4j.internal.batchimport.input.parquet.ParquetLogicalColumnType.PROPERTY) {
            return null;
        }
        var typeSplitPosition = columnNameValue.lastIndexOf(":");
        return columnNameValue.substring(typeSplitPosition + 1).trim();
    }

    IdType columnIdType() {
        String idTypeValue = configuration.get(ID_TYPE_KEY);
        if (idTypeValue == null || idTypeValue.isBlank()) {
            return null;
        }
        return columnIdType(idTypeValue);
    }

    static IdType columnIdType(String rawValue) {
        return switch (rawValue.toUpperCase(Locale.ROOT)) {
            case "INT", "LONG" -> IdType.INTEGER;
            case "STRING" -> IdType.STRING;
            case "ACTUAL" -> IdType.ACTUAL;
            default -> IdType.ACTUAL;
        };
    }

    private static String extractPropertyName(String columnNameValue) {
        var typeSplitPosition = columnNameValue.lastIndexOf(":");
        var columnName = typeSplitPosition > -1
                ? columnNameValue.substring(0, typeSplitPosition).trim()
                : columnNameValue;
        // never return empty property name
        return columnName.isBlank() ? null : columnName;
    }

    private static boolean hasArrayDefinition(String columnName) {
        if (!columnName.contains(":")) {
            return columnName.endsWith("[]");
        }
        var typeSplitPosition = columnName.lastIndexOf(":");
        return columnName.substring(typeSplitPosition).contains("[]");
    }

    boolean isRaw() {
        return columnType == ParquetColumnType.RAW;
    }

    boolean hasConfiguration() {
        return rawConfiguration() != null;
    }

    ParquetColumn withoutArray() {
        return new ParquetColumn(
                columnName(),
                headerDefinition(),
                propertyName(),
                groupName(),
                primitiveType(),
                logicalColumnType(),
                columnType(),
                logicalTypeAnnotation(),
                false,
                rawConfiguration(),
                configuration());
    }

    ParquetColumn withColumnType(ParquetColumnType columnType) {
        return new ParquetColumn(
                columnName(),
                headerDefinition(),
                propertyName(),
                groupName(),
                primitiveType(),
                logicalColumnType(),
                columnType,
                logicalTypeAnnotation(),
                isArray(),
                rawConfiguration(),
                configuration());
    }

    boolean hasPropertyName() {
        return propertyName != null && !propertyName.isBlank();
    }

    boolean isIdColumn() {
        return logicalColumnType == ParquetLogicalColumnType.ID;
    }

    boolean isLabelColumn() {
        return logicalColumnType == ParquetLogicalColumnType.LABEL;
    }

    boolean isStartId() {
        return logicalColumnType == ParquetLogicalColumnType.START_ID;
    }

    boolean isEndId() {
        return logicalColumnType == ParquetLogicalColumnType.END_ID;
    }

    boolean isType() {
        return logicalColumnType == ParquetLogicalColumnType.TYPE;
    }

    boolean isIgnoredColumn() {
        return logicalColumnType == ParquetLogicalColumnType.IGNORED;
    }

    Supplier<ZoneId> getTimezone(Supplier<ZoneId> defaultTimezoneSupplier) {
        if (!hasConfiguration()) {
            return defaultTimezoneSupplier;
        }
        return () -> {
            String zoneIdValue = configuration.get("timezone");
            if (zoneIdValue == null) {
                return defaultTimezoneSupplier.get();
            }
            return ZoneId.of(zoneIdValue);
        };
    }

    static String getReservedColumns(EntityType entityType) {
        return "Column types: " + Arrays.toString(ParquetColumnType.values()) + ", logical types: "
                + org.neo4j.internal.batchimport.input.parquet.ParquetLogicalColumnType.getReservedColumns(entityType);
    }

    private record EnclosureMatch(char startSymbol, char endSymbol, int startIndex, int endIndex, String parsedMatch) {

        static EnclosureMatch parseEnclosure(
                char startCharacter, char endCharacter, String content, boolean includeSymbols) {
            if (!content.contains(":")) {
                return unmatched(startCharacter, endCharacter);
            }
            var startPos = findLastRegularColon(content);
            int startIndex = content.indexOf(startCharacter + "", startPos);
            if (startIndex == -1) {
                return unmatched(startCharacter, endCharacter);
            }
            int endIndex = content.lastIndexOf(endCharacter + "");
            if (endIndex == -1) {
                return unmatched(startCharacter, endCharacter);
            }
            String match = content.substring(startIndex + 1, endIndex).trim();
            if (!includeSymbols) {
                return new EnclosureMatch(startCharacter, endCharacter, startIndex, endIndex, match);
            }
            return new EnclosureMatch(
                    startCharacter,
                    endCharacter,
                    startIndex,
                    endIndex,
                    "%c%s%c".formatted(startCharacter, match, endCharacter));
        }

        private static int findLastRegularColon(String content) {
            var lastColonIndex = content.lastIndexOf(":");
            int lastCurlyStartIndex = content.lastIndexOf("{");
            int lastCurlyEndIndex = content.lastIndexOf("}");
            int lastParenthesisStartIndex = content.lastIndexOf("(");
            int lastParenthesisEndIndex = content.lastIndexOf(")");
            if (lastCurlyStartIndex < lastColonIndex && lastCurlyEndIndex > lastColonIndex) {
                lastColonIndex = content.substring(0, lastCurlyStartIndex).lastIndexOf(":");
            }
            if (lastParenthesisStartIndex != -1
                    && lastParenthesisEndIndex != -1
                    && lastParenthesisStartIndex < lastColonIndex) {
                lastColonIndex = content.substring(0, lastParenthesisStartIndex).lastIndexOf(":");
            }
            return lastColonIndex;
        }

        private static EnclosureMatch unmatched(char start, char end) {
            return new EnclosureMatch(start, end, -1, -1, null);
        }

        String removeFrom(String content) {
            if (!matches()) {
                return content;
            }
            String startString = content.substring(0, startIndex);
            if (endIndex == content.length() - 1) {
                return startString;
            }
            return "%s%s".formatted(startString, content.substring(endIndex + 1));
        }

        EnclosureMatch adjustAfterRemovalOf(EnclosureMatch other) {
            if (!this.matches() || !other.matches()) {
                return this;
            }
            var otherMatchLength = other.endIndex - other.startIndex + 1;
            return new EnclosureMatch(
                    this.startSymbol,
                    this.endSymbol,
                    this.startIndex - otherMatchLength,
                    this.endIndex - otherMatchLength,
                    this.parsedMatch);
        }

        String getMatch() {
            return parsedMatch;
        }

        private boolean matches() {
            return parsedMatch != null;
        }
    }
}
