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

import java.util.Arrays;
import java.util.Locale;

enum ParquetLogicalColumnType {
    // node types
    ID("ID", EntityType.NODE),
    LABEL("LABEL", EntityType.NODE),
    // relationship types
    START_ID("START_ID", EntityType.RELATIONSHIP),
    END_ID("END_ID", EntityType.RELATIONSHIP),
    TYPE("TYPE", EntityType.RELATIONSHIP),
    // common types
    IGNORED("IGNORE", EntityType.COMMON),
    PROPERTY(null, EntityType.COMMON);

    private final String typeName;
    final EntityType entityType;

    ParquetLogicalColumnType(String typeName, EntityType entityType) {
        this.typeName = typeName;
        this.entityType = entityType;
    }

    static ParquetLogicalColumnType resolve(String logicalColumnTypeValue, EntityType entityTypeFilter)
            throws IllegalArgumentException {
        if (logicalColumnTypeValue == null) {
            return PROPERTY;
        }
        logicalColumnTypeValue = logicalColumnTypeValue.toUpperCase(Locale.ROOT);
        for (ParquetLogicalColumnType type : values()) {
            if (logicalColumnTypeValue.equals(type.typeName)
                    && (entityTypeFilter == null
                            || type.entityType == entityTypeFilter
                            || type.entityType == EntityType.COMMON)) {
                return type;
            }
        }
        // fall back to possible type PROPERTY
        return PROPERTY;
    }

    public static String getReservedColumns(EntityType entityType) {
        return Arrays.stream(values())
                .filter(value -> value.entityType == entityType)
                .toList()
                .toString();
    }
}
