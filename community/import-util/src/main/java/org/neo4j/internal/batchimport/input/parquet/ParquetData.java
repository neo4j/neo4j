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

import java.nio.file.Path;
import java.time.ZoneId;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.function.Predicate;
import java.util.function.Supplier;

/**
 * Represents labels / type and one file that was declared for these labels / type.
 */
record ParquetData(
        EntityType entityType,
        Set<String> labelsOrType,
        Path file,
        List<ParquetColumn> columns,
        Supplier<ZoneId> defaultTimezoneSupplier,
        String groupName,
        String relationshipStartIdGroupName,
        String relationshipEndIdGroupName) {

    ParquetData {
        Objects.requireNonNull(entityType, "entityType must not be null");
        if (entityType != EntityType.NODE && entityType != EntityType.RELATIONSHIP) {
            throw new IllegalArgumentException("entityType must be either NODE or RELATIONSHIP");
        }
    }

    ParquetData(
            EntityType entityType,
            Set<String> labelsOrType,
            Path file,
            List<ParquetColumn> columns,
            Supplier<ZoneId> defaultTimezoneSupplier) {
        this(
                entityType,
                labelsOrType,
                file,
                columns,
                defaultTimezoneSupplier,
                findColumnGroupName(columns, ParquetColumn::isIdColumn),
                findColumnGroupName(columns, ParquetColumn::isStartId),
                findColumnGroupName(columns, ParquetColumn::isEndId));
    }

    private static String findColumnGroupName(List<ParquetColumn> columns, Predicate<ParquetColumn> filter) {
        for (var column : columns) {
            if (filter.test(column)) {
                return column.groupName();
            }
        }

        return null;
    }
}
