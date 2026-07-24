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
package org.neo4j.index.nativeimpl;

import org.neo4j.internal.schema.IndexCapability;
import org.neo4j.internal.schema.IndexQuery;
import org.neo4j.util.Preconditions;
import org.neo4j.values.storable.ValueCategory;

class PointIndexCapability implements IndexCapability {
    @Override
    public boolean supportsOrdering() {
        return false;
    }

    @Override
    public boolean supportsReturningValues() {
        // The point index has values for all the queries it supports.
        return true;
    }

    @Override
    public boolean areValueCategoriesAccepted(ValueCategory... valueCategories) {
        Preconditions.requireNonEmpty(valueCategories);
        Preconditions.requireNoNullElements(valueCategories);
        return valueCategories.length == 1 && valueCategories[0] == ValueCategory.GEOMETRY;
    }

    @Override
    public boolean isQuerySupported(IndexQuery.IndexQueryType queryType, ValueCategory valueCategory) {
        if (queryType == IndexQuery.IndexQueryType.ALL_ENTRIES) {
            return true;
        }

        if (!areValueCategoriesAccepted(valueCategory)) {
            return false;
        }

        return switch (queryType) {
            case EXACT, BOUNDING_BOX -> true;
            default -> false;
        };
    }

    @Override
    public double getCostMultiplier(IndexQuery.IndexQueryType... queryTypes) {
        return COST_MULTIPLIER_STANDARD;
    }

    @Override
    public boolean supportPartitionedScan(IndexQuery... queries) {
        Preconditions.requireNonEmpty(queries);
        Preconditions.requireNoNullElements(queries);
        return false;
    }
}
