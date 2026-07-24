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

import org.neo4j.internal.kernel.api.PropertyIndexQuery;
import org.neo4j.internal.schema.IndexCapability;
import org.neo4j.internal.schema.IndexQuery;
import org.neo4j.util.Preconditions;
import org.neo4j.values.storable.Value;
import org.neo4j.values.storable.ValueCategory;

class RangeIndexCapability implements IndexCapability {
    @Override
    public boolean supportsOrdering() {
        return true;
    }

    @Override
    public boolean supportsReturningValues() {
        return true;
    }

    @Override
    public boolean areValueCategoriesAccepted(ValueCategory... valueCategories) {
        Preconditions.requireNonEmpty(valueCategories);
        Preconditions.requireNoNullElements(valueCategories);
        return true;
    }

    @Override
    public boolean areValuesAccepted(Value... values) {
        Preconditions.requireNonEmpty(values);
        Preconditions.requireNoNullElements(values);
        return true;
    }

    @Override
    public boolean isQuerySupported(IndexQuery.IndexQueryType queryType, ValueCategory valueCategory) {
        if (!areValueCategoriesAccepted(valueCategory)) {
            return false;
        }

        return switch (queryType) {
            case ALL_ENTRIES, EXISTS, EXACT, RANGE, STRING_PREFIX -> true;
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

        for (int i = 0; i < queries.length; i++) {
            IndexQuery query = queries[i];
            IndexQuery.IndexQueryType type = query.type();

            switch (type) {
                case ALL_ENTRIES, EXISTS, EXACT, STRING_PREFIX:
                    break;
                case RANGE:
                    switch (((PropertyIndexQuery) query).valueGroup()) {
                        case GEOMETRY, GEOMETRY_ARRAY:
                            return false;
                        default:
                            break;
                    }
                    break;
                default:
                    return false;
            }

            if (i > 0) {
                IndexQuery.IndexQueryType prevType = queries[i - 1].type();
                switch (type) {
                    case EXISTS:
                        switch (prevType) {
                            case EXISTS, EXACT, RANGE, STRING_PREFIX:
                                break;
                            default:
                                return false;
                        }
                        break;
                    case EXACT, RANGE, STRING_PREFIX:
                        if (prevType != IndexQuery.IndexQueryType.EXACT) {
                            return false;
                        }
                        break;
                    default:
                        return false;
                }
            }
        }
        return true;
    }
}
