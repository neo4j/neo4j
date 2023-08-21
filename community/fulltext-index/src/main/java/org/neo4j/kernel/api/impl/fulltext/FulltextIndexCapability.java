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
package org.neo4j.kernel.api.impl.fulltext;

import org.neo4j.internal.schema.IndexBehaviour;
import org.neo4j.internal.schema.IndexCapability;
import org.neo4j.internal.schema.IndexQuery;
import org.neo4j.internal.schema.IndexQuery.IndexQueryType;
import org.neo4j.util.Preconditions;
import org.neo4j.values.storable.ValueCategory;

public class FulltextIndexCapability implements IndexCapability {
    private static final IndexBehaviour[] EVENTUALLY_CONSISTENT_BEHAVIOUR = {
        IndexBehaviour.EVENTUALLY_CONSISTENT, IndexBehaviour.SKIP_AND_LIMIT
    };
    private static final IndexBehaviour[] NORMAL_BEHAVIOUR = {IndexBehaviour.SKIP_AND_LIMIT};

    private final boolean isEventuallyConsistent;

    public FulltextIndexCapability(boolean isEventuallyConsistent) {
        this.isEventuallyConsistent = isEventuallyConsistent;
    }

    @Override
    public boolean supportsOrdering() {
        return false;
    }

    @Override
    public boolean supportsReturningValues() {
        return false;
    }

    @Override
    public boolean areValueCategoriesAccepted(ValueCategory... valueCategories) {
        Preconditions.requireNonEmpty(valueCategories);
        Preconditions.requireNoNullElements(valueCategories);
        var anyValidCategory = false;
        for (final var valueCategory : valueCategories) {
            switch (valueCategory) {
                case TEXT, TEXT_ARRAY -> anyValidCategory = true;
                default -> {}
            }
        }
        return anyValidCategory;
    }

    @Override
    public boolean isQuerySupported(IndexQueryType queryType, ValueCategory valueCategory) {
        // read/write is not symmetric for fulltext - can only query for text values and not on arrays
        return queryType == IndexQueryType.FULLTEXT_SEARCH && valueCategory == ValueCategory.TEXT;
    }

    @Override
    public double getCostMultiplier(IndexQueryType... queryTypes) {
        return COST_MULTIPLIER_STANDARD;
    }

    @Override
    public boolean supportPartitionedScan(IndexQuery... queries) {
        Preconditions.requireNonEmpty(queries);
        Preconditions.requireNoNullElements(queries);
        return false;
    }

    @Override
    public IndexBehaviour[] behaviours() {
        return isEventuallyConsistent ? EVENTUALLY_CONSISTENT_BEHAVIOUR : NORMAL_BEHAVIOUR;
    }
}
