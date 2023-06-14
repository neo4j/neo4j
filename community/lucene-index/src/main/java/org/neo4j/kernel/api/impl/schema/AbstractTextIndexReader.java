/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
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
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.kernel.api.impl.schema;

import static org.neo4j.internal.schema.IndexQuery.IndexQueryType;

import org.neo4j.internal.kernel.api.PropertyIndexQuery;
import org.neo4j.internal.schema.IndexDescriptor;
import org.neo4j.kernel.api.impl.index.SearcherReference;
import org.neo4j.kernel.impl.api.index.IndexSamplingConfig;
import org.neo4j.kernel.impl.index.schema.IndexUsageTracker;
import org.neo4j.values.storable.ValueGroup;

public abstract class AbstractTextIndexReader extends AbstractLuceneIndexReader {

    protected AbstractTextIndexReader(
            IndexDescriptor descriptor,
            SearcherReference searcherReference,
            IndexSamplingConfig samplingConfig,
            TaskCoordinator taskCoordinator,
            IndexUsageTracker usageTracker) {
        super(descriptor, searcherReference, samplingConfig, taskCoordinator, usageTracker, false);
    }

    @Override
    protected void validateQuery(PropertyIndexQuery... predicates) {
        if (predicates.length > 1) {
            throw invalidCompositeQuery(predicates);
        }

        final var predicate = predicates[0];
        if (!(predicate.valueGroup() == ValueGroup.TEXT || predicate.type() == IndexQueryType.ALL_ENTRIES)) {
            throw invalidQuery(predicate);
        }
    }
}
