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
package org.neo4j.kernel.api.impl.schema.trigram;

import java.io.IOException;
import java.io.UncheckedIOException;
import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.TotalHitCountCollector;
import org.neo4j.internal.helpers.collection.BoundedIterable;
import org.neo4j.internal.kernel.api.IndexQueryConstraints;
import org.neo4j.internal.kernel.api.PropertyIndexQuery;
import org.neo4j.internal.kernel.api.PropertyIndexQuery.ExactPredicate;
import org.neo4j.internal.kernel.api.PropertyIndexQuery.StringContainsPredicate;
import org.neo4j.internal.kernel.api.PropertyIndexQuery.StringPrefixPredicate;
import org.neo4j.internal.kernel.api.PropertyIndexQuery.StringSuffixPredicate;
import org.neo4j.internal.schema.IndexDescriptor;
import org.neo4j.io.pagecache.context.CursorContext;
import org.neo4j.kernel.api.impl.index.SearcherReference;
import org.neo4j.kernel.api.impl.schema.AbstractTextIndexReader;
import org.neo4j.kernel.api.index.IndexSampler;
import org.neo4j.kernel.impl.index.schema.IndexUsageTracking;
import org.neo4j.util.Preconditions;
import org.neo4j.values.storable.Value;

public class TrigramIndexReader extends AbstractTextIndexReader {
    TrigramIndexReader(
            SearcherReference searcherReference, IndexDescriptor descriptor, IndexUsageTracking usageTracker) {
        super(descriptor, searcherReference, usageTracker);
    }

    @Override
    public IndexSampler createSampler() {
        return new TrigramIndexSampler(getIndexSearcher());
    }

    @Override
    protected Query toLuceneQuery(PropertyIndexQuery predicate, IndexQueryConstraints constraints) {
        return switch (predicate.type()) {
            case ALL_ENTRIES -> TrigramQueryFactory.allValues();
            case EXACT -> {
                final var value =
                        ((ExactPredicate) predicate).value().asObject().toString();
                yield TrigramQueryFactory.exact(value);
            }
            case STRING_PREFIX -> {
                final var spp = (StringPrefixPredicate) predicate;
                yield TrigramQueryFactory.stringPrefix(spp.prefix().stringValue());
            }
            case STRING_CONTAINS -> {
                final var scp = (StringContainsPredicate) predicate;
                yield TrigramQueryFactory.stringContains(scp.contains().stringValue());
            }
            case STRING_SUFFIX -> {
                final var ssp = (StringSuffixPredicate) predicate;
                yield TrigramQueryFactory.stringSuffix(ssp.suffix().stringValue());
            }
            default -> throw invalidQuery(IllegalArgumentException::new, predicate);
        };
    }

    @Override
    protected String entityIdFieldKey() {
        return TrigramDocumentStructure.ENTITY_ID_KEY;
    }

    @Override
    protected boolean needStoreFilter(PropertyIndexQuery predicate) {
        return TrigramQueryFactory.needStoreFilter(predicate);
    }

    /**
     * This isn't perfect. We will get false positives for searches where additional trigrams
     * than the trigrams of the property value is also stored in the index.
     * But since we are doing a query for the specific entity id it should be a pretty safe bet that if
     * we find an entity with the matching id and all the trigrams of our search word are indexed, then what
     * is stored in the index is most likely indexed correctly.
     * Don't use this for anything critical, but for a best effort consistency check it is fine.
     */
    @Override
    public long countIndexedEntities(
            long entityId, CursorContext cursorContext, int[] propertyKeyIds, Value... propertyValues) {
        final var entityIdQuery = TrigramQueryFactory.getById(entityId);

        final var entityIdAndValueQuery = new BooleanQuery.Builder();
        entityIdAndValueQuery.add(entityIdQuery, BooleanClause.Occur.MUST);

        Preconditions.checkState(
                propertyKeyIds.length == 1,
                "Text index does not support composite indexing. Tried to query index with multiple property keys.");
        final var value = propertyValues[0].asObject().toString();
        final var valueQuery = TrigramQueryFactory.exact(value);
        entityIdAndValueQuery.add(valueQuery, BooleanClause.Occur.MUST);

        try {
            final var collector = new TotalHitCountCollector();
            getIndexSearcher().search(entityIdAndValueQuery.build(), collector);
            // A <label,propertyKeyId,nodeId> tuple should only match at most a single propertyValue
            return collector.getTotalHits();
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    BoundedIterable<Long> newAllEntriesValueReader(long fromIdInclusive, long toIdExclusive) throws IOException {
        return newAllEntriesValueReaderForPartition(
                TrigramDocumentStructure.ENTITY_ID_KEY,
                getIndexSearcher(),
                TrigramQueryFactory.allValues(),
                fromIdInclusive,
                toIdExclusive);
    }
}
