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
import org.neo4j.internal.helpers.collection.BoundedIterable;
import org.neo4j.internal.kernel.api.PropertyIndexQuery;
import org.neo4j.internal.schema.IndexDescriptor;
import org.neo4j.internal.schema.IndexQuery;
import org.neo4j.io.pagecache.context.CursorContext;
import org.neo4j.kernel.api.impl.index.SearcherReference;
import org.neo4j.kernel.api.impl.index.lucene.LuceneDocumentsFactory;
import org.neo4j.kernel.api.impl.index.lucene.LuceneQueryContext;
import org.neo4j.kernel.api.impl.schema.AbstractTextIndexReader;
import org.neo4j.kernel.api.impl.schema.LuceneQueryFactory;
import org.neo4j.kernel.api.index.IndexSampler;
import org.neo4j.kernel.impl.index.schema.IndexUsageTracking;
import org.neo4j.logging.LogProvider;
import org.neo4j.util.Preconditions;
import org.neo4j.values.storable.Value;

public class TrigramIndexReader extends AbstractTextIndexReader {
    TrigramIndexReader(
            SearcherReference searcherReference,
            IndexDescriptor descriptor,
            IndexUsageTracking usageTracker,
            LogProvider logProvider) {
        super(
                descriptor,
                searcherReference,
                usageTracker,
                LuceneQueryFactory.TrigramQueryFactory.INSTANCE,
                logProvider);
    }

    @Override
    public IndexSampler createSampler() {
        return new TrigramIndexSampler(getIndexSearcher());
    }

    @Override
    protected String entityIdFieldKey() {
        return LuceneDocumentsFactory.ENTITY_ID_KEY;
    }

    @Override
    protected boolean needStoreFilter(PropertyIndexQuery predicate) {
        return !predicate.type().equals(IndexQuery.IndexQueryType.ALL_ENTRIES);
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
        LuceneQueryContext queryContext = getIndexSearcher().newQueryContext();
        queryContext.addMustTerm(LuceneDocumentsFactory.ENTITY_ID_KEY, String.valueOf(entityId));

        Preconditions.checkState(
                propertyKeyIds.length == 1,
                "Text index does not support composite indexing. Tried to query index with multiple property keys.");
        String value = propertyValues[0].asObject().toString();
        queryContext.addExactTrigram(value);

        try {
            return getIndexSearcher().count(queryContext);
            // A <label,propertyKeyId,nodeId> tuple should only match at most a single propertyValue
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    BoundedIterable<Long> newAllEntriesValueReader(long fromIdInclusive, long toIdExclusive) {
        return newAllEntriesValueReaderForPartition(
                LuceneDocumentsFactory.ENTITY_ID_KEY,
                getIndexSearcher(),
                getIndexSearcher().newQueryContext().matchAll(),
                fromIdInclusive,
                toIdExclusive);
    }
}
