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
package org.neo4j.kernel.api.impl.schema.trigram;

import static java.lang.String.format;

import java.io.IOException;
import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.TotalHitCountCollector;
import org.neo4j.graphdb.schema.IndexType;
import org.neo4j.internal.kernel.api.PropertyIndexQuery;
import org.neo4j.internal.schema.IndexDescriptor;
import org.neo4j.io.pagecache.context.CursorContext;
import org.neo4j.kernel.api.impl.index.SearcherReference;
import org.neo4j.kernel.api.impl.schema.AbstractTextIndexReader;
import org.neo4j.kernel.api.impl.schema.TaskCoordinator;
import org.neo4j.kernel.impl.api.index.IndexSamplingConfig;
import org.neo4j.values.storable.Value;

public class TrigramIndexReader extends AbstractTextIndexReader {
    TrigramIndexReader(
            SearcherReference searcherReference,
            IndexDescriptor descriptor,
            IndexSamplingConfig samplingConfig,
            TaskCoordinator taskCoordinator) {
        super(descriptor, searcherReference, samplingConfig, taskCoordinator);
    }

    @Override
    protected Query toLuceneQuery(PropertyIndexQuery predicate) {
        switch (predicate.type()) {
            case ALL_ENTRIES:
                return TrigramQueryFactory.allValues();
            case RANGE:
                var range = (PropertyIndexQuery.TextRangePredicate) predicate;
                return TrigramQueryFactory.range(range.from(), range.to());
            case EXACT:
                return TrigramQueryFactory.exact(((PropertyIndexQuery.ExactPredicate) predicate).value());
            case STRING_PREFIX:
                PropertyIndexQuery.StringPrefixPredicate spp = (PropertyIndexQuery.StringPrefixPredicate) predicate;
                return TrigramQueryFactory.stringPrefix(spp.prefix().stringValue());
            case STRING_CONTAINS:
                PropertyIndexQuery.StringContainsPredicate scp = (PropertyIndexQuery.StringContainsPredicate) predicate;
                return TrigramQueryFactory.stringContains(scp.contains().stringValue());
            case STRING_SUFFIX:
                PropertyIndexQuery.StringSuffixPredicate ssp = (PropertyIndexQuery.StringSuffixPredicate) predicate;
                return TrigramQueryFactory.stringSuffix(ssp.suffix().stringValue());
            default:
                throw new IllegalArgumentException(
                        format("Index query not supported for %s index. Query: %s", IndexType.TEXT, predicate));
        }
    }

    @Override
    protected String entityIdFieldKey() {
        return TrigramDocumentStructure.ENTITY_ID_KEY;
    }

    @Override
    protected boolean needStoreFilter(PropertyIndexQuery predicate) {
        return TrigramQueryFactory.needStoreFilter(predicate);
    }

    @Override
    public long countIndexedEntities(
            long entityId, CursorContext cursorContext, int[] propertyKeyIds, Value... propertyValues) {
        // TODO: Most likely trigram index countIndexesEntities doesn't work correctly since we don't filter through the
        //       store. Probably we will need to do the same as fulltext index and just verify the entity id without
        //       checking the property value.
        Query entityIdQuery = TrigramQueryFactory.getById(entityId);

        BooleanQuery.Builder entityIdAndValueQuery = new BooleanQuery.Builder();
        entityIdAndValueQuery.add(entityIdQuery, BooleanClause.Occur.MUST);
        for (Value propertyValue : propertyValues) {
            Query valueQuery = TrigramQueryFactory.exact(propertyValue);
            entityIdAndValueQuery.add(valueQuery, BooleanClause.Occur.MUST);
        }
        try {
            TotalHitCountCollector collector = new TotalHitCountCollector();
            getIndexSearcher().search(entityIdAndValueQuery.build(), collector);
            // A <label,propertyKeyId,nodeId> tuple should only match at most a single propertyValue
            return collector.getTotalHits();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
}
