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
import java.util.Arrays;
import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.TotalHitCountCollector;
import org.neo4j.internal.kernel.api.IndexQueryConstraints;
import org.neo4j.internal.kernel.api.PropertyIndexQuery;
import org.neo4j.internal.kernel.api.PropertyIndexQuery.RangePredicate;
import org.neo4j.internal.kernel.api.QueryContext;
import org.neo4j.internal.kernel.api.exceptions.schema.IndexNotApplicableKernelException;
import org.neo4j.internal.kernel.api.security.AccessMode;
import org.neo4j.internal.schema.IndexDescriptor;
import org.neo4j.internal.schema.IndexQuery.IndexQueryType;
import org.neo4j.io.pagecache.context.CursorContext;
import org.neo4j.kernel.api.impl.index.SearcherReference;
import org.neo4j.kernel.api.impl.index.collector.DocValuesCollector;
import org.neo4j.kernel.api.impl.schema.TaskCoordinator;
import org.neo4j.kernel.api.impl.schema.reader.IndexReaderCloseException;
import org.neo4j.kernel.api.impl.schema.sampler.NonUniqueLuceneIndexSampler;
import org.neo4j.kernel.api.index.AbstractValueIndexReader;
import org.neo4j.kernel.api.index.IndexProgressor;
import org.neo4j.kernel.api.index.IndexSampler;
import org.neo4j.kernel.impl.api.index.IndexSamplingConfig;
import org.neo4j.kernel.impl.index.schema.PartitionedValueSeek;
import org.neo4j.values.storable.TextValue;
import org.neo4j.values.storable.Value;
import org.neo4j.values.storable.ValueGroup;
import org.neo4j.values.storable.Values;

class TrigramValueIndexReader extends AbstractValueIndexReader {
    private final SearcherReference searcherReference;
    private final IndexSamplingConfig samplingConfig;
    private final TaskCoordinator taskCoordinator;

    TrigramValueIndexReader(
            SearcherReference searcherReference,
            IndexDescriptor descriptor,
            IndexSamplingConfig samplingConfig,
            TaskCoordinator taskCoordinator) {
        super(descriptor);
        this.searcherReference = searcherReference;
        this.samplingConfig = samplingConfig;
        this.taskCoordinator = taskCoordinator;
    }

    @Override
    public IndexSampler createSampler() {
        return new NonUniqueLuceneIndexSampler(getIndexSearcher(), taskCoordinator, samplingConfig);
    }

    @Override
    public void query(
            IndexProgressor.EntityValueClient client,
            QueryContext context,
            AccessMode accessMode,
            IndexQueryConstraints constraints,
            PropertyIndexQuery... predicates)
            throws IndexNotApplicableKernelException {
        validateQuery(predicates);
        context.monitor().queried(descriptor);

        PropertyIndexQuery predicate = predicates[0];
        Query query = toLuceneQuery(predicate);
        IndexProgressor progressor = search(query).getIndexProgressor(TrigramDocumentStructure.ENTITY_ID_KEY, client);
        var needStoreFilter = TrigramQueryFactory.needStoreFilter(predicate);
        client.initialize(descriptor, progressor, accessMode, false, needStoreFilter, constraints, predicate);
    }

    @Override
    public PartitionedValueSeek valueSeek(
            int desiredNumberOfPartitions, QueryContext context, PropertyIndexQuery... query) {
        throw new UnsupportedOperationException();
    }

    private DocValuesCollector search(Query query) {
        try {
            DocValuesCollector docValuesCollector = new DocValuesCollector();
            getIndexSearcher().search(query, docValuesCollector);
            return docValuesCollector;
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private static Query toLuceneQuery(PropertyIndexQuery predicate) {
        switch (predicate.type()) {
            case ALL_ENTRIES:
                return TrigramQueryFactory.allValues();
            case RANGE:
                // We have already validated that the query has TEXT type
                //noinspection unchecked
                var range = (RangePredicate<TextValue>) predicate;
                return TrigramQueryFactory.range(fromStringForRange(range), toStringForRange(range));
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
                throw new IllegalArgumentException(format(
                        "Index query not supported for %s index. Query: %s",
                        TrigramIndexProvider.DESCRIPTOR.getKey(), predicate));
        }
    }

    private static void validateQuery(PropertyIndexQuery... predicates) {
        if (predicates.length > 1) {
            throw new IllegalArgumentException(format(
                    "Tried to query a text index with a composite query. Composite queries are not supported by a text index. Query was: %s ",
                    Arrays.toString(predicates)));
        }

        PropertyIndexQuery predicate = predicates[0];
        if (!(predicate.valueGroup() == ValueGroup.TEXT || predicate.type() == IndexQueryType.ALL_ENTRIES)) {
            throw new IllegalArgumentException(format(
                    "Index query not supported for %s index. Query: %s",
                    TrigramIndexProvider.DESCRIPTOR.getKey(), predicate));
        }
    }

    @Override
    public long countIndexedEntities(
            long entityId, CursorContext cursorContext, int[] propertyKeyIds, Value... propertyValues) {
        // TODO: Apart from the 'entityIdQuery', it is the same as in text index. Let's unify it.
        Query entityIdQuery = TrigramQueryFactory.getById(entityId);

        BooleanQuery.Builder entityIdAndValueQuery = new BooleanQuery.Builder();
        entityIdAndValueQuery.add(entityIdQuery, BooleanClause.Occur.MUST);
        for (int i = 0; i < propertyValues.length; i++) {
            Query valueQuery = TrigramQueryFactory.exact(propertyValues[i]);
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

    @Override
    public void close() {
        try {
            searcherReference.close();
        } catch (IOException e) {
            throw new IndexReaderCloseException(e);
        }
    }

    private IndexSearcher getIndexSearcher() {
        return searcherReference.getIndexSearcher();
    }

    private static String fromStringForRange(PropertyIndexQuery.RangePredicate<TextValue> rangePredicate) {
        Value fromValue = rangePredicate.fromValue();
        if (fromValue == Values.NO_VALUE) {
            return null;
        } else {
            return ((TextValue) fromValue).stringValue();
        }
    }

    private static String toStringForRange(PropertyIndexQuery.RangePredicate<TextValue> rangePredicate) {
        Value toValue = rangePredicate.toValue();
        if (toValue == Values.NO_VALUE) {
            return null;
        } else {
            return ((TextValue) toValue).stringValue();
        }
    }
}
