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
package org.neo4j.kernel.api.impl.schema.reader;

import static java.lang.String.format;
import static org.neo4j.kernel.api.impl.schema.LuceneDocumentStructure.NODE_ID_KEY;

import java.io.IOException;
import java.util.Arrays;
import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.search.TotalHitCountCollector;
import org.neo4j.internal.kernel.api.IndexQueryConstraints;
import org.neo4j.internal.kernel.api.PropertyIndexQuery;
import org.neo4j.internal.kernel.api.QueryContext;
import org.neo4j.internal.kernel.api.exceptions.schema.IndexNotApplicableKernelException;
import org.neo4j.internal.kernel.api.security.AccessMode;
import org.neo4j.internal.schema.IndexDescriptor;
import org.neo4j.internal.schema.IndexQuery.IndexQueryType;
import org.neo4j.internal.schema.IndexType;
import org.neo4j.io.pagecache.context.CursorContext;
import org.neo4j.kernel.api.impl.index.SearcherReference;
import org.neo4j.kernel.api.impl.index.collector.DocValuesCollector;
import org.neo4j.kernel.api.impl.schema.LuceneDocumentStructure;
import org.neo4j.kernel.api.impl.schema.TaskCoordinator;
import org.neo4j.kernel.api.impl.schema.sampler.NonUniqueLuceneIndexSampler;
import org.neo4j.kernel.api.index.AbstractValueIndexReader;
import org.neo4j.kernel.api.index.IndexProgressor;
import org.neo4j.kernel.api.index.IndexSampler;
import org.neo4j.kernel.impl.api.index.IndexSamplingConfig;
import org.neo4j.kernel.impl.index.schema.PartitionedValueSeek;
import org.neo4j.values.storable.Value;
import org.neo4j.values.storable.ValueGroup;

/**
 * Schema index reader that is able to read/sample a single partition of a partitioned Lucene index.
 *
 * @see PartitionedValueIndexReader
 */
public class SimpleValueIndexReader extends AbstractValueIndexReader {
    private final SearcherReference searcherReference;
    private final IndexSamplingConfig samplingConfig;
    private final TaskCoordinator taskCoordinator;

    public SimpleValueIndexReader(
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
        validateQuery(descriptor, predicates);
        context.monitor().queried(descriptor);

        PropertyIndexQuery predicate = predicates[0];
        Query query = toLuceneQuery(descriptor, predicate);
        IndexProgressor progressor = search(query).getIndexProgressor(NODE_ID_KEY, client);
        client.initialize(descriptor, progressor, accessMode, false, constraints, predicate);
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

    private static Query toLuceneQuery(IndexDescriptor index, PropertyIndexQuery predicate) {
        // Todo: After removal of Fusion index, remove `IndexDescriptor` parameter, and replace `key` with
        // `IndexType.Text` for consistency
        String key = index.getIndexProvider().getKey();
        switch (predicate.type()) {
            case ALL_ENTRIES:
            case EXISTS:
                return LuceneDocumentStructure.newScanQuery();
            case EXACT:
                return LuceneDocumentStructure.newSeekQuery(((PropertyIndexQuery.ExactPredicate) predicate).value());
            case RANGE:
                PropertyIndexQuery.TextRangePredicate sp = (PropertyIndexQuery.TextRangePredicate) predicate;
                return CypherStringQueryFactory.range(sp.from(), sp.fromInclusive(), sp.to(), sp.toInclusive());
            case STRING_PREFIX:
                PropertyIndexQuery.StringPrefixPredicate spp = (PropertyIndexQuery.StringPrefixPredicate) predicate;
                return CypherStringQueryFactory.stringPrefix(spp.prefix().stringValue());
            case STRING_CONTAINS:
                PropertyIndexQuery.StringContainsPredicate scp = (PropertyIndexQuery.StringContainsPredicate) predicate;
                return CypherStringQueryFactory.stringContains(scp.contains().stringValue());
            case STRING_SUFFIX:
                PropertyIndexQuery.StringSuffixPredicate ssp = (PropertyIndexQuery.StringSuffixPredicate) predicate;
                return CypherStringQueryFactory.stringSuffix(ssp.suffix().stringValue());
            default:
                throw new IllegalArgumentException(
                        format("Index query not supported for %s index. Query: %s", key, predicate));
        }
    }

    private static void validateQuery(IndexDescriptor index, PropertyIndexQuery... predicates) {
        // Todo: After removal of Fusion index, remove `IndexDescriptor` parameter, and replace `key` with
        // `IndexType.Text` for consistency
        String key = index.getIndexProvider().getKey();
        if (predicates.length > 1) {
            throw new IllegalArgumentException(format(
                    "Tried to query a %s index with a composite query. Composite queries are not supported by a %s index. Query was: %s ",
                    key, key, Arrays.toString(predicates)));
        }

        PropertyIndexQuery predicate = predicates[0];
        // Todo: After removal of Fusion index, this can be simplified by removing the second line in the conditional
        // expression
        if (!(predicate.valueGroup() == ValueGroup.TEXT
                || predicate.type() == IndexQueryType.ALL_ENTRIES
                || (index.getIndexType() != IndexType.TEXT && predicate.type() == IndexQueryType.EXISTS))) {
            throw new IllegalArgumentException(
                    format("Index query not supported for %s index. Query: %s", key, predicate));
        }
    }

    @Override
    public long countIndexedEntities(
            long entityId, CursorContext cursorContext, int[] propertyKeyIds, Value... propertyValues) {
        Query entityIdQuery = new TermQuery(LuceneDocumentStructure.newTermForChangeOrRemove(entityId));
        Query valueQuery = LuceneDocumentStructure.newSeekQuery(propertyValues);
        BooleanQuery.Builder entityIdAndValueQuery = new BooleanQuery.Builder();
        entityIdAndValueQuery.add(entityIdQuery, BooleanClause.Occur.MUST);
        entityIdAndValueQuery.add(valueQuery, BooleanClause.Occur.MUST);
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
}
