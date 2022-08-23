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

import static java.lang.String.format;

import java.io.IOException;
import java.util.Arrays;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.neo4j.graphdb.schema.IndexType;
import org.neo4j.internal.kernel.api.IndexQueryConstraints;
import org.neo4j.internal.kernel.api.PropertyIndexQuery;
import org.neo4j.internal.kernel.api.QueryContext;
import org.neo4j.internal.kernel.api.exceptions.schema.IndexNotApplicableKernelException;
import org.neo4j.internal.kernel.api.security.AccessMode;
import org.neo4j.internal.schema.IndexDescriptor;
import org.neo4j.internal.schema.IndexQuery.IndexQueryType;
import org.neo4j.kernel.api.impl.index.SearcherReference;
import org.neo4j.kernel.api.impl.index.collector.DocValuesCollector;
import org.neo4j.kernel.api.impl.schema.reader.IndexReaderCloseException;
import org.neo4j.kernel.api.impl.schema.sampler.LuceneIndexSampler;
import org.neo4j.kernel.api.index.IndexProgressor;
import org.neo4j.kernel.api.index.IndexSampler;
import org.neo4j.kernel.api.index.ValueIndexReader;
import org.neo4j.kernel.impl.api.index.IndexSamplingConfig;
import org.neo4j.kernel.impl.index.schema.PartitionedValueSeek;
import org.neo4j.values.storable.ValueGroup;

public abstract class AbstractTextIndexReader implements ValueIndexReader {
    protected final IndexDescriptor descriptor;
    protected final SearcherReference searcherReference;
    protected final IndexSamplingConfig samplingConfig;
    protected final TaskCoordinator taskCoordinator;

    protected AbstractTextIndexReader(
            IndexDescriptor descriptor,
            SearcherReference searcherReference,
            IndexSamplingConfig samplingConfig,
            TaskCoordinator taskCoordinator) {
        this.descriptor = descriptor;
        this.searcherReference = searcherReference;
        this.samplingConfig = samplingConfig;
        this.taskCoordinator = taskCoordinator;
    }

    @Override
    public IndexSampler createSampler() {
        return new LuceneIndexSampler(getIndexSearcher(), taskCoordinator, samplingConfig);
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
        IndexProgressor progressor = search(query).getIndexProgressor(entityIdFieldKey(), client);
        var needStoreFilter = needStoreFilter(predicate);
        client.initialize(descriptor, progressor, accessMode, false, needStoreFilter, constraints, predicate);
    }

    protected abstract Query toLuceneQuery(PropertyIndexQuery predicate);

    protected abstract String entityIdFieldKey();

    protected abstract boolean needStoreFilter(PropertyIndexQuery predicate);

    @Override
    public PartitionedValueSeek valueSeek(
            int desiredNumberOfPartitions, QueryContext context, PropertyIndexQuery... query) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void close() {
        try {
            searcherReference.close();
        } catch (IOException e) {
            throw new IndexReaderCloseException(e);
        }
    }

    protected IndexSearcher getIndexSearcher() {
        return searcherReference.getIndexSearcher();
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

    private static void validateQuery(PropertyIndexQuery... predicates) {
        IndexType key = IndexType.TEXT;
        if (predicates.length > 1) {
            throw new IllegalArgumentException(format(
                    "Tried to query a %s index with a composite query. Composite queries are not supported by a %s index. Query was: %s ",
                    key, key, Arrays.toString(predicates)));
        }

        PropertyIndexQuery predicate = predicates[0];
        // expression
        if (!(predicate.valueGroup() == ValueGroup.TEXT || predicate.type() == IndexQueryType.ALL_ENTRIES)) {
            throw new IllegalArgumentException(
                    format("Index query not supported for %s index. Query: %s", key, predicate));
        }
    }
}
