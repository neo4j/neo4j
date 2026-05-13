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
package org.neo4j.kernel.api.impl.index.lucene.v9;

import static org.neo4j.kernel.api.impl.index.collector.ScoredEntityIterator.mergeIterators;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.function.Function;
import org.neo4j.internal.kernel.api.IndexQueryConstraints;
import org.neo4j.kernel.api.impl.index.collector.ValuesIterator;
import org.neo4j.kernel.api.impl.index.lucene.LuceneAllDocumentsReader;
import org.neo4j.kernel.api.impl.index.lucene.LuceneDocument;
import org.neo4j.kernel.api.impl.index.lucene.LuceneIndexSearcher;
import org.neo4j.kernel.api.impl.index.lucene.LucenePartitionedSearch;
import org.neo4j.kernel.api.impl.index.lucene.LuceneQueryContext;
import org.neo4j.kernel.api.impl.index.lucene.LuceneSearcherManager;
import org.neo4j.kernel.api.impl.schema.TaskCoordinator;
import org.neo4j.kernel.api.index.IndexProgressor;
import org.neo4j.kernel.api.index.IndexSampler;
import org.neo4j.kernel.impl.api.index.IndexSamplingConfig;
import org.neo4j.shaded.lucene9.index.IndexReader;
import org.neo4j.shaded.lucene9.search.CollectorManager;
import org.neo4j.shaded.lucene9.search.IndexSearcher;
import org.neo4j.shaded.lucene9.search.Query;
import org.neo4j.shaded.lucene9.search.ReferenceManager;
import org.neo4j.shaded.lucene9.search.ScoreDoc;
import org.neo4j.shaded.lucene9.search.TopDocs;

class Lucene9IndexSearcher implements LuceneIndexSearcher {
    final IndexSearcher indexSearcher;
    private final ReferenceManager<IndexSearcher> referenceManager;

    Lucene9IndexSearcher(LuceneSearcherManager searcherManager) throws IOException {
        this.referenceManager = ((Lucene9SearcherManager) searcherManager).searcherManager;
        this.indexSearcher = referenceManager.acquire();

        this.indexSearcher.setQueryCache(null); // Disable query cache
    }

    Lucene9IndexSearcher(IndexSearcher indexSearcher) {
        this.referenceManager = null;
        this.indexSearcher = indexSearcher;
    }

    IndexReader getIndexReader() {
        return indexSearcher.getIndexReader();
    }

    @Override
    public LuceneDocument doc(int docId) throws IOException {
        return new Lucene9Document(indexSearcher.storedFields().document(docId));
    }

    @Override
    public IndexProgressor searchDocValues(LuceneQueryContext queryContext, String field, EntityConsumer entityConsumer)
            throws IOException {
        return indexSearcher.search(
                toInternal(queryContext),
                new DocValuesCollectorManager(c -> c.getIndexProgressor(field, entityConsumer)));
    }

    @Override
    public IndexProgressor searchDocValues(
            LuceneQueryContext queryContext, String field, IndexProgressor.EntityValueClient client)
            throws IOException {
        return indexSearcher.search(
                toInternal(queryContext), new DocValuesCollectorManager(c -> c.getIndexProgressor(field, client)));
    }

    @Override
    public ValuesIterator searchVectors(LuceneQueryContext queryContext, IndexQueryConstraints constraints)
            throws IOException {
        return indexSearcher.search(toInternal(queryContext), new VectorValuesCollectorManager(constraints));
    }

    @Override
    public List<LuceneDocument> searchTopN(LuceneQueryContext queryContext, int n) throws IOException {
        TopDocs search = indexSearcher.search(toInternal(queryContext), n);
        ScoreDoc[] scoreDocs = search.scoreDocs;

        List<LuceneDocument> results = new ArrayList<>(scoreDocs.length);
        for (ScoreDoc scoreDoc : scoreDocs) {
            results.add(doc(scoreDoc.doc));
        }
        return results;
    }

    @Override
    public int count(LuceneQueryContext queryContext) throws IOException {
        return indexSearcher.count(toInternal(queryContext));
    }

    @Override
    public LuceneQueryContext newQueryContext() {
        return new Lucene9QueryContext();
    }

    @Override
    public LuceneQueryContext rewrite(LuceneQueryContext queryContext) throws IOException {
        Query originalQuery = toInternal(queryContext);
        Query rewrite = indexSearcher.rewrite(originalQuery);
        if (originalQuery == rewrite) {
            return queryContext;
        }
        return Lucene9QueryContext.wrap(rewrite);
    }

    @Override
    public void close() throws IOException {
        if (referenceManager != null) {
            referenceManager.release(indexSearcher);
        }
    }

    @Override
    public LucenePartitionedSearch newPartitionedSearcher(int size) {
        return new Lucene9PartitionedSearch(size);
    }

    @Override
    public LuceneAllDocumentsReader newAllDocumentsReader() {
        return new Lucene9AllDocumentsReader(this);
    }

    @Override
    public int numDocs() {
        return indexSearcher.getIndexReader().numDocs();
    }

    @Override
    public IndexSampler newIndexSampler(TaskCoordinator taskCoordinator, IndexSamplingConfig samplingConfig) {
        return new Lucene9IndexSampler(this, taskCoordinator, samplingConfig);
    }

    private static Query toInternal(LuceneQueryContext queryContext) {
        return ((Lucene9QueryContext) queryContext).build();
    }

    private static class DocValuesCollectorManager
            implements CollectorManager<Lucene9DocValuesCollector, IndexProgressor> {
        private final Function<Lucene9DocValuesCollector, IndexProgressor> progressFactory;

        public DocValuesCollectorManager(Function<Lucene9DocValuesCollector, IndexProgressor> progressFactory) {
            this.progressFactory = progressFactory;
        }

        @Override
        public Lucene9DocValuesCollector newCollector() {
            return new Lucene9DocValuesCollector();
        }

        @Override
        public IndexProgressor reduce(Collection<Lucene9DocValuesCollector> collectors) {
            List<IndexProgressor> list =
                    collectors.stream().map(progressFactory).toList();
            return new IndexProgressor.ConcatenatingIndexProgressor(list);
        }
    }

    private static class VectorValuesCollectorManager
            implements CollectorManager<Lucene9VectorResultCollector, ValuesIterator> {

        private final IndexQueryConstraints constraints;

        private VectorValuesCollectorManager(IndexQueryConstraints constraints) {
            this.constraints = constraints;
        }

        @Override
        public Lucene9VectorResultCollector newCollector() {
            return new Lucene9VectorResultCollector(constraints);
        }

        @Override
        public ValuesIterator reduce(Collection<Lucene9VectorResultCollector> collectors) {
            return mergeIterators(collectors.stream()
                    .map(Lucene9VectorResultCollector::iterator)
                    .toList());
        }
    }
}
