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
package org.neo4j.kernel.api.impl.index;

import java.io.IOException;
import java.util.Iterator;
import org.apache.lucene.document.Document;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.MultiBits;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.search.FilteredDocIdSetIterator;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.util.Bits;
import org.neo4j.internal.helpers.collection.BoundedIterable;
import org.neo4j.internal.helpers.collection.PrefetchingIterator;

/**
 * Provides a view of all {@link Document}s in a single partition.
 */
public class LucenePartitionAllDocumentsReader implements BoundedIterable<Document> {
    private final SearcherReference searcherReference;
    private final IndexSearcher searcher;
    private final IndexReader reader;

    public LucenePartitionAllDocumentsReader(SearcherReference searcherReference) {
        this.searcherReference = searcherReference;
        this.searcher = searcherReference.getIndexSearcher();
        this.reader = searcher.getIndexReader();
    }

    @Override
    public long maxCount() {
        return reader.maxDoc();
    }

    @Override
    public Iterator<Document> iterator() {
        return documentIterator(iterateAllDocs());
    }

    public Iterator<Document> iterator(int from, int to) {
        return documentIterator(iterateDocs(from, to));
    }

    private Iterator<Document> documentIterator(DocIdSetIterator idIterator) {
        return new PrefetchingIterator<>() {
            @Override
            protected Document fetchNextOrNull() {
                try {
                    int doc = idIterator.nextDoc();
                    if (doc == DocIdSetIterator.NO_MORE_DOCS) {
                        return null;
                    }
                    return getDocument(doc);
                } catch (IOException e) {
                    throw new LuceneDocumentRetrievalException("Can't fetch document id from lucene index.", e);
                }
            }
        };
    }

    @Override
    public void close() throws IOException {
        searcherReference.close();
    }

    private Document getDocument(int docId) {
        try {
            return searcher.doc(docId);
        } catch (IOException e) {
            throw new LuceneDocumentRetrievalException("Can't retrieve document with id: " + docId + ".", docId, e);
        }
    }

    private DocIdSetIterator iterateAllDocs() {
        return filterRemovals(DocIdSetIterator.all(reader.maxDoc()));
    }

    private DocIdSetIterator iterateDocs(int from, int to) {
        return from == to ? DocIdSetIterator.empty() : filterRemovals(DocIdSetIterator.range(from, to));
    }

    private DocIdSetIterator filterRemovals(DocIdSetIterator docs) {
        if (!reader.hasDeletions()) {
            return docs;
        }

        return new FilteredDocIdSetIterator(docs) {
            private final Bits liveDocs = MultiBits.getLiveDocs(reader);

            @Override
            protected boolean match(int doc) {
                return liveDocs.get(doc);
            }
        };
    }
}
