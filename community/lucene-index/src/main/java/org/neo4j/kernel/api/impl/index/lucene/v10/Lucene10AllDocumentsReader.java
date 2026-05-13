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
package org.neo4j.kernel.api.impl.index.lucene.v10;

import java.io.IOException;
import java.util.Iterator;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.MultiBits;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.search.FilteredDocIdSetIterator;
import org.apache.lucene.util.Bits;
import org.neo4j.internal.helpers.collection.PrefetchingIterator;
import org.neo4j.kernel.api.impl.index.LuceneDocumentRetrievalException;
import org.neo4j.kernel.api.impl.index.lucene.LuceneAllDocumentsReader;
import org.neo4j.kernel.api.impl.index.lucene.LuceneDocument;

/**
 * Provides a view of all {@link LuceneDocument}s in a single partition.
 */
class Lucene10AllDocumentsReader implements LuceneAllDocumentsReader {
    private final Lucene10IndexSearcher searcher;
    private final IndexReader reader;

    Lucene10AllDocumentsReader(Lucene10IndexSearcher searcher) {
        this.searcher = searcher;
        this.reader = searcher.getIndexReader();
    }

    @Override
    public long maxCount() {
        return reader.maxDoc();
    }

    @Override
    public Iterator<LuceneDocument> iterator() {
        return documentIterator(iterateAllDocs());
    }

    @Override
    public Iterator<LuceneDocument> iterator(int from, int to) {
        return documentIterator(iterateDocs(from, to));
    }

    private Iterator<LuceneDocument> documentIterator(DocIdSetIterator idIterator) {
        return new PrefetchingIterator<>() {
            @Override
            protected LuceneDocument fetchNextOrNull() {
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
    public void close() {}

    private LuceneDocument getDocument(int docId) {
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
