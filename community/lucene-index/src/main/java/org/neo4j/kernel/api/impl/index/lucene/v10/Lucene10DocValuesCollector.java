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
import java.io.UncheckedIOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import org.apache.lucene.index.DocValuesType;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.NumericDocValues;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.search.ScoreMode;
import org.apache.lucene.search.SimpleCollector;
import org.apache.lucene.util.DocIdSetBuilder;
import org.neo4j.kernel.api.impl.index.lucene.LuceneIndexSearcher.EntityConsumer;
import org.neo4j.kernel.api.index.IndexProgressor;
import org.neo4j.util.VisibleForTesting;
import org.neo4j.values.storable.Value;

/**
 * Collector to record per-segment {@code DocIdSet}s and {@code LeafReaderContext}s for every
 * segment that contains a hit. Those items can be later used to read {@code DocValues} fields
 * and iterate over the matched {@code DocIdSet}s. This collector is different from
 * {@code org.apache.lucene.search.CachingCollector} in that the later focuses on predictable RAM usage
 * and feeding other collectors while this collector focuses on exposing the required per-segment data structures
 * to the user.
 */
class Lucene10DocValuesCollector extends SimpleCollector {
    private LeafReaderContext context;
    private int segmentHits;
    private int totalHits;
    private final List<MatchingDocs> matchingDocs = new ArrayList<>();
    private Docs docs;

    Lucene10DocValuesCollector() {}

    public IndexProgressor getIndexProgressor(String field, IndexProgressor.EntityValueClient client) {
        return new LongValuesIndexProgressor(getMatchingDocs(), getTotalHits(), field, client::acceptEntity);
    }

    public IndexProgressor getIndexProgressor(String field, EntityConsumer entityConsumer) {
        return new LongValuesIndexProgressor(getMatchingDocs(), getTotalHits(), field, entityConsumer);
    }

    /**
     * @return the total number of hits across all segments.
     */
    int getTotalHits() {
        return totalHits;
    }

    @Override
    public final void collect(int doc) {
        docs.addDoc(doc);
        segmentHits++;
        totalHits++;
    }

    @Override
    public ScoreMode scoreMode() {
        return ScoreMode.COMPLETE_NO_SCORES;
    }

    @Override
    public void doSetNextReader(LeafReaderContext context) throws IOException {
        if (docs != null && segmentHits > 0) {
            createMatchingDocs();
        }
        int maxDoc = context.reader().maxDoc();
        docs = new Docs(maxDoc);
        segmentHits = 0;
        this.context = context;
    }

    /**
     * @return the documents matched by the query, one {@link MatchingDocs} per visited segment that contains a hit.
     */
    @VisibleForTesting
    List<MatchingDocs> getMatchingDocs() {
        if (docs != null && segmentHits > 0) {
            try {
                createMatchingDocs();
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            } finally {
                docs = null;
                context = null;
            }
        }

        return matchingDocs;
    }

    private void createMatchingDocs() throws IOException {
        matchingDocs.add(new MatchingDocs(this.context, docs.getDocIdSet(), segmentHits));
    }

    /**
     * Iterates over all per-segment {@link MatchingDocs}.
     * Provides base functionality for extracting entity ids and other values from documents.
     */
    private abstract static class LongValuesSource {
        private final Iterator<MatchingDocs> matchingDocs;
        private final String field;
        final int totalHits;

        DocIdSetIterator currentIdIterator;
        NumericDocValues currentDocValues;
        MatchingDocs currentDocs;
        final float score;
        int index;
        long next;

        LongValuesSource(Iterable<MatchingDocs> allMatchingDocs, int totalHits, String field) {
            this.totalHits = totalHits;
            this.field = field;
            matchingDocs = allMatchingDocs.iterator();
            score = Float.NaN;
        }

        /**
         * @return true if it was able to make sure, that currentDisi is valid
         */
        boolean ensureValidDisi() {
            while (currentIdIterator == null) {
                if (matchingDocs.hasNext()) {
                    currentDocs = matchingDocs.next();
                    currentIdIterator = currentDocs.docIdSet;
                    index = 0;
                    if (currentIdIterator != null) {
                        currentDocValues = currentDocs.readDocValues(field);
                    }
                } else {
                    return false;
                }
            }
            return true;
        }

        boolean fetchNextEntityId() {
            try {
                if (ensureValidDisi()) {
                    int nextDoc = currentIdIterator.nextDoc();
                    if (nextDoc != DocIdSetIterator.NO_MORE_DOCS) {
                        index++;
                        int valueDoc = currentDocValues.advance(nextDoc);
                        if (valueDoc != nextDoc) {
                            throw new RuntimeException(
                                    "Document id and document value iterators are out of sync. Id iterator gave me document "
                                            + nextDoc + ", while the value iterator gave me document " + valueDoc
                                            + ".");
                        }
                        next = currentDocValues.longValue();
                        return true;
                    } else {
                        currentIdIterator = null;
                        return fetchNextEntityId();
                    }
                }
            } catch (IOException e) {
                throw new RuntimeException(e);
            }

            return false;
        }
    }

    private static class LongValuesIndexProgressor extends LongValuesSource implements IndexProgressor {
        private final EntityConsumer entityConsumer;

        LongValuesIndexProgressor(
                Iterable<MatchingDocs> allMatchingDocs, int totalHits, String field, EntityConsumer entityConsumer) {
            super(allMatchingDocs, totalHits, field);
            this.entityConsumer = entityConsumer;
        }

        @Override
        public boolean next() {
            while (fetchNextEntityId()) {
                if (entityConsumer.acceptEntity(next, score, (Value[]) null)) {
                    return true;
                }
            }
            return false;
        }

        @Override
        public void close() {
            // nothing to close
        }
    }

    /**
     * Holds the documents that were matched per segment.
     */
    static final class MatchingDocs {

        /** The {@code LeafReaderContext} for this segment. */
        public final LeafReaderContext context;

        /** Which documents were seen. */
        final DocIdSetIterator docIdSet;

        /** Total number of hits */
        final int totalHits;

        MatchingDocs(LeafReaderContext context, DocIdSetIterator docIdSet, int totalHits) {
            this.context = context;
            this.docIdSet = docIdSet;
            this.totalHits = totalHits;
        }

        /**
         * @return the {@code NumericDocValues} for a given field
         * @throws IllegalArgumentException if this field is not indexed with numeric doc values
         */
        private NumericDocValues readDocValues(String field) {
            try {
                NumericDocValues dv = context.reader().getNumericDocValues(field);
                if (dv == null) {
                    FieldInfo fi = context.reader().getFieldInfos().fieldInfo(field);
                    DocValuesType actual = null;
                    if (fi != null) {
                        actual = fi.getDocValuesType();
                    }
                    throw new IllegalStateException("The field '" + field
                            + "' is not indexed properly, expected NumericDV, but got '" + actual + "'");
                }
                return dv;
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }
    }

    /**
     * Used during collection to record matching docs and then return a
     * {@see DocIdSet} that contains them.
     */
    private static final class Docs {
        private final DocIdSetBuilder bits;

        Docs(int maxDoc) {
            bits = new DocIdSetBuilder(maxDoc);
        }

        /** Record the given document. */
        private void addDoc(int docId) {
            bits.grow(1).add(docId);
        }

        /** Return the {@see DocIdSet} which contains all the recorded docs. */
        private DocIdSetIterator getDocIdSet() throws IOException {
            return bits.build().iterator();
        }
    }
}
