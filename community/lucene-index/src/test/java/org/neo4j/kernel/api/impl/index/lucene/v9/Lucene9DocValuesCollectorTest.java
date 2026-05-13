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

import static java.util.Collections.emptyList;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.List;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.Test;
import org.neo4j.internal.kernel.api.IndexQueryConstraints;
import org.neo4j.internal.kernel.api.PropertyIndexQuery;
import org.neo4j.internal.schema.IndexDescriptor;
import org.neo4j.kernel.api.index.IndexProgressor;
import org.neo4j.shaded.lucene9.index.LeafReaderContext;
import org.neo4j.shaded.lucene9.index.NumericDocValues;
import org.neo4j.shaded.lucene9.search.ConstantScoreScorer;
import org.neo4j.shaded.lucene9.search.ConstantScoreWeight;
import org.neo4j.shaded.lucene9.search.DocIdSetIterator;
import org.neo4j.shaded.lucene9.search.ScoreMode;
import org.neo4j.shaded.lucene9.search.Scorer;
import org.neo4j.shaded.lucene9.search.Weight;
import org.neo4j.values.storable.Value;

final class Lucene9DocValuesCollectorTest {
    @Test
    void shouldStartWithEmptyMatchingDocs() {
        // given
        Lucene9DocValuesCollector collector = new Lucene9DocValuesCollector();

        // when
        // then
        assertEquals(emptyList(), collector.getMatchingDocs());
    }

    @Test
    void shouldCollectAllHitsPerSegment() throws Exception {
        // given
        Lucene9DocValuesCollector collector = new Lucene9DocValuesCollector();
        Lucene9IndexReaderStub readerStub = indexReaderWithMaxDocs(42);

        // when
        collector.doSetNextReader(readerStub.getContext());
        collector.collect(1);
        collector.collect(3);
        collector.collect(5);
        collector.collect(9);

        // then
        assertEquals(4, collector.getTotalHits());
        List<Lucene9DocValuesCollector.MatchingDocs> allMatchingDocs = collector.getMatchingDocs();
        assertEquals(1, allMatchingDocs.size());
        Lucene9DocValuesCollector.MatchingDocs matchingDocs = allMatchingDocs.getFirst();
        assertSame(readerStub.getContext(), matchingDocs.context);
        assertEquals(4, matchingDocs.totalHits);
        DocIdSetIterator idIterator = matchingDocs.docIdSet;
        assertEquals(1, idIterator.nextDoc());
        assertEquals(3, idIterator.nextDoc());
        assertEquals(5, idIterator.nextDoc());
        assertEquals(9, idIterator.nextDoc());
        assertEquals(DocIdSetIterator.NO_MORE_DOCS, idIterator.nextDoc());
    }

    @Test
    void shouldCollectOneMatchingDocsPerSegment() throws Exception {
        // given
        Lucene9DocValuesCollector collector = new Lucene9DocValuesCollector();
        Lucene9IndexReaderStub readerStub = indexReaderWithMaxDocs(42);

        // when
        collector.doSetNextReader(readerStub.getContext());
        collector.collect(1);
        collector.collect(3);
        collector.doSetNextReader(readerStub.getContext());
        collector.collect(5);
        collector.collect(9);

        // then
        assertEquals(4, collector.getTotalHits());
        List<Lucene9DocValuesCollector.MatchingDocs> allMatchingDocs = collector.getMatchingDocs();
        assertEquals(2, allMatchingDocs.size());

        Lucene9DocValuesCollector.MatchingDocs matchingDocs = allMatchingDocs.getFirst();
        assertSame(readerStub.getContext(), matchingDocs.context);
        assertEquals(2, matchingDocs.totalHits);
        DocIdSetIterator idIterator = matchingDocs.docIdSet;
        assertEquals(1, idIterator.nextDoc());
        assertEquals(3, idIterator.nextDoc());
        assertEquals(DocIdSetIterator.NO_MORE_DOCS, idIterator.nextDoc());

        matchingDocs = allMatchingDocs.get(1);
        assertSame(readerStub.getContext(), matchingDocs.context);
        assertEquals(2, matchingDocs.totalHits);
        idIterator = matchingDocs.docIdSet;
        assertEquals(5, idIterator.nextDoc());
        assertEquals(9, idIterator.nextDoc());
        assertEquals(DocIdSetIterator.NO_MORE_DOCS, idIterator.nextDoc());
    }

    @Test
    void shouldNotSaveScoresForIndexProgressorWhenNotRequired() throws Exception {
        // given
        Lucene9DocValuesCollector collector = new Lucene9DocValuesCollector();
        Lucene9IndexReaderStub readerStub = indexReaderWithMaxDocs(42);

        // when
        collector.doSetNextReader(readerStub.getContext());
        collector.collect(1);

        // then
        AtomicReference<AcceptedEntity> ref = new AtomicReference<>();
        IndexProgressor.EntityValueClient client = new EntityValueClientWritingToReference(ref);
        IndexProgressor progressor = collector.getIndexProgressor("field", client);
        assertTrue(progressor.next());
        assertFalse(progressor.next());
        progressor.close();
        AcceptedEntity entity = ref.get();
        assertThat(entity.reference).isEqualTo(1L);
        assertTrue(Float.isNaN(entity.score));
    }

    private static Lucene9IndexReaderStub indexReaderWithMaxDocs(int maxDocs) {
        NumericDocValues identityValues = new NumericDocValues() {
            @Override
            public boolean advanceExact(int target) {
                advance(target);
                return true;
            }

            private int next = -1;

            @Override
            public int docID() {
                return next;
            }

            @Override
            public int nextDoc() {
                return advance(next + 1);
            }

            @Override
            public int advance(int target) {
                return next = target;
            }

            @Override
            public long cost() {
                return 0;
            }

            @Override
            public long longValue() {
                return next;
            }
        };
        Lucene9IndexReaderStub stub = new Lucene9IndexReaderStub(identityValues);
        stub.setElements(new String[maxDocs]);
        return stub;
    }

    private static Scorer constantScorer(float score) {
        Weight weight = new ConstantScoreWeight(null, score) {
            @Override
            public Scorer scorer(LeafReaderContext context) {
                return null;
            }

            @Override
            public boolean isCacheable(LeafReaderContext ctx) {
                return false;
            }
        };
        return new ConstantScoreScorer(weight, score, ScoreMode.COMPLETE, (DocIdSetIterator) null);
    }

    private static final class AcceptedEntity {
        long reference;
        float score;
        Value[] values;
    }

    private static class EntityValueClientWritingToReference implements IndexProgressor.EntityValueClient {
        private final AtomicReference<AcceptedEntity> ref;

        private EntityValueClientWritingToReference(AtomicReference<AcceptedEntity> ref) {
            this.ref = ref;
        }

        @Override
        public void initializeQuery(
                IndexDescriptor descriptor,
                IndexProgressor progressor,
                boolean indexIncludesTransactionState,
                boolean needStoreFilter,
                IndexQueryConstraints constraints,
                PropertyIndexQuery... query) {}

        @Override
        public boolean acceptEntity(long reference, float score, Value... values) {
            assertNull(ref.get());
            AcceptedEntity entity = new AcceptedEntity();
            entity.reference = reference;
            entity.score = score;
            entity.values = values;
            ref.set(entity);
            return true;
        }

        @Override
        public boolean needsValues() {
            return false;
        }
    }
}
