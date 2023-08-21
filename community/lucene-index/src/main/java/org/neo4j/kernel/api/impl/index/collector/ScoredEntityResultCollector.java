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
package org.neo4j.kernel.api.impl.index.collector;

import java.io.IOException;
import java.util.Arrays;
import java.util.NoSuchElementException;
import java.util.function.LongPredicate;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.NumericDocValues;
import org.apache.lucene.search.Collector;
import org.apache.lucene.search.LeafCollector;
import org.apache.lucene.search.Scorable;
import org.apache.lucene.search.ScoreMode;
import org.apache.lucene.search.TopScoreDocCollector;
import org.eclipse.collections.api.block.procedure.primitive.LongFloatProcedure;
import org.neo4j.internal.kernel.api.IndexQueryConstraints;

/**
 * Collects hits from lucene search and stores them in priority queue comparing by scores.
 *
 * If limit is specified by constraints it will employ approach similar to {@link TopScoreDocCollector} and will give hints to scorer to skip
 * non-competitive document improving search performance.
 *
 * This collector doesn't track total number of hits.
 */
public abstract class ScoredEntityResultCollector implements Collector {
    private static final int NO_LIMIT = -1;

    private final long limit;
    private final ScoredEntityPriorityQueue pq;
    private final LongPredicate exclusionFilter;

    protected ScoredEntityResultCollector(IndexQueryConstraints constraints, LongPredicate exclusionFilter) {
        this.exclusionFilter = exclusionFilter;
        this.limit = getLimit(constraints);
        // Use a max-queue for no-limit otherwise a min-queue to continuously drop the entry with the lowest score.
        this.pq = new ScoredEntityPriorityQueue(this.limit == NO_LIMIT);
    }

    public ValuesIterator iterator() {
        return pq.iterator();
    }

    protected abstract String entityIdFieldKey();

    @Override
    public LeafCollector getLeafCollector(LeafReaderContext context) throws IOException {
        return new ScoredEntityLeafCollector(context, entityIdFieldKey(), pq, limit, exclusionFilter);
    }

    @Override
    public ScoreMode scoreMode() {
        return limit == NO_LIMIT ? ScoreMode.COMPLETE : ScoreMode.TOP_SCORES;
    }

    private static long getLimit(IndexQueryConstraints constraints) {
        final var limit = constraints.limit().orElse(Integer.MAX_VALUE)
                + constraints.skip().orElse(0);
        // If the limit is enormous, and we will never reach it from just querying a single index partition.
        // An index partition can "only" hold 2 billion documents.
        // Just let the IndexProgressor apply the skip and limit.
        return limit < Integer.MAX_VALUE ? limit : NO_LIMIT;
    }

    private static class ScoredEntityLeafCollector implements LeafCollector {
        private final ScoredEntityPriorityQueue pq;
        private final long limit;
        private final LongPredicate exclusionFilter;
        private final NumericDocValues values;
        private Scorable scorer;

        private float minCompetitiveScore;

        ScoredEntityLeafCollector(
                LeafReaderContext context,
                String entityIdFieldKey,
                ScoredEntityPriorityQueue pq,
                long limit,
                LongPredicate exclusionFilter)
                throws IOException {
            this.pq = pq;
            this.limit = limit;
            this.exclusionFilter = exclusionFilter;
            final var reader = context.reader();
            values = reader.getNumericDocValues(entityIdFieldKey);
        }

        @Override
        public void setScorer(Scorable scorer) throws IOException {
            this.scorer = scorer;
            minCompetitiveScore = 0f;
            updateMinCompetitiveScore(scorer);
        }

        @Override
        public void collect(int doc) throws IOException {
            assert scorer.docID() == doc;
            if (values.advanceExact(doc)) {
                long entityId = values.longValue();
                float score = scorer.score();
                if (exclusionFilter.test(entityId)) {
                    return;
                }
                if (limit == NO_LIMIT) {
                    pq.insert(entityId, score);
                } else {
                    if (pq.size() < limit) {
                        pq.insert(entityId, score);
                        updateMinCompetitiveScore(scorer);
                    } else if (pq.peekTopScore()
                            < score) // when limit is set pq is min-queue, if new score is better use it
                    {
                        pq.removeTop();
                        pq.insert(entityId, score);
                        updateMinCompetitiveScore(scorer);
                    }
                    // Otherwise, don't bother inserting this entry.
                }
            } else {
                throw new RuntimeException("No document value for document id " + doc + ".");
            }
        }

        /**
         * Update minimum competitive score for scorer, so it can skip documents with lower score, to improve search performance with limit.
         * This score is updated only if limit is specified. In this case pq is min-queue and top element contains lowest collected score,
         * we are not interested in documents with score lower then that.
         */
        private void updateMinCompetitiveScore(Scorable scorer) throws IOException {
            // limit is set and enough elements have already collected, we can start skipping low scored documents
            if (limit != NO_LIMIT && pq.size() >= limit) {
                // since we tie-break on doc id and collect in doc id order, we can require
                // the next float
                var localMinScore = Math.nextUp(pq.peekTopScore());
                if (localMinScore > minCompetitiveScore) {
                    scorer.setMinCompetitiveScore(localMinScore);
                    minCompetitiveScore = localMinScore;
                }
            }
        }
    }

    /**
     * Organise entity ids by decreasing scores, using a binary heap.
     * The implementation of the priority queue algorithm follows the one in Algorithms, 4th Edition by Robert Sedgewick and Kevin Wayne.
     */
    public static class ScoredEntityPriorityQueue {
        private static final int ROOT = 1; // Root of the heap is always at index 1.
        private static final int INITIAL_CAPACITY = 33; // Some number not too big, and not too small.
        private final boolean maxQueue; // 'true' if this is a max-priority queue, 'false' for a min-priority queue.
        private long[] entities;
        private float[] scores;
        private int size;

        public ScoredEntityPriorityQueue(boolean maxQueue) {
            this.maxQueue = maxQueue;
            entities = new long[INITIAL_CAPACITY];
            scores = new float[INITIAL_CAPACITY];
        }

        public int size() {
            return size;
        }

        public boolean isEmpty() {
            return size == 0;
        }

        public void insert(long entityId, float score) {
            size += 1;
            if (size == entities.length) {
                growCapacity();
            }
            entities[size] = entityId;
            scores[size] = score;
            liftTowardsRoot(size);
        }

        public float peekTopScore() {
            return scores[ROOT];
        }

        public void removeTop(LongFloatProcedure receiver) {
            receiver.value(entities[ROOT], scores[ROOT]);
            removeTop();
        }

        public void removeTop() {
            swap(ROOT, size);
            size -= 1;
            pushTowardsBottom();
        }

        private void growCapacity() {
            entities = Arrays.copyOf(entities, entities.length * 2);
            scores = Arrays.copyOf(scores, scores.length * 2);
        }

        private void liftTowardsRoot(int index) {
            int parentIndex;
            while (index > ROOT && subordinate(parentIndex = index >> 1, index)) {
                swap(index, parentIndex);
                index = parentIndex;
            }
        }

        private void pushTowardsBottom() {
            int index = ROOT;
            int child;
            while ((child = index << 1) <= size) {
                if (child < size && subordinate(child, child + 1)) {
                    child += 1;
                }
                if (!subordinate(index, child)) {
                    break;
                }
                swap(index, child);
                index = child;
            }
        }

        private boolean subordinate(int indexA, int indexB) {
            float scoreA = scores[indexA];
            float scoreB = scores[indexB];
            return maxQueue ? scoreA < scoreB : scoreA > scoreB;
        }

        private void swap(int indexA, int indexB) {
            long entity = entities[indexA];
            float score = scores[indexA];
            entities[indexA] = entities[indexB];
            scores[indexA] = scores[indexB];
            entities[indexB] = entity;
            scores[indexB] = score;
        }

        ValuesIterator iterator() {
            if (isEmpty()) {
                return ValuesIterator.EMPTY;
            }

            return maxQueue
                    ? // The queye will pop entries in their correctly sorted order.
                    new ScoredEntityResultsMaxQueueIterator(this)
                    : // Otherwise, we need to reverse the result collected in the queue.
                    new ScoredEntityResultsMinQueueIterator(this);
        }
    }

    /**
     * Produce entity/score results from the given priority queue, assuming it's a max-queue that itself delivers entries in descending order.
     */
    public static class ScoredEntityResultsMaxQueueIterator implements ValuesIterator, LongFloatProcedure {
        private final ScoredEntityPriorityQueue pq;
        private long currentEntity;
        private float currentScore;

        public ScoredEntityResultsMaxQueueIterator(ScoredEntityPriorityQueue pq) {
            this.pq = pq;
        }

        @Override
        public int remaining() {
            return 0; // Not used.
        }

        @Override
        public float currentScore() {
            return currentScore;
        }

        @Override
        public long next() {
            if (hasNext()) {
                pq.removeTop(this);
                return currentEntity;
            } else {
                throw new NoSuchElementException();
            }
        }

        @Override
        public boolean hasNext() {
            return !pq.isEmpty();
        }

        @Override
        public long current() {
            return currentEntity;
        }

        @Override
        public void value(long entityId, float score) {
            currentEntity = entityId;
            currentScore = score;
        }
    }

    /**
     * Produce entity/score results from the given priority queue, assuming it's a min-queue which delivers entries in ascending order, so that they have
     * to be reversed before we can iterate them.
     */
    public static class ScoredEntityResultsMinQueueIterator implements ValuesIterator, LongFloatProcedure {
        private final long[] entityIds;
        private final float[] scores;
        private int index;

        public ScoredEntityResultsMinQueueIterator(ScoredEntityPriorityQueue pq) {
            int size = pq.size();
            this.entityIds = new long[size];
            this.scores = new float[size];
            this.index = size - 1;
            while (!pq.isEmpty()) {
                pq.removeTop(this); // Populate the arrays in the correct order, basically using Heap Sort.
            }
        }

        @Override
        public int remaining() {
            return 0; // Not used.
        }

        @Override
        public long next() {
            if (hasNext()) {
                index++;
                return current();
            }
            throw new NoSuchElementException();
        }

        @Override
        public boolean hasNext() {
            return index < entityIds.length - 1;
        }

        @Override
        public long current() {
            return entityIds[index];
        }

        @Override
        public float currentScore() {
            return scores[index];
        }

        @Override
        public void value(long entityId, float score) {
            this.entityIds[index] = entityId;
            this.scores[index] = score;
            index--;
        }
    }
}
