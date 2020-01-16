/*
 * Copyright (c) 2002-2020 "Neo4j,"
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
package org.neo4j.kernel.api.impl.fulltext;

import org.apache.lucene.index.LeafReader;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.NumericDocValues;
import org.apache.lucene.search.Collector;
import org.apache.lucene.search.LeafCollector;
import org.apache.lucene.search.Scorable;
import org.apache.lucene.search.ScoreMode;
import org.eclipse.collections.api.block.procedure.primitive.LongFloatProcedure;

import java.io.IOException;
import java.util.Arrays;
import java.util.NoSuchElementException;

import org.neo4j.kernel.api.impl.index.collector.ValuesIterator;

class FulltextResultCollector implements Collector
{
    private final EntityScorePriorityQueue pq;

    FulltextResultCollector()
    {
        pq = new EntityScorePriorityQueue();
    }

    public ValuesIterator iterator()
    {
        if ( pq.isEmpty() )
        {
            return ValuesIterator.EMPTY;
        }
        return new EntityResultsIterator( pq );
    }

    @Override
    public LeafCollector getLeafCollector( LeafReaderContext context ) throws IOException
    {
        return new ScoredEntityLeafCollector( context, pq );
    }

    @Override
    public ScoreMode scoreMode()
    {
        return ScoreMode.COMPLETE;
    }

    private static class ScoredEntityLeafCollector implements LeafCollector
    {
        private final EntityScorePriorityQueue pq;
        private final NumericDocValues values;
        private Scorable scorer;

        ScoredEntityLeafCollector( LeafReaderContext context, EntityScorePriorityQueue pq ) throws IOException
        {
            LeafReader reader = context.reader();
            values = reader.getNumericDocValues( LuceneFulltextDocumentStructure.FIELD_ENTITY_ID );
            this.pq = pq;
        }

        @Override
        public void setScorer( Scorable scorer )
        {
            this.scorer = scorer;
        }

        @Override
        public void collect( int doc ) throws IOException
        {
            assert scorer.docID() == doc;
            if ( values.advanceExact( doc ) )
            {
                float score = scorer.score();
                long entityId = values.longValue();
                pq.insert( entityId, score );
            }
            else
            {
                throw new RuntimeException( "No document value for document id " + doc + "." );
            }
        }
    }

    /**
     * Organise entity ids by decreasing scores, using a binary heap.
     * The implementation of the priority queue algorithm follows the one in Algorithms, 4th Edition by Robert Sedgewick and Kevin Wayne.
     */
    static class EntityScorePriorityQueue
    {
        private static final int ROOT = 1; // Root of the heap is always at index 1.
        private static final int INITIAL_CAPACITY = 33; // Some number not too big, and not too small.
        private final boolean maxQueue; // 'true' if this is a max-priority queue, 'false' for a min-priority queue.
        private long[] entities;
        private float[] scores;
        private int size;

        EntityScorePriorityQueue()
        {
            this( true );
        }

        EntityScorePriorityQueue( boolean maxQueue )
        {
            this.maxQueue = maxQueue;
            entities = new long[INITIAL_CAPACITY];
            scores = new float[INITIAL_CAPACITY];
        }

        public int size()
        {
            return size;
        }

        public boolean isEmpty()
        {
            return size == 0;
        }

        public void insert( long entityId, float score )
        {
            size += 1;
            if ( size == entities.length )
            {
                growCapacity();
            }
            entities[size] = entityId;
            scores[size] = score;
            liftTowardsRoot( size );
        }

        public void removeTop( LongFloatProcedure receiver )
        {
            receiver.value( entities[ROOT], scores[ROOT] );
            swap( ROOT, size );
            size -= 1;
            pushTowardsBottom();
        }

        private void growCapacity()
        {
            entities = Arrays.copyOf( entities, entities.length * 2 );
            scores = Arrays.copyOf( scores, scores.length * 2 );
        }

        private void liftTowardsRoot( int index )
        {
            int parentIndex;
            while ( index > ROOT && subordinate( parentIndex = index >> 1, index ) )
            {
                swap( index, parentIndex );
                index = parentIndex;
            }
        }

        private void pushTowardsBottom()
        {
            int index = ROOT;
            int child;
            while ( (child = index << 1) <= size )
            {
                if ( child < size && subordinate( child, child + 1 ) )
                {
                    child += 1;
                }
                if ( !subordinate( index, child ) )
                {
                    break;
                }
                swap( index, child );
                index = child;
            }
        }

        private boolean subordinate( int indexA, int indexB )
        {
            float scoreA = scores[indexA];
            float scoreB = scores[indexB];
            return maxQueue ? scoreA < scoreB : scoreA > scoreB;
        }

        private void swap( int indexA, int indexB )
        {
            long entity = entities[indexA];
            float score = scores[indexA];
            entities[indexA] = entities[indexB];
            scores[indexA] = scores[indexB];
            entities[indexB] = entity;
            scores[indexB] = score;
        }
    }

    static class EntityResultsIterator implements ValuesIterator, LongFloatProcedure
    {
        private final EntityScorePriorityQueue pq;
        private long currentEntity;
        private float currentScore;

        EntityResultsIterator( EntityScorePriorityQueue pq )
        {
            this.pq = pq;
        }

        @Override
        public int remaining()
        {
            return 0; // Not used.
        }

        @Override
        public float currentScore()
        {
            return currentScore;
        }

        @Override
        public long next()
        {
            if ( hasNext() )
            {
                pq.removeTop( this );
                return currentEntity;
            }
            else
            {
                throw new NoSuchElementException();
            }
        }

        @Override
        public boolean hasNext()
        {
            return !pq.isEmpty();
        }

        @Override
        public long current()
        {
            return currentEntity;
        }

        @Override
        public void value( long entity, float score )
        {
            currentEntity = entity;
            currentScore = score;
        }
    }
}
