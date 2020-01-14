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

import org.eclipse.collections.api.block.procedure.primitive.LongFloatProcedure;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.RepeatedTest;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.PriorityQueue;
import java.util.SplittableRandom;
import java.util.concurrent.ThreadLocalRandom;

import org.neo4j.kernel.api.impl.fulltext.FulltextResultCollector.EntityResultsIterator;
import org.neo4j.kernel.api.impl.fulltext.FulltextResultCollector.EntityScorePriorityQueue;

import static org.assertj.core.api.Assertions.assertThat;

class FulltextResultCollectorTest
{
    static class EntityScore implements Comparable<EntityScore>, LongFloatProcedure
    {
        long entity;
        float score;

        EntityScore( long entity, float score )
        {
            this.entity = entity;
            this.score = score;
        }

        @Override
        public int compareTo( EntityScore o )
        {
            return Float.compare( o.score, score );
        }

        @Override
        public String toString()
        {
            return "EntityScore{" + "entity=" + entity + ", score=" + score + '}';
        }

        @Override
        public boolean equals( Object o )
        {
            if ( this == o )
            {
                return true;
            }
            if ( o == null || getClass() != o.getClass() )
            {
                return false;
            }

            EntityScore that = (EntityScore) o;

            if ( entity != that.entity )
            {
                return false;
            }
            return Float.compare( that.score, score ) == 0;
        }

        @Override
        public int hashCode()
        {
            int result = (int) (entity ^ (entity >>> 32));
            result = 31 * result + (score != +0.0f ? Float.floatToIntBits( score ) : 0);
            return result;
        }

        @Override
        public void value( long entity, float score )
        {
            this.entity = entity;
            this.score = score;
        }
    }

    @Nested
    class PriorityQueueTest
    {
        @Test
        void queueMustCollectAndOrderResultsByScore()
        {
            EntityScorePriorityQueue pq = new EntityScorePriorityQueue();
            assertThat( pq.isEmpty() ).isTrue();
            pq.insert( 1, 3.0f );
            assertThat( pq.isEmpty() ).isFalse();
            pq.insert( 2, 1.0f );
            pq.insert( 3, 4.0f );
            pq.insert( 4, 2.0f );
            pq.insert( 5, 7.0f );
            pq.insert( 6, 5.0f );
            pq.insert( 7, 6.0f );

            List<Integer> ids = new ArrayList<>( 7 );
            LongFloatProcedure receiver = ( id, score ) -> ids.add( (int) id );
            assertThat( pq.size() ).isEqualTo( 7 );
            pq.removeTop( receiver );
            assertThat( pq.size() ).isEqualTo( 6 );
            pq.removeTop( receiver );
            assertThat( pq.size() ).isEqualTo( 5 );
            pq.removeTop( receiver );
            assertThat( pq.size() ).isEqualTo( 4 );
            pq.removeTop( receiver );
            assertThat( pq.size() ).isEqualTo( 3 );
            pq.removeTop( receiver );
            assertThat( pq.size() ).isEqualTo( 2 );
            pq.removeTop( receiver );
            assertThat( pq.size() ).isEqualTo( 1 );
            assertThat( pq.isEmpty() ).isFalse();
            pq.removeTop( receiver );
            assertThat( pq.size() ).isEqualTo( 0 );
            assertThat( pq.isEmpty() ).isTrue();
            assertThat( ids ).isEqualTo( List.of( 5, 7, 6, 3, 1, 4, 2 ) );
        }

        @RepeatedTest( 200 )
        void randomizedPriorityQueueTest()
        {
            long seed = ThreadLocalRandom.current().nextLong();
            SplittableRandom rng = new SplittableRandom( seed );
            int count = rng.nextInt( 5, 100 );

            try
            {
                EntityScorePriorityQueue actualQueue = new EntityScorePriorityQueue();
                PriorityQueue<EntityScore> expectedQueue = new PriorityQueue<>();
                for ( int i = 0; i < count; i++ )
                {
                    float score = (float) rng.nextDouble();
                    expectedQueue.add( new EntityScore( i, score ) );
                    actualQueue.insert( i, score );
                }

                assertThat( actualQueue.size() ).isEqualTo( expectedQueue.size() );

                EntityScore entityScore = new EntityScore( 0, 0.0f );
                while ( !actualQueue.isEmpty() )
                {
                    actualQueue.removeTop( entityScore );
                    assertThat( entityScore ).isEqualTo( expectedQueue.remove() );
                }
                assertThat( expectedQueue ).isEmpty();
            }
            catch ( Throwable e )
            {
                throw new RuntimeException( "Failed using seed = " + seed, e );
            }
        }
    }

    @Nested
    class EntityResultsIteratorTest
    {
        @RepeatedTest( 200 )
        void randomizedPriorityQueueTest()
        {
            long seed = ThreadLocalRandom.current().nextLong();
            SplittableRandom rng = new SplittableRandom( seed );
            int count = rng.nextInt( 50, 100 );

            try
            {
                PriorityQueue<EntityScorePriorityQueue> actualQueue = new PriorityQueue<>();
                EntityScorePriorityQueue currentQueue = new EntityScorePriorityQueue();
                PriorityQueue<EntityScore> expectedQueue = new PriorityQueue<>( count );
                for ( int i = 0, j = 1; i < count; i++, j++ )
                {
                    if ( j == 10 )
                    {
                        actualQueue.add( currentQueue );
                        currentQueue = new EntityScorePriorityQueue();
                        j = 0;
                    }
                    float score = (float) rng.nextDouble();
                    expectedQueue.add( new EntityScore( i, score ) );
                    currentQueue.insert( i, score );
                }
                actualQueue.add( currentQueue );
                EntityResultsIterator iterator = new EntityResultsIterator( actualQueue );

                EntityScore entityScore = new EntityScore( 0, 0.0f );
                int i = 0;
                while ( iterator.hasNext() )
                {
                    iterator.next();
                    entityScore.value( iterator.current(), iterator.currentScore() );
                    assertThat( entityScore ).as( "iteration %s", i++ ).isEqualTo( expectedQueue.remove() );
                }
                assertThat( expectedQueue ).isEmpty();
            }
            catch ( Throwable e )
            {
                throw new RuntimeException( "Failed using seed = " + seed, e );
            }
        }
    }
}
