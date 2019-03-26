/*
 * Copyright (c) 2002-2019 "Neo4j,"
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

import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;

import org.neo4j.kernel.api.impl.index.collector.ValuesIterator;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class ScoreEntityIteratorTest
{
    @Test
    public void mergeShouldReturnOrderedResults()
    {
        StubValuesIterator one = new StubValuesIterator().add( 3, 10 ).add( 10, 3 ).add( 12, 1 );
        StubValuesIterator two = new StubValuesIterator().add( 1, 12 ).add( 5, 8 ).add( 7, 6 ).add( 8, 5 ).add( 11, 2 );
        StubValuesIterator three = new StubValuesIterator().add( 2, 11 ).add( 4, 9 ).add( 6, 7 ).add( 9, 4 );

        ValuesIterator concat = ScoreEntityIterator.mergeIterators( Arrays.asList( one, two, three ) );

        for ( int i = 1; i <= 12; i++ )
        {
            assertTrue( concat.hasNext() );
            assertEquals( i, concat.next() );
            assertEquals( i, concat.current() );
            assertEquals( 13 - i, concat.currentScore(), 0.001 );
        }
        assertFalse( concat.hasNext() );
    }

    @Test
    public void mergeShouldCorrectlyOrderSpecialValues()
    {
        // According to CIP2016-06-14, NaN comes between positive infinity and the largest float/double value.
        StubValuesIterator one = new StubValuesIterator().add( 2, Float.POSITIVE_INFINITY ).add( 4, 1.0f ).add( 6, Float.MIN_VALUE ).add( 8, -1.0f );
        StubValuesIterator two = new StubValuesIterator().add( 1, Float.NaN ).add( 3, Float.MAX_VALUE ).add( 5, Float.MIN_NORMAL ).add( 7, 0.0f )
                .add( 9, Float.NEGATIVE_INFINITY );

        ValuesIterator concat = ScoreEntityIterator.mergeIterators( Arrays.asList( one, two ) );

        assertTrue( concat.hasNext() );
        assertEquals( 1, concat.next() );
        assertTrue( Double.isNaN( concat.currentScore() ) );
        assertEquals( 2, concat.next() );
        assertTrue( Double.isInfinite( concat.currentScore() ) );
        assertTrue( concat.currentScore() > 0.0f );
        assertEquals( 3, concat.next() );
        assertEquals( 4, concat.next() );
        assertEquals( 5, concat.next() );
        assertEquals( 6, concat.next() );
        assertEquals( 7, concat.next() );
        assertEquals( 8, concat.next() );
        assertEquals( 9, concat.next() );
        assertFalse( concat.hasNext() );
    }

    @Test
    public void mergeShouldHandleEmptyIterators()
    {
        StubValuesIterator one = new StubValuesIterator();
        StubValuesIterator two = new StubValuesIterator().add( 1, 5 ).add( 2, 4 ).add( 3, 3 ).add( 4, 2 ).add( 5, 1 );
        StubValuesIterator three = new StubValuesIterator();

        ValuesIterator concat = ScoreEntityIterator.mergeIterators( Arrays.asList( one, two, three ) );

        for ( int i = 1; i <= 5; i++ )
        {
            assertTrue( concat.hasNext() );
            assertEquals( i, concat.next() );
            assertEquals( i, concat.current() );
            assertEquals( 6 - i, concat.currentScore(), 0.001 );
        }
        assertFalse( concat.hasNext() );
    }

    @Test
    public void mergeShouldHandleAllEmptyIterators()
    {
        StubValuesIterator one = new StubValuesIterator();
        StubValuesIterator two = new StubValuesIterator();
        StubValuesIterator three = new StubValuesIterator();

        ValuesIterator concat = ScoreEntityIterator.mergeIterators( Arrays.asList( one, two, three ) );

        assertFalse( concat.hasNext() );
    }

    private class StubValuesIterator implements ValuesIterator
    {
        private ArrayList<Long> entityIds = new ArrayList<>();
        private ArrayList<Float> scores = new ArrayList<>();
        private int nextIndex;

        public StubValuesIterator add( long entityId, float score )
        {
            entityIds.add( entityId );
            scores.add( score );
            return this;
        }

        @Override
        public int remaining()
        {
            return entityIds.size() - nextIndex;
        }

        @Override
        public float currentScore()
        {
            return scores.get( nextIndex - 1 );
        }

        @Override
        public long next()
        {
            long entityId = entityIds.get( nextIndex );
            nextIndex++;
            return entityId;
        }

        @Override
        public boolean hasNext()
        {
            return remaining() > 0;
        }

        @Override
        public long current()
        {
            return entityIds.get( nextIndex - 1 );
        }
    }
}
