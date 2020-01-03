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

import org.junit.Test;

import java.util.Arrays;
import java.util.Iterator;

import org.neo4j.kernel.api.impl.fulltext.ScoreEntityIterator.ScoreEntry;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class ScoreEntityIteratorTest
{
    @Test
    public void mergeShouldReturnOrderedResults()
    {
        ScoreEntityIterator one = iteratorOf( new ScoreEntry[]{entry( 3, 10 ), entry( 10, 3 ), entry( 12, 1 )} );
        ScoreEntityIterator two = iteratorOf( new ScoreEntry[]{entry( 1, 12 ), entry( 5, 8 ), entry( 7, 6 ), entry( 8, 5 ), entry( 11, 2 )} );
        ScoreEntityIterator three = iteratorOf( new ScoreEntry[]{entry( 2, 11 ), entry( 4, 9 ), entry( 6, 7 ), entry( 9, 4 )} );

        ScoreEntityIterator concat = ScoreEntityIterator.mergeIterators( Arrays.asList( one, two, three ) );

        for ( int i = 1; i <= 12; i++ )
        {
            assertTrue( concat.hasNext() );
            ScoreEntry entry = concat.next();
            assertEquals( i, entry.entityId() );
            assertEquals( 13 - i, entry.score(), 0.001 );
        }
        assertFalse( concat.hasNext() );
    }

    private static ScoreEntry entry( long id, float s )
    {
        return new ScoreEntry( id, s );
    }

    @Test
    public void mergeShouldHandleEmptyIterators()
    {
        ScoreEntityIterator one = iteratorOf( emptyEntries() );
        ScoreEntityIterator two = iteratorOf( new ScoreEntry[]{entry( 1, 5 ), entry( 2, 4 ), entry( 3, 3 ), entry( 4, 2 ), entry( 5, 1 )} );
        ScoreEntityIterator three = iteratorOf( emptyEntries() );

        ScoreEntityIterator concat = ScoreEntityIterator.mergeIterators( Arrays.asList( one, two, three ) );

        for ( int i = 1; i <= 5; i++ )
        {
            assertTrue( concat.hasNext() );
            ScoreEntry entry = concat.next();
            assertEquals( i, entry.entityId() );
            assertEquals( 6 - i, entry.score(), 0.001 );
        }
        assertFalse( concat.hasNext() );
    }

    @Test
    public void mergeShouldHandleAllEmptyIterators()
    {
        ScoreEntityIterator one = iteratorOf( emptyEntries() );
        ScoreEntityIterator two = iteratorOf( emptyEntries() );
        ScoreEntityIterator three = iteratorOf( emptyEntries() );

        ScoreEntityIterator concat = ScoreEntityIterator.mergeIterators( Arrays.asList( one, two, three ) );

        assertFalse( concat.hasNext() );
    }

    private static ScoreEntry[] emptyEntries()
    {
        return new ScoreEntry[]{};
    }

    private static ScoreEntityIterator iteratorOf( ScoreEntry[] input )
    {
        return new ScoreEntityIterator( null )
        {
            Iterator<ScoreEntry> entries = Arrays.asList( input ).iterator();

            @Override
            public boolean hasNext()
            {
                return entries.hasNext();
            }

            @Override
            public ScoreEntry next()
            {
                return entries.next();
            }
        };
    }
}
