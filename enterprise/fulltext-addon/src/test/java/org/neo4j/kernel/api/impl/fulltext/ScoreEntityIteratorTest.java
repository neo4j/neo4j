/*
 * Copyright (c) 2002-2018 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
 *
 * This file is part of Neo4j.
 *
 * Neo4j is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program. If not, see <http://www.gnu.org/licenses/>.
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
    public void concatShouldReturnOrderedResults()
    {
        ScoreEntityIterator one = iteratorOf( new ScoreEntry[]{e( 3, 10 ), e( 10, 3 ), e( 12, 1 )} );
        ScoreEntityIterator two = iteratorOf( new ScoreEntry[]{e( 1, 12 ), e( 5, 8 ), e( 7, 6 ), e( 8, 5 ), e( 11, 2 )} );
        ScoreEntityIterator three = iteratorOf( new ScoreEntry[]{e( 2, 11 ), e( 4, 9 ), e( 6, 7 ), e( 9, 4 )} );

        ScoreEntityIterator concat = ScoreEntityIterator.concat( Arrays.asList( one, two, three ) );

        for ( int i = 1; i <= 12; i++ )
        {
            assertTrue( concat.hasNext() );
            ScoreEntry entry = concat.next();
            assertEquals( i, entry.entityId() );
            assertEquals( 13 - i, entry.score(), 0.001 );
        }
        assertFalse( concat.hasNext() );
    }

    private ScoreEntry e( long id, float s )
    {
        return new ScoreEntry( id, s );
    }

    @Test
    public void concatShouldHandleEmptyIterators()
    {
        ScoreEntityIterator one = iteratorOf( new ScoreEntry[]{} );
        ScoreEntityIterator two = iteratorOf( new ScoreEntry[]{e( 1, 5 ), e( 2, 4 ), e( 3, 3 ), e( 4, 2 ), e( 5, 1 )} );
        ScoreEntityIterator three = iteratorOf( new ScoreEntry[]{} );

        ScoreEntityIterator concat = ScoreEntityIterator.concat( Arrays.asList( one, two, three ) );

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
    public void concatShouldHandleAllEmptyIterators()
    {
        ScoreEntityIterator one = iteratorOf( new ScoreEntry[]{} );
        ScoreEntityIterator two = iteratorOf( new ScoreEntry[]{} );
        ScoreEntityIterator three = iteratorOf( new ScoreEntry[]{} );

        ScoreEntityIterator concat = ScoreEntityIterator.concat( Arrays.asList( one, two, three ) );

        assertFalse( concat.hasNext() );
    }

    private ScoreEntityIterator iteratorOf( ScoreEntry[] input )
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
