/*
 * Copyright (c) 2002-2018 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
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
package org.neo4j.storageengine.api.schema;


import org.junit.Test;

import java.util.List;

import static java.util.Arrays.asList;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.neo4j.storageengine.api.schema.IndexProgressors.concat;

public class IndexProgressorsTest
{
    @Test
    public void shouldConcatTwoIndexProgressors()
    {
        //Given
        IndexProgressor a = progressor( 1, 2, 3 );
        IndexProgressor b = progressor( 3, 4, 5, 6 );

        //When
        IndexProgressor concat = concat( a, b );

        //Then
        assertThat( count( concat ), equalTo( 7 ) );
    }

    @Test
    public void shouldConcatMultipleIndexProgressors()
    {
        //Given
        List<IndexProgressor> progressors =
                asList( progressor( 1, 2, 3 ), progressor( 3, 4, 5, 6 ), progressor( 3 ) );

        //When
        IndexProgressor concat = concat( progressors );

        //Then
        assertThat( count( concat ), equalTo( 8 ) );
    }

    private int count( IndexProgressor progressor )
    {
        int count = 0;
        while ( progressor.next() )
        {
            count++;
        }
        return count;
    }

    private IndexProgressor progressor( int... values )
    {
        return new StubProgressor( values );
    }

    private static class StubProgressor implements IndexProgressor
    {
        private final int[] values;
        private int current = -1;

        StubProgressor( int... values )
        {
            this.values = values;
        }

        @Override
        public boolean next()
        {
            return ++current < values.length;
        }

        public int value()
        {
            return values[current];
        }

        @Override
        public void close()
        {
            //nothing to close
        }
    }
}
