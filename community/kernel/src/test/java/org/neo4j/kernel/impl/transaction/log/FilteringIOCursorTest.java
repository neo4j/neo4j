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
package org.neo4j.kernel.impl.transaction.log;

import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.neo4j.helpers.Predicates;

import static java.util.Arrays.asList;
import static org.junit.Assert.assertEquals;
import static org.neo4j.helpers.Predicates.in;
import static org.neo4j.helpers.Predicates.not;

public class FilteringIOCursorTest
{

    @Test
    public void shouldNotFilterWhenNothingToFilter() throws IOException
    {
        String[] strings = { "a", "b", "c" };

        IOCursor<String> delegate = new ArrayIOCursor<>( strings );
        FilteringIOCursor<String> cursor = new FilteringIOCursor<>( delegate, Predicates.<String>TRUE() );

        assertEquals( asList( strings ), extractCursorContent( cursor ) );
    }

    @Test
    public void shouldFilterFirstObject() throws IOException
    {
        String[] strings = { "a", "b", "c" };

        IOCursor<String> delegate = new ArrayIOCursor<>( strings );
        FilteringIOCursor<String> cursor = new FilteringIOCursor<>( delegate, not( in( "a" ) ) );

        assertEquals( exclude( asList( strings ), "a" ), extractCursorContent( cursor ) );
    }

    @Test
    public void shouldFilterMiddleObject() throws IOException
    {
        String[] strings = { "a", "b", "c" };

        IOCursor<String> delegate = new ArrayIOCursor<>( strings );
        FilteringIOCursor<String> cursor = new FilteringIOCursor<>( delegate, not( in( "b" ) ) );

        assertEquals( exclude( asList( strings ), "b" ), extractCursorContent( cursor ) );
    }

    @Test
    public void shouldFilterLastObject() throws IOException
    {
        String[] strings = { "a", "b", "c" };

        IOCursor<String> delegate = new ArrayIOCursor<>( strings );
        FilteringIOCursor<String> cursor = new FilteringIOCursor<>( delegate, not( in( "c" ) ) );

        assertEquals( exclude( asList( strings ), "c" ), extractCursorContent( cursor ) );
    }

    private <T> List<T> exclude( List<T> list, T... toExclude )
    {
        List<T> toReturn = new ArrayList<>( list );

        for ( T item : toExclude )
        {
            while ( toReturn.remove( item ) )
            {
                // Continue
            }
        }

        return toReturn;
    }

    private <T> List<T> extractCursorContent( FilteringIOCursor<T> cursor ) throws IOException
    {
        List<T> list = new ArrayList<>();

        while ( cursor.next() )
        {
            list.add( cursor.get() );
        }

        return list;
    }
}