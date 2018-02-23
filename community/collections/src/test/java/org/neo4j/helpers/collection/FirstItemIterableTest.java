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
package org.neo4j.helpers.collection;

import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.Iterator;
import java.util.NoSuchElementException;

import static java.util.Arrays.asList;
import static org.hamcrest.Matchers.instanceOf;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.fail;

/**
 * @author mh
 * @since 19.04.12
 */
public class FirstItemIterableTest
{
    @Test
    public void testEmptyIterator()
    {
        FirstItemIterable<?> firstItemIterable = new FirstItemIterable<>( Collections.emptyList() );
        Iterator<?> empty = firstItemIterable.iterator();
        assertEquals( false, empty.hasNext() );
        try
        {
            empty.next();
            fail( "Exception expected" );
        }
        catch ( Exception e )
        {
            assertThat( e, instanceOf( NoSuchElementException.class ) );
        }
        assertEquals( null, firstItemIterable.getFirst() );
    }

    @Test
    public void testSingleIterator()
    {
        FirstItemIterable<Boolean> firstItemIterable = new FirstItemIterable<>( Collections.singleton( Boolean.TRUE ) );
        Iterator<Boolean> empty = firstItemIterable.iterator();
        assertEquals( true, empty.hasNext() );
        assertEquals( Boolean.TRUE, empty.next() );
        assertEquals( Boolean.TRUE, firstItemIterable.getFirst() );
        assertEquals( false, empty.hasNext() );
        try
        {
            empty.next();
            fail( "Exception expected" );
        }
        catch ( Exception e )
        {
            assertThat( e, instanceOf( NoSuchElementException.class ) );
        }
        assertEquals( Boolean.TRUE, firstItemIterable.getFirst() );
    }

    @Test
    public void testMultiIterator()
    {
        FirstItemIterable<Boolean> firstItemIterable = new FirstItemIterable<>( asList( Boolean.TRUE, Boolean.FALSE ) );
        Iterator<Boolean> empty = firstItemIterable.iterator();
        assertEquals( true, empty.hasNext() );
        assertEquals( Boolean.TRUE, empty.next() );
        assertEquals( Boolean.TRUE, firstItemIterable.getFirst() );
        assertEquals( true, empty.hasNext() );
        assertEquals( Boolean.FALSE, empty.next() );
        assertEquals( Boolean.TRUE, firstItemIterable.getFirst() );
        assertEquals( false, empty.hasNext() );
        try
        {
            empty.next();
            fail( "Exception expected" );
        }
        catch ( Exception e )
        {
            assertThat( e, instanceOf( NoSuchElementException.class ) );
        }
        assertEquals( Boolean.TRUE, firstItemIterable.getFirst() );
    }
}
