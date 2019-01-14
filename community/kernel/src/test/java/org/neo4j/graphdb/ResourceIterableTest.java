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
package org.neo4j.graphdb;

import org.junit.Test;

import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

import static java.util.Arrays.asList;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.neo4j.helpers.collection.Iterators.iterator;
import static org.neo4j.helpers.collection.ResourceClosingIterator.newResourceIterator;

public class ResourceIterableTest
{
    @Test
    public void streamShouldCloseSingleOnCompleted()
    {
        // Given
        AtomicBoolean closed = new AtomicBoolean( false );
        ResourceIterator<Integer> resourceIterator = newResourceIterator( iterator( new Integer[]{1, 2, 3} ), () -> closed.set( true ) );

        ResourceIterable<Integer> iterable = () -> resourceIterator;

        // When
        List<Integer> result = iterable.stream().collect( Collectors.toList() );

        // Then
        assertEquals( asList(1,2,3), result );
        assertTrue( closed.get() );
    }

    @Test
    public void streamShouldCloseMultipleOnCompleted()
    {
        // Given
        AtomicInteger closed = new AtomicInteger();
        Resource resource = closed::incrementAndGet;
        ResourceIterator<Integer> resourceIterator =
                newResourceIterator( iterator( new Integer[]{1, 2, 3} ), resource, resource );

        ResourceIterable<Integer> iterable = () -> resourceIterator;

        // When
        List<Integer> result = iterable.stream().collect( Collectors.toList() );

        // Then
        assertEquals( asList(1,2,3), result );
        assertEquals( "two calls to close", 2, closed.get() );
    }
}
