/*
 * Copyright (c) 2002-2016 "Neo Technology,"
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
package org.neo4j.graphdb;

import org.junit.Test;

import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;

import static java.util.Arrays.asList;
import static junit.framework.TestCase.assertEquals;
import static junit.framework.TestCase.assertTrue;
import static org.neo4j.helpers.collection.Iterators.iterator;
import static org.neo4j.helpers.collection.ResourceClosingIterator.newResourceIterator;

public class ResourceIterableTest
{
    @Test
    public void streamShouldCloseOnCompleted() throws Throwable
    {
        // Given
        AtomicBoolean closed = new AtomicBoolean( false );
        ResourceIterator<Integer> resourceIterator = newResourceIterator( () -> closed.set( true ), iterator( new Integer[]{1, 2, 3} ) );

        ResourceIterable<Integer> iterable = () -> resourceIterator;

        // When
        List<Integer> result = iterable.stream().collect( Collectors.toList() );

        // Then
        assertEquals( asList(1,2,3), result );
        assertTrue( closed.get() );
    }
}