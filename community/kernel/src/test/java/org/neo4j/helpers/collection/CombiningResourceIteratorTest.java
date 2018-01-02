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

import org.junit.Test;

import org.neo4j.graphdb.ResourceIterator;

import static java.util.Arrays.asList;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;

import static org.neo4j.helpers.collection.IteratorUtil.asResourceIterator;
import static org.neo4j.helpers.collection.IteratorUtil.iterator;

public class CombiningResourceIteratorTest
{
    @Test
    public void shouldNotCloseDuringIteration() throws Exception
    {
        // Given
        ResourceIterator<Long> it1 = spy( asResourceIterator( iterator( 1l, 2l, 3l ) ) );
        ResourceIterator<Long> it2 = spy( asResourceIterator( iterator( 5l, 6l, 7l ) ) );
        CombiningResourceIterator<Long> combingIterator = new CombiningResourceIterator<>( iterator(it1, it2) );

        // When I iterate through it, things come back in the right order
        assertThat( IteratorUtil.asList( combingIterator ), equalTo(asList(1l,2l,3l,5l,6l,7l)) );

        // Then
        verify(it1, never()).close();
        verify(it2, never()).close();
    }

    @Test
    public void closesAllIteratorsOnShutdown() throws Exception
    {
        // Given
        ResourceIterator<Long> it1 = spy( asResourceIterator( iterator( 1l, 2l, 3l ) ) );
        ResourceIterator<Long> it2 = spy( asResourceIterator( iterator( 5l, 6l, 7l ) ) );
        CombiningResourceIterator<Long> combingIterator = new CombiningResourceIterator<>( iterator(it1, it2) );

        // Given I iterate through half of it
        int iterations = 4;
        while( iterations --> 0 )
        {
            combingIterator.next();
        }

        // When
        combingIterator.close();

        // Then
        verify(it1).close();
        verify(it2).close();
    }

    @Test
    public void shouldHandleSingleItemIterators() throws Exception
    {
        // Given
        ResourceIterator<Long> it1 = asResourceIterator( iterator( 1l) );
        ResourceIterator<Long> it2 = asResourceIterator( iterator( 5l, 6l, 7l ) );
        CombiningResourceIterator<Long> combingIterator = new CombiningResourceIterator<>( iterator(it1, it2) );

        // When I iterate through it, things come back in the right order
        assertThat( IteratorUtil.asList( combingIterator ), equalTo(asList(1l,5l,6l,7l)) );
    }
}
