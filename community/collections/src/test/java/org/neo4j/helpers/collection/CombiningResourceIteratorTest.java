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
package org.neo4j.helpers.collection;

import org.junit.Test;

import org.neo4j.graphdb.ResourceIterator;

import static java.util.Arrays.asList;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;
import static org.neo4j.helpers.collection.Iterators.asResourceIterator;
import static org.neo4j.helpers.collection.Iterators.iterator;

public class CombiningResourceIteratorTest
{
    @Test
    public void shouldNotCloseDuringIteration()
    {
        // Given
        ResourceIterator<Long> it1 = spy( asResourceIterator( iterator( 1L, 2L, 3L ) ) );
        ResourceIterator<Long> it2 = spy( asResourceIterator( iterator( 5L, 6L, 7L ) ) );
        CombiningResourceIterator<Long> combingIterator = new CombiningResourceIterator<>( iterator(it1, it2) );

        // When I iterate through it, things come back in the right order
        assertThat( Iterators.asList( combingIterator ), equalTo(asList( 1L, 2L, 3L, 5L, 6L, 7L )) );

        // Then
        verify(it1, never()).close();
        verify(it2, never()).close();
    }

    @Test
    public void closesAllIteratorsOnShutdown()
    {
        // Given
        ResourceIterator<Long> it1 = spy( asResourceIterator( iterator( 1L, 2L, 3L ) ) );
        ResourceIterator<Long> it2 = spy( asResourceIterator( iterator( 5L, 6L, 7L ) ) );
        CombiningResourceIterator<Long> combingIterator = new CombiningResourceIterator<>( iterator(it1, it2) );

        // Given I iterate through half of it
        int iterations = 4;
        while ( iterations-- > 0 )
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
    public void shouldHandleSingleItemIterators()
    {
        // Given
        ResourceIterator<Long> it1 = asResourceIterator( iterator( 1L ) );
        ResourceIterator<Long> it2 = asResourceIterator( iterator( 5L, 6L, 7L ) );
        CombiningResourceIterator<Long> combingIterator = new CombiningResourceIterator<>( iterator(it1, it2) );

        // When I iterate through it, things come back in the right order
        assertThat( Iterators.asList( combingIterator ), equalTo(asList( 1L, 5L, 6L, 7L )) );
    }
}
