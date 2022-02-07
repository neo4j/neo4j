/*
 * Copyright (c) "Neo4j"
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
package org.neo4j.internal.helpers.collection;

import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

import org.neo4j.function.ThrowingConsumer;
import org.neo4j.graphdb.ResourceIterable;
import org.neo4j.graphdb.ResourceIterator;

import static java.util.Arrays.asList;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.fail;
import static org.neo4j.internal.helpers.collection.Iterators.iterator;
import static org.neo4j.internal.helpers.collection.ResourceClosingIterator.newResourceIterator;

class IterablesTest
{
    @Test
    void safeForAllShouldConsumeAllSubjectsRegardlessOfSuccess()
    {
        // given
        List<String> seenSubjects = new ArrayList<>();
        List<String> failedSubjects = new ArrayList<>();
        ThrowingConsumer<String,RuntimeException> consumer = s ->
        {
            seenSubjects.add( s );

            // Fail every other
            if ( seenSubjects.size() % 2 == 1 )
            {
                failedSubjects.add( s );
                throw new RuntimeException( s );
            }
        };
        Iterable<String> subjects = asList( "1", "2", "3", "4", "5" );

        // when
        try
        {
            Iterables.safeForAll( consumer, subjects );
            fail( "Should have thrown exception" );
        }
        catch ( RuntimeException e )
        {
            // then good
            assertThat( subjects ).isEqualTo( seenSubjects );
            Iterator<String> failed = failedSubjects.iterator();
            assertThat( failed.hasNext() ).isTrue();
            assertThat( e.getMessage() ).isEqualTo( failed.next() );
            for ( Throwable suppressed : e.getSuppressed() )
            {
                assertThat( failed.hasNext() ).isTrue();
                assertThat( suppressed.getMessage() ).isEqualTo( failed.next() );
            }
            assertThat( failed.hasNext() ).isFalse();
        }
    }

    @Test
    void resourceIterableShouldNotCloseIfNoIteratorCreated()
    {
        // Given
        AtomicBoolean closed = new AtomicBoolean( false );
        ResourceIterator<Integer> resourceIterator = newResourceIterator( iterator( new Integer[0] ), () -> closed.set( true ) );

        // When
        Iterables.resourceIterable( () -> resourceIterator ).close();

        // Then
        assertThat( closed.get() ).isFalse();
    }

    @Test
    void resourceIterableShouldAlsoCloseIteratorIfResource()
    {
        // Given
        AtomicBoolean closed = new AtomicBoolean( false );
        ResourceIterator<Integer> resourceIterator = newResourceIterator( iterator( new Integer[] {1} ), () -> closed.set( true ) );

        // When
        try ( ResourceIterable<Integer> integers = Iterables.resourceIterable( () -> resourceIterator ) )
        {
            integers.iterator().next();
        }

        // Then
        assertThat( closed.get() ).isTrue();
    }
}
