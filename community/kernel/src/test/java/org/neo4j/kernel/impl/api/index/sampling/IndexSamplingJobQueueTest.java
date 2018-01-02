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
package org.neo4j.kernel.impl.api.index.sampling;

import org.junit.Test;
import org.neo4j.function.Predicate;
import org.neo4j.function.Predicates;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.neo4j.helpers.collection.Iterables.toArray;

public class IndexSamplingJobQueueTest
{
    public static final Predicate<Object> TRUE = Predicates.alwaysTrue();
    public static final Predicate<Object> FALSE = Predicates.alwaysFalse();

    @Test
    public void returnsNullWhenEmpty()
    {
        assertNull( new IndexSamplingJobQueue<>( Predicates.alwaysTrue() ).poll() );
    }

    @Test
    public void shouldEnqueueJobWhenEmpty()
    {
        // given
        final IndexSamplingJobQueue<Object> jobQueue = new IndexSamplingJobQueue<>( TRUE );
        jobQueue.add( false, something );

        // when
        Object result = jobQueue.poll();

        // then
        assertEquals( something, result );
    }

    @Test
    public void shouldEnqueueJobOnlyOnce()
    {
        // given
        final IndexSamplingJobQueue<Object> jobQueue = new IndexSamplingJobQueue<>( TRUE );
        jobQueue.add( false, something );

        // when
        jobQueue.add( false, something );

        // then
        assertEquals( something, jobQueue.poll() );
        assertNull( jobQueue.poll() );
    }

    @Test
    public void shouldNotEnqueueJobOnlyIfForbiddenByThePredicate()
    {
        // given
        final IndexSamplingJobQueue<Object> jobQueue = new IndexSamplingJobQueue<>( FALSE );

        // when
        jobQueue.add( false, something );

        // then
        assertNull( jobQueue.poll() );
    }

    @Test
    public void shouldForceEnqueueOfAnJobEvenIfThePredicateForbidsIt()
    {
        // given
        final IndexSamplingJobQueue<Object> jobQueue = new IndexSamplingJobQueue<>( FALSE );

        // when
        jobQueue.add( true, something );

        // then
        assertEquals( something, jobQueue.poll() );
    }

    @Test
    public void shouldDequeueAll()
    {
        // given
        final Object somethingElse = new Object();
        final IndexSamplingJobQueue<Object> jobQueue = new IndexSamplingJobQueue<>( TRUE );
        jobQueue.add( false, something );
        jobQueue.add( false, somethingElse );

        // when
        Iterable<Object> objects = jobQueue.pollAll();

        // then
        assertArrayEquals(
                new Object[]{something, somethingElse},
                toArray( Object.class, objects )
        );
        assertNull( jobQueue.poll() );
    }

    private final Object something = new Object();
}
