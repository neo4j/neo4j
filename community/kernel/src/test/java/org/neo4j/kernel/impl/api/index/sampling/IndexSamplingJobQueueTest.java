/**
 * Copyright (c) 2002-2014 "Neo Technology,"
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

import org.neo4j.kernel.api.index.IndexDescriptor;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

import static org.neo4j.helpers.Predicates.TRUE;
import static org.neo4j.helpers.Predicates.not;
import static org.neo4j.helpers.collection.Iterables.toArray;
import static org.neo4j.kernel.impl.api.index.sampling.IndexSamplingMode.BACKGROUND_REBUILD_UPDATED;

public class IndexSamplingJobQueueTest
{
    @Test
    public void returnsNullWhenEmpty()
    {
        assertNull( new IndexSamplingJobQueue( TRUE() ).poll() );
    }

    @Test
    public void enqueuesJobWhenEmpty()
    {
        // given
        final IndexSamplingJobQueue jobQueue = new IndexSamplingJobQueue( TRUE() );
        jobQueue.sampleIndex( BACKGROUND_REBUILD_UPDATED, descriptor );

        // when
        IndexDescriptor result = jobQueue.poll();

        // then
        assertEquals( descriptor, result );
    }

    @Test
    public void enqueuesJobOnlyOnce()
    {
        // given
        final IndexSamplingJobQueue jobQueue = new IndexSamplingJobQueue( TRUE() );
        jobQueue.sampleIndex( BACKGROUND_REBUILD_UPDATED, descriptor );

        // when
        jobQueue.sampleIndex( BACKGROUND_REBUILD_UPDATED, descriptor );

        // then
        assertEquals( descriptor, jobQueue.poll() );
        assertNull( jobQueue.poll() );
    }

    @Test
    public void enqueuesJobOnlyIfUpdatedIfThatIsRequested()
    {
        // given
        final IndexSamplingJobQueue jobQueue = new IndexSamplingJobQueue( not( TRUE() ) );

        // when
        jobQueue.sampleIndex( BACKGROUND_REBUILD_UPDATED, descriptor );

        // then
        assertNull( jobQueue.poll() );
    }

    @Test
    public void dequeuesAll()
    {
        // given
        final IndexDescriptor anotherDescriptor = new IndexDescriptor( 3, 4 );
        final IndexSamplingJobQueue jobQueue = new IndexSamplingJobQueue( TRUE() );
        jobQueue.sampleIndex( BACKGROUND_REBUILD_UPDATED, descriptor );
        jobQueue.sampleIndex( BACKGROUND_REBUILD_UPDATED, anotherDescriptor );

        // when
        Iterable<IndexDescriptor> descriptors = jobQueue.pollAll();

        // then
        assertArrayEquals(
                new IndexDescriptor[]{descriptor, anotherDescriptor},
                toArray( IndexDescriptor.class, descriptors )
        );
        assertNull( jobQueue.poll() );
    }

    private final IndexDescriptor descriptor = new IndexDescriptor( 1, 2 );
}
