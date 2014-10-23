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

import org.neo4j.helpers.collection.Iterables;
import org.neo4j.helpers.collection.IteratorUtil;
import org.neo4j.kernel.api.index.IndexDescriptor;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.neo4j.helpers.collection.Iterables.toArray;

import java.util.Arrays;

public class IndexSamplingJobQueueTest
{
    private final IndexDescriptor descriptor = new IndexDescriptor( 1, 2 );

    @Test
    public void returnsNullWhenEmpty()
    {
        assertNull( new IndexSamplingJobQueue().poll() );
    }

    @Test
    public void enqueuesJobWhenEmpty()
    {
        // given
        IndexSamplingJobQueue queue = new IndexSamplingJobQueue();
        queue.sampleIndex( descriptor );

        // when
        IndexDescriptor result = queue.poll();

        // then
        assertEquals( descriptor, result );
    }

    @Test
    public void enqueuesJobOnlyOnce()
    {
        // given
        IndexSamplingJobQueue queue = new IndexSamplingJobQueue();
        queue.sampleIndex( descriptor );

        // when
        queue.sampleIndex( descriptor );

        // then
        assertEquals( descriptor, queue.poll() );
        assertNull( queue.poll() );
    }

    @Test
    public void dequeuesAll()
    {
        // given
        IndexSamplingJobQueue queue = new IndexSamplingJobQueue();
        IndexDescriptor anotherDescriptor = new IndexDescriptor( 3, 4 );
        queue.sampleIndex( descriptor );
        queue.sampleIndex( anotherDescriptor );

        // when
        Iterable<IndexDescriptor> descriptors = queue.pollAll();

        // then
        assertArrayEquals(
                new IndexDescriptor[]{descriptor, anotherDescriptor},
                toArray( IndexDescriptor.class, descriptors )
        );
        assertNull( queue.poll() );
    }
}
