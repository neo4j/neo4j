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

import java.util.ArrayDeque;
import java.util.HashSet;
import java.util.Queue;
import java.util.Set;

import org.neo4j.helpers.Predicate;
import org.neo4j.kernel.api.index.IndexDescriptor;

public class IndexSamplingJobQueue
{
    private final Predicate<? super IndexDescriptor> updatePredicate;
    private final Queue<IndexDescriptor> queue;

    public IndexSamplingJobQueue( Predicate<? super IndexDescriptor> updatePredicate )
    {
        this.updatePredicate = updatePredicate;
        this.queue = new ArrayDeque<>();
    }

    public synchronized void sampleIndex( IndexSamplingMode mode, IndexDescriptor descriptor ) {
        if ( shouldSample( mode, descriptor ) )
        {
            queue.add( descriptor );
        }
    }

    private boolean shouldSample( IndexSamplingMode mode, IndexDescriptor descriptor )
    {

        // Add index if not in queue
        if ( queue.contains( descriptor ) )
        {
            return false;
        }

        // and either adding all
        if ( ! mode.sampleOnlyIfUpdated )
        {
            return true;
        }

        // or otherwise only if seen enough updates (as determined by updatePredicate)
        return updatePredicate.accept( descriptor );
    }

    public synchronized IndexDescriptor poll()
    {
        return queue.poll();
    }

    public synchronized Iterable<IndexDescriptor> pollAll()
    {
        Set<IndexDescriptor> descriptors = new HashSet<>();
        while ( true )
        {
            IndexDescriptor descriptor = queue.poll();
            if ( descriptor == null )
            {
                return descriptors;
            }
            descriptors.add( descriptor );
        }
    }
}
