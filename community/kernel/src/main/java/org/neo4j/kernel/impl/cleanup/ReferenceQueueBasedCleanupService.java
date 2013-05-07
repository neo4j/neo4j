/**
 * Copyright (c) 2002-2013 "Neo Technology,"
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
package org.neo4j.kernel.impl.cleanup;

import static org.neo4j.helpers.collection.IteratorUtil.EMPTY_CLOSEABLE;

import java.io.Closeable;
import java.util.Iterator;
import java.util.concurrent.TimeUnit;

import org.neo4j.graphdb.ResourceIterator;
import org.neo4j.helpers.Thunk;
import org.neo4j.helpers.collection.ResourceClosingIterator;
import org.neo4j.kernel.impl.util.JobScheduler;
import org.neo4j.kernel.logging.Logging;

class ReferenceQueueBasedCleanupService extends CleanupService implements Runnable
{
    private volatile boolean running;
    private final JobScheduler scheduler;
    private final Thunk<Boolean> cleanupNecessity;
    final CleanupReferenceQueue collectedReferences;
    CleanupReference first;

    ReferenceQueueBasedCleanupService( JobScheduler scheduler, Logging logging,
            CleanupReferenceQueue collectedReferences, Thunk<Boolean> cleanupNecessity )
    {
        super( logging );
        this.scheduler = scheduler;
        this.collectedReferences = collectedReferences;
        this.cleanupNecessity = cleanupNecessity;
    }

    @Override
    public <T> ResourceIterator<T> resourceIterator( Iterator<T> iterator, Closeable closeable )
    {
        if ( cleanupNecessity.evaluate() )
        {
            return linked( new AutoCleanupResourceIterator<T>( iterator ), closeable );
        }
        else
        {
            // Just pick the best way of wrapping an Iterator in a ResourceIterator, bypassing cleanup
            return ResourceClosingIterator.newResourceIterator( EMPTY_CLOSEABLE, iterator );
        }
    }

    private <T> ResourceIterator<T> linked( AutoCleanupResourceIterator<T> iterator, Closeable handler )
    {
        CleanupReference cleanup = new CleanupReference( iterator, this, handler );
        link( cleanup );
        iterator.cleanup = cleanup;
        return iterator;
    }

    @Override
    public void start()
    {
        running = true;
        scheduler.scheduleRecurring( this, 1, TimeUnit.SECONDS );
    }

    @Override
    public void run()
    {
        if ( running )
        {
            for ( CleanupReference reference; running && (reference = collectedReferences.remove()) != null; )
            {
                cleanup( reference );
            }
        }
    }

    @Override
    public synchronized void stop()
    {
        running = false;
        for ( CleanupReference cur = first; cur != null; cur = cur.next )
        {
            cleanup( cur );
        }
        first = null;
    }

    synchronized void link( CleanupReference reference )
    {
        CleanupReference next = first;
        reference.next = next;
        first = reference;
        if ( next != null )
        {
            next.prev = reference;
        }
    }

    synchronized void unlink( CleanupReference reference )
    {
        CleanupReference prev = reference.prev, next = reference.next;
        if ( prev == null )
        {
            first = next;
        }
        else
        {
            prev.next = next;
        }
        if ( next != null )
        {
            next.prev = prev;
        }
    }
}
