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
package org.neo4j.kernel.api.impl.fulltext;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.concurrent.Semaphore;

import org.neo4j.kernel.api.exceptions.index.IndexEntryConflictException;
import org.neo4j.kernel.api.impl.index.DatabaseIndex;
import org.neo4j.kernel.api.index.IndexEntryUpdate;
import org.neo4j.kernel.api.index.IndexUpdater;
import org.neo4j.scheduler.Group;
import org.neo4j.scheduler.JobScheduler;
import org.neo4j.storageengine.api.schema.IndexReader;
import org.neo4j.util.concurrent.BinaryLatch;

/**
 * A sink for index updates that will eventually be applied.
 */
public class IndexUpdateSink
{
    private final JobScheduler scheduler;
    private final Semaphore updateQueueLimit;

    IndexUpdateSink( JobScheduler scheduler, int eventuallyConsistentUpdateQueueLimit )
    {
        this.scheduler = scheduler;
        updateQueueLimit = new Semaphore( eventuallyConsistentUpdateQueueLimit );
    }

    public void enqueueUpdate( DatabaseIndex<? extends IndexReader> index, IndexUpdater indexUpdater, IndexEntryUpdate<?> update )
    {
        updateQueueLimit.acquireUninterruptibly();
        Runnable eventualUpdate = () ->
        {
            try
            {
                indexUpdater.process( update );
            }
            catch ( IndexEntryConflictException e )
            {
                markAsFailed( index, e );
            }
            finally
            {
                updateQueueLimit.release();
            }
        };

        try
        {
            scheduler.schedule( Group.INDEX_UPDATING, eventualUpdate );
        }
        catch ( Exception e )
        {
            updateQueueLimit.release(); // Avoid leaking permits if job scheduling fails.
            throw e;
        }
    }

    private static void markAsFailed( DatabaseIndex<? extends IndexReader> index, IndexEntryConflictException conflict )
    {
        try
        {
            index.markAsFailed( conflict.getMessage() );
        }
        catch ( IOException ioe )
        {
            ioe.addSuppressed( conflict );
            throw new UncheckedIOException( ioe );
        }
    }

    public void closeUpdater( DatabaseIndex<? extends IndexReader> index, IndexUpdater indexUpdater )
    {
        scheduler.schedule( Group.INDEX_UPDATING, () ->
        {
            try
            {
                indexUpdater.close();
            }
            catch ( IndexEntryConflictException e )
            {
                markAsFailed( index, e );
            }
        } );
    }

    public void awaitUpdateApplication()
    {
        BinaryLatch updateLatch = new BinaryLatch();
        scheduler.schedule( Group.INDEX_UPDATING, updateLatch::release );
        updateLatch.await();
    }
}
