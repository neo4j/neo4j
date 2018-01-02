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
package org.neo4j.consistency.checking.full;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Base class for workers that processes records during consistency check.
 */
public class RecordCheckWorker<RECORD> implements Runnable
{
    private volatile boolean done;
    protected final BlockingQueue<RECORD> recordsQ;
    private final int id;
    private final AtomicInteger idQueue;
    private final RecordProcessor<RECORD> processor;

    public RecordCheckWorker( int id, AtomicInteger idQueue, BlockingQueue<RECORD> recordsQ,
            RecordProcessor<RECORD> processor )
    {
        this.id = id;
        this.idQueue = idQueue;
        this.recordsQ = recordsQ;
        this.processor = processor;
    }

    public void done()
    {
        done = true;
    }

    @Override
    public void run()
    {
        // We assign threads to ids, first come first serve and the the thread assignment happens
        // inside the record processing which accesses CacheAccess#client() and that happens
        // lazily. So... we need to coordinate so that the processing threads initializes the processing
        // in order of thread id. This may change later so that the thread ids are assigned
        // explicitly on creating the threads... which should be much better, although hard with
        // the current design due to the state living inside ThreadLocal which makes it depend
        // on the actual and correct thread making the call... which is what we do here.
        awaitMyTurnToInitialize();

        // This was the first record, the first record processing has now happened and so we
        // can notify the others that we have initialized this thread id and the next one
        // can go ahead and do so.
        processor.init( id );
        tellNextThreadToInitialize();

        while ( !done || !recordsQ.isEmpty() )
        {
            RECORD record;
            try
            {
                record = recordsQ.poll( 10, TimeUnit.MILLISECONDS );
                if ( record != null )
                {
                    processor.process( record );
                }
            }
            catch ( InterruptedException e )
            {
                Thread.interrupted();
                break;
            }
        }
    }

    private void awaitMyTurnToInitialize()
    {
        while ( idQueue.get() < id-1 )
        {
            try
            {
                Thread.sleep( 10 );
            }
            catch ( InterruptedException e )
            {
                Thread.interrupted();
                break;
            }
        }
    }

    private void tellNextThreadToInitialize()
    {
        boolean set = idQueue.compareAndSet( id-1, id );
        assert set : "Something wrong with the design here";
    }
}
