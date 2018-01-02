/*
 * Copyright (c) 2002-2018 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
 *
 * This file is part of Neo4j.
 *
 * Neo4j is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program. If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.kernel.stresstests.transaction.checkpoint.workload;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Resource;
import org.neo4j.kernel.stresstests.transaction.checkpoint.mutation.RandomMutation;

public class Workload implements Resource
{
    private final int threads;
    private final SyncMonitor sync;
    private final Worker worker;
    private final ExecutorService executor;

    public Workload( GraphDatabaseService db, RandomMutation randomMutation, int threads )
    {
        this.threads = threads;
        this.sync = new SyncMonitor( threads );
        this.worker = new Worker( db, randomMutation, sync, 100 );
        this.executor = Executors.newCachedThreadPool();
    }

    public interface TransactionThroughput
    {
        TransactionThroughput NONE = new TransactionThroughput()
        {
            @Override
            public void report( long transactions, long timeSlotMillis )
            {
                // ignore
            }
        };

        void report( long transactions, long timeSlotMillis);
    }

    public void run( long runningTimeMillis, TransactionThroughput throughput )
            throws InterruptedException
    {
        for ( int i = 0; i < threads; i++ )
        {
            executor.submit( worker );
        }

        TimeUnit.SECONDS.sleep( 20 ); // sleep to make sure workers are started

        long now = System.currentTimeMillis();
        long previousReportTime = System.currentTimeMillis();
        long finishLine = runningTimeMillis + now;
        long sampleRate = TimeUnit.SECONDS.toMillis( 10 );
        long lastReport = sampleRate + now;
        long previousTransactionCount = sync.transactions();
        Thread.sleep( sampleRate );
        do
        {
            now = System.currentTimeMillis();
            if ( lastReport <= now )
            {
                long currentTransactionCount = sync.transactions();
                long diff = currentTransactionCount - previousTransactionCount;
                throughput.report( diff, now - previousReportTime );

                previousReportTime = now;
                previousTransactionCount = currentTransactionCount;

                lastReport = sampleRate + now;
                Thread.sleep( sampleRate );
            }
            else
            {
                Thread.sleep( 10 );
            }
        }
        while ( now < finishLine );

        if ( lastReport < now )
        {
            long diff = sync.transactions() - previousTransactionCount;
            throughput.report( diff, now - previousReportTime );
        }
        sync.stopAndWaitWorkers();

    }

    @Override
    public void close()
    {
        try
        {
            executor.shutdown();
            executor.awaitTermination( 10, TimeUnit.SECONDS );
        }
        catch ( Exception e )
        {
            throw new RuntimeException( e );
        }
    }
}
