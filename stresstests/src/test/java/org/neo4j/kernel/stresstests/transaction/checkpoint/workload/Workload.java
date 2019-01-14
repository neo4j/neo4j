/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 *
 * This file is part of Neo4j Enterprise Edition. The included source
 * code can be redistributed and/or modified under the terms of the
 * GNU AFFERO GENERAL PUBLIC LICENSE Version 3
 * (http://www.fsf.org/licensing/licenses/agpl-3.0.html) with the
 * Commons Clause, as found in the associated LICENSE.txt file.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * Neo4j object code can be licensed independently from the source
 * under separate terms from the AGPL. Inquiries can be directed to:
 * licensing@neo4j.com
 *
 * More information is also available at:
 * https://neo4j.com/licensing/
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
        TransactionThroughput NONE = ( transactions, timeSlotMillis ) ->
        {
            // ignore
        };

        void report( long transactions, long timeSlotMillis );
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
