/*
 * Copyright (c) 2002-2018 "Neo4j,"
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
package org.neo4j.causalclustering.core.replication;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Future;
import java.util.stream.Stream;

import org.neo4j.causalclustering.core.state.machines.dummy.DummyRequest;
import org.neo4j.internal.kernel.api.security.SecurityContext;
import org.neo4j.logging.Log;
import org.neo4j.procedure.Admin;
import org.neo4j.procedure.Context;
import org.neo4j.procedure.Description;
import org.neo4j.procedure.Name;
import org.neo4j.procedure.Procedure;

import static java.lang.Math.toIntExact;
import static org.neo4j.procedure.Mode.DBMS;

@SuppressWarnings( "unused" )
public class ReplicationBenchmarkProcedure
{
    @Context
    public Replicator replicator;

    @Context
    public SecurityContext securityContext;

    @Context
    public Log log;

    private static long startTime;
    private static List<Worker> workers;

    @Admin
    @Description( "Start the benchmark." )
    @Procedure( name = "dbms.cluster.benchmark.start", mode = DBMS )
    public synchronized void start( @Name( "nThreads" ) Long nThreads, @Name( "blockSize" ) Long blockSize )
    {
        if ( workers != null )
        {
            throw new IllegalStateException( "Already running." );
        }

        log.info( "Starting replication benchmark procedure" );

        startTime = System.currentTimeMillis();
        workers = new ArrayList<>( toIntExact( nThreads ) );

        for ( int i = 0; i < nThreads; i++ )
        {
            Worker worker = new Worker( toIntExact( blockSize ) );
            workers.add( worker );
            worker.start();
        }
    }

    @Admin
    @Description( "Stop a running benchmark." )
    @Procedure( name = "dbms.cluster.benchmark.stop", mode = DBMS )
    public synchronized Stream<BenchmarkResult> stop() throws InterruptedException
    {
        if ( workers == null )
        {
            throw new IllegalStateException( "Not running." );
        }

        log.info( "Stopping replication benchmark procedure" );

        for ( Worker worker : workers )
        {
            worker.stop();
        }

        for ( Worker worker : workers )
        {
            worker.join();
        }

        long runTime = System.currentTimeMillis() - startTime;

        long totalRequests = 0;
        long totalBytes = 0;

        for ( Worker worker : workers )
        {
            totalRequests += worker.totalRequests;
            totalBytes += worker.totalBytes;
        }

        workers = null;

        return Stream.of( new BenchmarkResult( totalRequests, totalBytes, runTime ) );
    }

    private class Worker implements Runnable
    {
        private final int blockSize;

        long totalRequests;
        long totalBytes;

        private Thread t;
        private volatile boolean stopped;

        Worker( int blockSize )
        {
            this.blockSize = blockSize;
        }

        void start()
        {
            t = new Thread( this );
            t.start();
        }

        @Override
        public void run()
        {
            try
            {
                while ( !stopped )
                {
                    Future<Object> future = replicator.replicate( new DummyRequest( new byte[blockSize] ), true );
                    DummyRequest request = (DummyRequest) future.get();
                    totalRequests++;
                    totalBytes += request.byteCount();
                }
            }
            catch ( Throwable e )
            {
                log.error( "Worker exception", e );
            }
        }

        void stop()
        {
            stopped = true;
        }

        void join() throws InterruptedException
        {
            t.join();
        }
    }
}
