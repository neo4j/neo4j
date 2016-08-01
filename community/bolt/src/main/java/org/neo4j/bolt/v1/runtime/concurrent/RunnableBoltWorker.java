/*
 * Copyright (c) 2002-2016 "Neo Technology,"
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
package org.neo4j.bolt.v1.runtime.concurrent;

import org.neo4j.bolt.v1.runtime.BoltConnectionFatality;
import org.neo4j.bolt.v1.runtime.BoltStateMachine;
import org.neo4j.bolt.v1.runtime.BoltWorker;
import org.neo4j.bolt.v1.runtime.Job;
import org.neo4j.kernel.impl.logging.LogService;
import org.neo4j.logging.Log;

import java.util.ArrayList;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.TimeUnit;

/**
 * Executes incoming Bolt requests for a given connection.
 */
class RunnableBoltWorker implements Runnable, BoltWorker
{
    /** Poison pill for closing the session and shutting down the worker */
    static final Job SHUTDOWN = session1 -> {};

    private static final int workQueueSize = Integer.getInteger( "org.neo4j.bolt.workQueueSize", 100 );

    private final ArrayBlockingQueue<Job> jobQueue = new ArrayBlockingQueue<>( workQueueSize );
    private final BoltStateMachine machine;
    private final Log log;
    private final Log userLog;
    private boolean keepRunning;

    RunnableBoltWorker( BoltStateMachine machine, LogService logging )
    {
        this.machine = machine;
        this.log = logging.getInternalLog( getClass() );
        this.userLog = logging.getUserLog( getClass() );
    }

    /**
     * Accept a command to be executed at some point in the future. This will get queued and executed as soon as
     * possible.
     * @param job an operation to be performed on the session
     */
    public void enqueue( Job job )
    {
        try
        {
            jobQueue.put( job );
        }
        catch ( InterruptedException e )
        {
            throw new RuntimeException( "Worker interrupted while queueing request, the session may have been " +
                    "forcibly closed, or the database may be shutting down." );
        }
    }

    @Override
    public void run()
    {
        keepRunning = true;
        ArrayList<Job> batch = new ArrayList<>( workQueueSize );

        try
        {
            while ( keepRunning )
            {
                Job job = jobQueue.poll( 10, TimeUnit.SECONDS );
                if ( job != null )
                {
                    execute( job );

                    for ( int jobCount = jobQueue.drainTo( batch ); keepRunning && jobCount > 0;
                          jobCount = jobQueue.drainTo( batch ) )
                    {
                        executeBatch( batch );
                    }
                }
            }
        }
        catch ( Throwable e )
        {
            log.error( "Worker for session '" + machine.key() + "' crashed: " + e.getMessage(), e );
            userLog.error( "Fatal, worker for session '" + machine.key() + "' crashed. Please" +
                           " contact your support representative if you are unable to resolve this.", e );

            // Attempt to close the session, as an effort to release locks and other resources held by the session
            machine.close();
        }
    }

    private void executeBatch( ArrayList<Job> batch ) throws BoltConnectionFatality
    {
        for ( int i = 0; keepRunning && i < batch.size(); i++ )
        {
            execute( batch.get( i ) );
        }
        batch.clear();
    }

    private void execute( Job job ) throws BoltConnectionFatality
    {
        if ( job == SHUTDOWN )
        {
            machine.close();
            keepRunning = false;
        }
        else
        {
            job.perform( machine );
        }
    }

    @Override
    public void interrupt()
    {
        machine.interrupt();
    }

    @Override
    public void halt()
    {
        try
        {
            machine.close();
        }
        finally
        {
            keepRunning = false;
        }
    }

}
