/*
 * Copyright (c) 2002-2015 "Neo Technology,"
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
package org.neo4j.ndp.runtime.internal.concurrent;

import java.util.ArrayList;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.TimeUnit;

import org.neo4j.function.Consumer;
import org.neo4j.kernel.impl.logging.LogService;
import org.neo4j.logging.Log;
import org.neo4j.ndp.runtime.Session;

/**
 * Executes incoming session commands on a specified session.
 */
public class SessionWorker implements Runnable
{
    /** Poison pill for closing the session and shutting down the worker */
    public static final Consumer<Session> SHUTDOWN = new Consumer<Session>()
    {
        @Override
        public void accept( Session session )
        {

        }
    };

    private final static int workQueueSize = Integer.getInteger( "org.neo4j.ndp.workQueueSize", 100 );

    private final ArrayBlockingQueue<Consumer<Session>> workQueue = new ArrayBlockingQueue<>( workQueueSize );
    private final Session session;
    private final Log log;
    private final Log userLog;
    private boolean keepRunning;

    public SessionWorker( Session session, LogService logging )
    {
        this.session = session;
        this.log = logging.getInternalLog( getClass() );
        this.userLog = logging.getUserLog( getClass() );
    }

    /**
     * Accept a command to be executed at some point in the future. This will get queued and executed as soon as
     * possible.
     * @param command an operation to be performed on the session
     */
    public void handle( Consumer<Session> command ) throws InterruptedException
    {
        workQueue.put( command );
    }

    @Override
    public void run()
    {
        keepRunning = true;
        ArrayList<Consumer<Session>> batch = new ArrayList<>( workQueueSize );

        try
        {
            while ( keepRunning )
            {
                Consumer<Session> work = workQueue.poll( 10, TimeUnit.SECONDS );
                if ( work != null )
                {
                    execute( work );

                    for ( int items = workQueue.drainTo( batch ); keepRunning && items > 0;
                          items = workQueue.drainTo( batch ) )
                    {
                        executeBatch( batch );
                    }
                }
            }
        }
        catch ( Throwable e )
        {
            log.error( "Worker for session '" + session.key() + "' crashed: " + e.getMessage(), e );
            userLog.error( "Fatal, worker for session '" + session.key() + "' crashed. Please" +
                           " contact your support representative if you are unable to resolve this.", e );

            // Attempt to close the session, as an effort to release locks and other resources held by the session
            session.close();
        }
    }

    private void executeBatch( ArrayList<Consumer<Session>> batch )
    {
        for ( int i = 0; keepRunning && i < batch.size(); i++ )
        {
            execute( batch.get( i ) );
        }
        batch.clear();
    }

    private void execute( Consumer<Session> work )
    {
        if ( work == SHUTDOWN )
        {
            session.close();
            keepRunning = false;
        }
        else
        {
            work.accept( session );
        }
    }
}
