/**
 * Copyright (c) 2002-2011 "Neo Technology,"
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
package org.neo4j.index.impl.lucene;

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.index.Index;

import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;

public class BaseWorker extends Thread
{
    private static final int WAITING = 1;
    private static final int RUNNING = 2;
    private static final int DONE = 3;
    private static final int STARTING = 4;
    
    protected final Index<Node> index;
    protected final GraphDatabaseService graphDb;
    private volatile Exception exception;
    private volatile CountDownLatch latch = new CountDownLatch( 1 );
    private final AtomicInteger threadState = new AtomicInteger( STARTING );
    private final Queue<Command> commands = new ConcurrentLinkedQueue<Command>();

    public BaseWorker( Index<Node> index, GraphDatabaseService graphDb )
    {
        this.index = index;
        this.graphDb = graphDb;
        waitForWorkerToStart();
    }

    @Override
    public void run()
    {
        final CommandState state = new CommandState( index, graphDb );
        threadState.set( STARTING );
        while ( state.alive )
        {
            try
            {
                latch = new CountDownLatch( 1 );
                log( "WORKER: Waiting for latch" );
                latch.await();
                threadState.set( RUNNING );
                Command command = commands.poll();
                log( "WORKER: I have a command! " + command.getClass().getSimpleName() );
                command.doWork( state );
                threadState.set( DONE );

            }
            catch ( InterruptedException e )
            {
                throw new RuntimeException( e );
            }
            catch ( Exception exception )
            {
                this.exception = exception;
                threadState.set( DONE );
            }

        }
    }

    private void log( String s )
    {
//        System.out.println( Thread.currentThread().getId() + " - " + s );
    }

    protected void queueCommand( Command cmd )
    {
        commands.add( cmd );
        log( "MASTER: Queuing command, and starting worker - " + cmd.getClass().getSimpleName() );
        latch.countDown();
        waitForCommandToComplete();
        threadState.set( WAITING );
    }

    public void waitForCommandToComplete()
    {
        waitFor( DONE, WAITING );
    }

    public void waitForWorkerToStart()
    {
        waitFor( STARTING, WAITING );
    }

    private void waitFor( int expectedState, int newState )
    {
        int retries = 0;
        while ( !threadState.compareAndSet( expectedState, newState ) && retries++ < 100 )
        {
            try
            {
                Thread.sleep( 10 );
            }
            catch ( InterruptedException e )
            {
                throw new RuntimeException( e );
            }
        }

        if (retries > 300)
        {
            throw new IllegalStateException( "Something didn't finish in a timely manner. Aborting..." );
        }
    }
    
    public boolean hasException()
    {
        return exception != null;
    }
}
