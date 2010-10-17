/*
 * Copyright (c) 2002-2010 "Neo Technology,"
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
    protected Index<Node> index;
    protected GraphDatabaseService graphDb;
    protected Exception exception;
    protected CountDownLatch latch = new CountDownLatch( WAITING );
    protected AtomicInteger threadState = new AtomicInteger();
    protected static final int WAITING = 1;
    private static final int RUNNING = 2;
    protected static final int DONE = 3;
    private Queue<Command> commands = new ConcurrentLinkedQueue<Command>();

    public BaseWorker( Index<Node> index, GraphDatabaseService graphDb )
    {
        this.index = index;
        this.graphDb = graphDb;
        start();
    }

    @Override
    public void run()
    {
        CommandState state = new CommandState( index, graphDb );
        threadState.set( WorkThread.WAITING );
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
                threadState.set( WorkThread.DONE );

            } catch ( InterruptedException e )
            {
                throw new RuntimeException( e );
            } catch ( Exception exception )
            {
                this.exception = exception;
                threadState.set( WorkThread.DONE );
            }

        }
    }

    private void log( String s )
    {
        System.out.println( Thread.currentThread().getId() + " - " + s );
    }

    protected void queueCommand( Command cmd )
    {
        commands.add( cmd );
        log( "MASTER: Queuing command, and starting worker - " + cmd.getClass().getSimpleName() );
        latch.countDown();
        waitFor();
        threadState.set( WAITING );
    }

    private void waitFor()
    {
        while ( !threadState.compareAndSet( DONE, WAITING ) )
        {
            try
            {
                Thread.sleep( 10 );
            } catch ( InterruptedException e )
            {
                throw new RuntimeException( e );
            }
        }
    }
}
