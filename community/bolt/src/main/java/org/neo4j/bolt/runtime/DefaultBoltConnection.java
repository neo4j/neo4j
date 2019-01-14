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
package org.neo4j.bolt.runtime;

import io.netty.channel.Channel;
import org.apache.commons.lang3.exception.ExceptionUtils;

import java.net.SocketAddress;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.atomic.AtomicBoolean;

import org.neo4j.bolt.BoltChannel;
import org.neo4j.bolt.BoltKernelExtension;
import org.neo4j.bolt.v1.packstream.PackOutput;
import org.neo4j.bolt.v1.runtime.BoltConnectionAuthFatality;
import org.neo4j.bolt.v1.runtime.BoltProtocolBreachFatality;
import org.neo4j.bolt.v1.runtime.BoltStateMachine;
import org.neo4j.bolt.v1.runtime.Job;
import org.neo4j.bolt.v1.runtime.Neo4jError;
import org.neo4j.kernel.api.exceptions.Status;
import org.neo4j.kernel.impl.logging.LogService;
import org.neo4j.logging.Log;
import org.neo4j.util.FeatureToggles;

import static java.util.concurrent.TimeUnit.SECONDS;

public class DefaultBoltConnection implements BoltConnection
{
    protected static final int DEFAULT_MAX_BATCH_SIZE = FeatureToggles.getInteger( BoltKernelExtension.class, "max_batch_size", 100 );

    private final String id;

    private final BoltChannel channel;
    private final BoltStateMachine machine;
    private final BoltConnectionLifetimeListener listener;
    private final BoltConnectionQueueMonitor queueMonitor;
    private final PackOutput output;

    private final Log log;
    private final Log userLog;

    private final int maxBatchSize;
    private final List<Job> batch;
    private final LinkedBlockingQueue<Job> queue = new LinkedBlockingQueue<>();

    private final AtomicBoolean shouldClose = new AtomicBoolean();
    private final AtomicBoolean closed = new AtomicBoolean();

    public DefaultBoltConnection( BoltChannel channel, PackOutput output, BoltStateMachine machine, LogService logService,
            BoltConnectionLifetimeListener listener,
            BoltConnectionQueueMonitor queueMonitor )
    {
        this( channel, output, machine, logService, listener, queueMonitor, DEFAULT_MAX_BATCH_SIZE );
    }

    public DefaultBoltConnection( BoltChannel channel, PackOutput output, BoltStateMachine machine, LogService logService,
            BoltConnectionLifetimeListener listener,
            BoltConnectionQueueMonitor queueMonitor, int maxBatchSize )
    {
        this.id = channel.id();
        this.channel = channel;
        this.output = output;
        this.machine = machine;
        this.listener = listener;
        this.queueMonitor = queueMonitor;
        this.log = logService.getInternalLog( getClass() );
        this.userLog = logService.getUserLog( getClass() );
        this.maxBatchSize = maxBatchSize;
        this.batch = new ArrayList<>( maxBatchSize );
    }

    @Override
    public String id()
    {
        return id;
    }

    @Override
    public SocketAddress localAddress()
    {
        return channel.serverAddress();
    }

    @Override
    public SocketAddress remoteAddress()
    {
        return channel.clientAddress();
    }

    @Override
    public Channel channel()
    {
        return channel.rawChannel();
    }

    @Override
    public PackOutput output()
    {
        return output;
    }

    @Override
    public boolean hasPendingJobs()
    {
        return !queue.isEmpty();
    }

    @Override
    public void start()
    {
        notifyCreated();
    }

    @Override
    public void enqueue( Job job )
    {
        enqueueInternal( job );
    }

    @Override
    public boolean processNextBatch()
    {
        return processNextBatch( maxBatchSize, false );
    }

    protected boolean processNextBatch( int batchCount, boolean exitIfNoJobsAvailable )
    {
        try
        {
            boolean waitForMessage = false;
            boolean loop = false;
            do
            {
                // exit loop if we'll close the connection
                if ( willClose() )
                {
                    break;
                }

                // do we have pending jobs or shall we wait for new jobs to
                // arrive, which is required only for releasing stickiness
                // condition to this thread
                if ( waitForMessage || !queue.isEmpty() )
                {
                    queue.drainTo( batch, batchCount );
                    // if we expect one message but did not get any (because it was already
                    // processed), silently exit
                    if ( batch.size() == 0 && !exitIfNoJobsAvailable )
                    {
                        // loop until we get a new job, if we cannot then validate
                        // transaction to check for termination condition. We'll
                        // break loop if we'll close the connection
                        while ( !willClose() )
                        {
                            Job nextJob = queue.poll( 10, SECONDS );
                            if ( nextJob != null )
                            {
                                batch.add( nextJob );

                                break;
                            }
                            else
                            {
                                machine.validateTransaction();
                            }
                        }
                    }
                    notifyDrained( batch );

                    // execute each job that's in the batch
                    while ( batch.size() > 0 )
                    {
                        Job current = batch.remove( 0 );

                        current.perform( machine );
                    }

                    // do we have any condition that require this connection to
                    // stick to the current thread (i.e. is there an open statement
                    // or an open transaction)?
                    loop = machine.shouldStickOnThread();
                    waitForMessage = loop;
                }

                // we processed all pending messages, let's flush underlying channel
                if ( queue.size() == 0 || maxBatchSize == 1 )
                {
                    output.flush();
                }
            }
            while ( loop );

            // assert only if we'll stay alive
            if ( !willClose() )
            {
                assert !machine.hasOpenStatement();
            }
        }
        catch ( BoltConnectionAuthFatality ex )
        {
            shouldClose.set( true );
            if ( ex.isLoggable() )
            {
                userLog.warn( ex.getMessage() );
            }
        }
        catch ( BoltProtocolBreachFatality ex )
        {
            shouldClose.set( true );
            log.error( String.format( "Protocol breach detected in bolt session '%s'.", id() ), ex );
        }
        catch ( InterruptedException ex )
        {
            shouldClose.set( true );
            log.info( "Bolt session '%s' is interrupted probably due to server shutdown.", id() );
        }
        catch ( Throwable t )
        {
            shouldClose.set( true );
            userLog.error( String.format( "Unexpected error detected in bolt session '%s'.", id() ), t );
        }
        finally
        {
            if ( willClose() )
            {
                close();
            }
        }

        return !closed.get();
    }

    @Override
    public void handleSchedulingError( Throwable t )
    {
        // if the connection is closing, don't output any logs
        if ( !willClose() )
        {
            String message;
            Neo4jError error;
            if ( ExceptionUtils.hasCause( t, RejectedExecutionException.class ) )
            {
                error = Neo4jError.from( Status.Request.NoThreadsAvailable, Status.Request.NoThreadsAvailable.code().description() );
                message = String.format( "Unable to schedule bolt session '%s' for execution since there are no available threads to " +
                        "serve it at the moment. You can retry at a later time or consider increasing max thread pool size for bolt connector(s).", id() );
            }
            else
            {
                error = Neo4jError.fatalFrom( t );
                message = String.format( "Unexpected error during scheduling of bolt session '%s'.", id() );
            }

            log.error( message, t );
            userLog.error( message );
            machine.markFailed( error );
        }

        // this will ensure that the scheduled job will be executed on this thread (fork-join pool)
        // and it will either send a failure response to the client or close the connection and its
        // related resources (if closing)
        processNextBatch( 1, true );
    }

    @Override
    public void interrupt()
    {
        machine.interrupt();
    }

    @Override
    public void stop()
    {
        if ( shouldClose.compareAndSet( false, true ) )
        {
            machine.terminate();

            // Enqueue an empty job for close to be handled linearly
            // This is for already executing connections
            enqueueInternal( ignore ->
            {

            } );
        }
    }

    private boolean willClose()
    {
        return shouldClose.get();
    }

    private void close()
    {
        if ( closed.compareAndSet( false, true ) )
        {
            try
            {
                output.close();
            }
            catch ( Throwable t )
            {
                log.error( String.format( "Unable to close pack output of bolt session '%s'.", id() ), t );
            }

            try
            {
                machine.close();
            }
            catch ( Throwable t )
            {
                log.error( String.format( "Unable to close state machine of bolt session '%s'.", id() ), t );
            }
            finally
            {
                notifyDestroyed();
            }
        }
    }

    private void enqueueInternal( Job job )
    {
        queue.offer( job );
        notifyEnqueued( job );
    }

    private void notifyCreated()
    {
        if ( listener != null )
        {
            listener.created( this );
        }
    }

    private void notifyDestroyed()
    {
        if ( listener != null )
        {
            listener.closed( this );
        }
    }

    private void notifyEnqueued( Job job )
    {
        if ( queueMonitor != null )
        {
            queueMonitor.enqueued( this, job );
        }
    }

    private void notifyDrained( List<Job> jobs )
    {
        if ( queueMonitor != null && jobs.size() > 0 )
        {
            queueMonitor.drained( this, jobs );
        }
    }
}
