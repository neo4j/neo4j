/*
 * Copyright (c) 2002-2015 "Neo Technology,"
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
package org.neo4j.kernel.ha.transaction;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.Iterator;
import java.util.concurrent.Callable;
import java.util.concurrent.CancellationException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import org.neo4j.cluster.ClusterSettings;
import org.neo4j.cluster.InstanceId;
import org.neo4j.com.ComException;
import org.neo4j.helpers.NamedThreadFactory;
import org.neo4j.helpers.Predicate;
import org.neo4j.helpers.collection.FilteringIterator;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.ha.HaSettings;
import org.neo4j.kernel.ha.com.master.Slave;
import org.neo4j.kernel.ha.com.master.SlavePriorities;
import org.neo4j.kernel.ha.com.master.SlavePriority;
import org.neo4j.kernel.ha.com.master.Slaves;
import org.neo4j.kernel.impl.util.CappedOperation;
import org.neo4j.kernel.impl.util.StringLogger;
import org.neo4j.kernel.lifecycle.Lifecycle;

public class TransactionPropagator implements Lifecycle
{
    public interface Configuration
    {
        int getTxPushFactor();

        InstanceId getServerId();

        SlavePriority getReplicationStrategy();
    }

    private static class ReplicationContext
    {
        final Future<Void> future;
        final Slave slave;

        Throwable throwable;

        ReplicationContext( Future<Void> future, Slave slave )
        {
            this.future = future;
            this.slave = slave;
        }
    }

    public static Configuration from( final Config config )
    {
        return new Configuration()
        {
            @Override
            public int getTxPushFactor()
            {
                return config.get( HaSettings.tx_push_factor );
            }

            @Override
            public InstanceId getServerId()
            {
                return config.get( ClusterSettings.server_id );
            }

            @Override
            public SlavePriority getReplicationStrategy()
            {
                switch ( config.get( HaSettings.tx_push_strategy ) )
                {
                    case fixed:
                        return SlavePriorities.fixed();

                    case round_robin:
                        return SlavePriorities.roundRobin();

                    default:
                        throw new RuntimeException( "Unknown replication strategy " );
                }
            }
        };
    }

    public static Configuration from( final Config config, final SlavePriority slavePriority )
    {
        return new Configuration()
        {
            @Override
            public int getTxPushFactor()
            {
                return config.get( HaSettings.tx_push_factor );
            }

            @Override
            public InstanceId getServerId()
            {
                return config.get( ClusterSettings.server_id );
            }

            @Override
            public SlavePriority getReplicationStrategy()
            {
                return slavePriority;
            }
        };
    }

    private int desiredReplicationFactor;
    private SlavePriority replicationStrategy;
    private ExecutorService slaveCommitters;
    private final StringLogger log;
    private final Configuration config;
    private final Slaves slaves;
    private final CommitPusher pusher;
    private final CappedOperation<ReplicationContext> slaveCommitFailureLogger =
            new CappedOperation<ReplicationContext>(
                    CappedOperation.time( 5, TimeUnit.SECONDS ),
                    CappedOperation.differentItemClasses() )
            {
                @Override
                protected void triggered( ReplicationContext context )
                {
                    log.error( "Slave " + context.slave.getServerId() + ": Replication commit threw" +
                               (context.throwable instanceof ComException ? " communication" : "") +
                               " exception:", context.throwable );
                }
            };

    public TransactionPropagator( Configuration config, StringLogger log, Slaves slaves, CommitPusher pusher )
    {
        this.config = config;
        this.log = log;
        this.slaves = slaves;
        this.pusher = pusher;
    }

    @Override
    public void init() throws Throwable
    {
    }

    @Override
    public void start() throws Throwable
    {
        this.slaveCommitters = Executors.newCachedThreadPool( new NamedThreadFactory( "slave-committer" ) );
        desiredReplicationFactor = config.getTxPushFactor();
        replicationStrategy = config.getReplicationStrategy();
    }

    @Override
    public void stop() throws Throwable
    {
        this.slaveCommitters.shutdown();
    }

    @Override
    public void shutdown() throws Throwable
    {
    }

    public void committed( long txId, int authorId )
    {
        int replicationFactor = desiredReplicationFactor;
        // If the author is not this instance, then we need to push to one less - the committer already has it
        boolean isAuthoredBySlave = config.getServerId().toIntegerIndex() != authorId;
        if ( isAuthoredBySlave )
        {
            replicationFactor--;
        }

        if ( replicationFactor == 0 )
        {
            return;
        }
        Collection<ReplicationContext> committers = new HashSet<>();
        try
        {
            // TODO: Move this logic into {@link CommitPusher}
            // Commit at the configured amount of slaves in parallel.
            int successfulReplications = 0;
            Iterator<Slave> slaveList = filter( replicationStrategy.prioritize( slaves.getSlaves() ).iterator(),
                    authorId );
            CompletionNotifier notifier = new CompletionNotifier();

            // Start as many initial committers as needed
            for ( int i = 0; i < replicationFactor && slaveList.hasNext(); i++ )
            {
                Slave slave = slaveList.next();
                Callable<Void> slaveCommitter = slaveCommitter( slave, txId, notifier );
                committers.add( new ReplicationContext( slaveCommitters.submit( slaveCommitter ), slave ) );
            }

            // Wait for them and perhaps spawn new ones for failing committers until we're done
            // or until we have no more slaves to try out.
            Collection<ReplicationContext> toAdd = new ArrayList<>();
            Collection<ReplicationContext> toRemove = new ArrayList<>();
            while ( !committers.isEmpty() && successfulReplications < replicationFactor )
            {
                toAdd.clear();
                toRemove.clear();
                for ( ReplicationContext context : committers )
                {
                    if ( !context.future.isDone() )
                    {
                        continue;
                    }

                    if ( isSuccessful( context ) )
                    // This committer was successful, increment counter
                    {
                        successfulReplications++;
                    }
                    else if ( slaveList.hasNext() )
                    // This committer failed, spawn another one
                    {
                        Slave newSlave = slaveList.next();
                        Callable<Void> slaveCommitter = slaveCommitter( newSlave, txId, notifier );
                        toAdd.add( new ReplicationContext( slaveCommitters.submit( slaveCommitter ), newSlave ) );
                    }
                    toRemove.add( context );
                }

                // Incorporate the results into committers collection
                if ( !toAdd.isEmpty() )
                {
                    committers.addAll( toAdd );
                }
                if ( !toRemove.isEmpty() )
                {
                    committers.removeAll( toRemove );
                }

                if ( !committers.isEmpty() )
                // There are committers doing work right now, so go and wait for
                // any of the committers to be done so that we can reevaluate
                // the situation again.
                {
                    notifier.waitForAnyCompletion();
                }
            }

            // We did the best we could, have we committed successfully on enough slaves?
            if ( !(successfulReplications >= replicationFactor) )
            {
                log.logMessage( "Transaction " + txId + " couldn't commit on enough slaves, desired " +
                                replicationFactor + ", but could only commit at " + successfulReplications );
            }
        }
        catch ( Throwable t )
        {
            log.logMessage( "Unknown error commit master transaction at slave", t );
        }
        finally
        {
            // Cancel all ongoing committers in the executor
            for ( ReplicationContext context : committers )
            {
                context.future.cancel( false );
            }
        }
    }

    private Iterator<Slave> filter( Iterator<Slave> slaves, final Integer externalAuthorServerId )
    {
        return externalAuthorServerId == null ? slaves : new FilteringIterator<>( slaves, new Predicate<Slave>()
        {
            @Override
            public boolean accept( Slave item )
            {
                return item.getServerId() != externalAuthorServerId;
            }
        } );
    }

    private boolean isSuccessful( ReplicationContext context )
    {
        try
        {
            context.future.get();
            return true;
        }
        catch ( InterruptedException e )
        {
            return false;
        }
        catch ( ExecutionException e )
        {
            context.throwable = e.getCause();
            slaveCommitFailureLogger.event( context );
            return false;
        }
        catch ( CancellationException e )
        {
            return false;
        }
    }

    /**
     * A version of wait/notify which can handle that a notify comes before the
     * call to wait, in which case the call to wait will return immediately.
     *
     * @author Mattias Persson
     */
    private static class CompletionNotifier
    {
        private boolean notified;

        synchronized void completed()
        {
            notified = true;
            notifyAll();
        }

        synchronized void waitForAnyCompletion()
        {
            if ( !notified )
            {
                notified = false;
                try
                {
                    wait( 2000 /*wait timeout just for safety*/ );
                }
                catch ( InterruptedException e )
                {
                    Thread.interrupted();
                    // Hmm, ok we got interrupted. No biggy I'd guess
                }
            }
            else
            {
                notified = false;
            }
        }

        @Override
        public String toString()
        {
            return "CompletionNotifier{id=" + System.identityHashCode( this ) + ",notified=" + notified + "}";
        }
    }

    private Callable<Void> slaveCommitter( final Slave slave, final long txId, final CompletionNotifier notifier )
    {
        return new Callable<Void>()
        {
            @Override
            public Void call()
            {
                try
                {
                    // TODO Bypass the CommitPusher, now that we have a single thread pulling updates on each slave
                    // The CommitPusher is all about batching transaction pushing to slaves, to reduce the overhead
                    // of multiple threads pulling the same transactions on each slave. That should be fine now.
//                    slave.pullUpdates( txId );
                    pusher.queuePush( slave, txId );

                    return null;
                }
                finally
                {
                    notifier.completed();
                }
            }
        };
    }
}
