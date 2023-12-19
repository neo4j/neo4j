/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 *
 * This file is part of Neo4j Enterprise Edition. The included source
 * code can be redistributed and/or modified under the terms of the
 * Neo4j Sweden Software License, as found in the associated LICENSE.txt
 * file.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * Neo4j Sweden Software License for more details.
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

import org.neo4j.cluster.ClusterSettings;
import org.neo4j.cluster.InstanceId;
import org.neo4j.com.ComException;
import org.neo4j.helpers.NamedThreadFactory;
import org.neo4j.helpers.collection.FilteringIterator;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.ha.HaSettings;
import org.neo4j.kernel.ha.com.master.Slave;
import org.neo4j.kernel.ha.com.master.SlavePriorities;
import org.neo4j.kernel.ha.com.master.SlavePriority;
import org.neo4j.kernel.ha.com.master.Slaves;
import org.neo4j.kernel.impl.util.CappedLogger;
import org.neo4j.kernel.lifecycle.Lifecycle;
import org.neo4j.logging.Log;
import org.neo4j.time.Clocks;

import static java.util.concurrent.TimeUnit.SECONDS;

/**
 * Pushes transactions committed on master to one or more slaves. Number of slaves receiving each transactions
 * is controlled by {@link HaSettings#tx_push_factor}. Which slaves receives transactions is controlled by
 * {@link HaSettings#tx_push_strategy}.
 *
 * An attempt is made to push each transaction to the wanted number of slaves, but if it isn't possible
 * and a timeout is hit, propagation will still be considered as successful and occurrence will be logged.
 */
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
                    case fixed_descending:
                        return SlavePriorities.fixedDescending();

                    case fixed_ascending:
                        return SlavePriorities.fixedAscending();

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
    private final Log log;
    private final Configuration config;
    private final Slaves slaves;
    private final CommitPusher pusher;
    private final CappedLogger slaveCommitFailureLogger;
    private final CappedLogger pushedToTooFewSlaveLogger;

    public TransactionPropagator( Configuration config, Log log, Slaves slaves, CommitPusher pusher )
    {
        this.config = config;
        this.log = log;
        this.slaves = slaves;
        this.pusher = pusher;
        slaveCommitFailureLogger = new CappedLogger( log ).setTimeLimit( 5, SECONDS, Clocks.systemClock() );
        pushedToTooFewSlaveLogger = new CappedLogger( log ).setTimeLimit( 5, SECONDS, Clocks.systemClock() );
    }

    @Override
    public void init()
    {
    }

    @Override
    public void start()
    {
        this.slaveCommitters = Executors.newCachedThreadPool( new NamedThreadFactory( "slave-committer" ) );
        desiredReplicationFactor = config.getTxPushFactor();
        replicationStrategy = config.getReplicationStrategy();
    }

    @Override
    public void stop()
    {
        this.slaveCommitters.shutdown();
    }

    @Override
    public void shutdown()
    {
    }

    /**
     *
     * @param txId transaction id to replicate
     * @param authorId author id for such transaction id
     * @return the number of missed replicas (e.g., desired replication factor - number of successful replications)
     */
    public int committed( long txId, int authorId )
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
            return replicationFactor;
        }
        Collection<ReplicationContext> committers = new HashSet<>();

        try
        {
            // TODO: Move this logic into {@link CommitPusher}
            // Commit at the configured amount of slaves in parallel.
            int successfulReplications = 0;
            Iterator<Slave> slaveList =
                    filter( replicationStrategy.prioritize( slaves.getSlaves() ).iterator(), authorId );
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
                        Callable<Void> slaveCommitter;
                        try
                        {
                            slaveCommitter = slaveCommitter( newSlave, txId, notifier );
                        }
                        catch ( Throwable t )
                        {
                            log.error( "Unknown error commit master transaction at slave", t );
                            return desiredReplicationFactor /* missed them all :( */;
                        }

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
            if ( successfulReplications < replicationFactor )
            {
                pushedToTooFewSlaveLogger
                        .info( "Transaction " + txId + " couldn't commit on enough slaves, desired " +
                               replicationFactor +
                               ", but could only commit at " + successfulReplications );
            }

            return replicationFactor - successfulReplications;
        }
        finally
        {
            // Cancel all ongoing committers in the executor
            for ( ReplicationContext committer : committers )
            {
                committer.future.cancel( false );
            }
        }
    }

    private Iterator<Slave> filter( Iterator<Slave> slaves, final Integer externalAuthorServerId )
    {
        return externalAuthorServerId == null ? slaves : new FilteringIterator<>( slaves,
                item -> item.getServerId() != externalAuthorServerId );
    }

    private boolean isSuccessful( ReplicationContext context )
    {
        try
        {
            context.future.get();
            return true;
        }
        catch ( InterruptedException | CancellationException e )
        {
            return false;
        }
        catch ( ExecutionException e )
        {
            context.throwable = e.getCause();
            slaveCommitFailureLogger.error( "Slave " + context.slave.getServerId() + ": Replication commit threw" +
                                            (context.throwable instanceof ComException ? " communication" : "") +
                                            " exception:", context.throwable );
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
        return () ->
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
        };
    }
}
