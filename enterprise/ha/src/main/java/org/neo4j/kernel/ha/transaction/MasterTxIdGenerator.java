/**
 * Copyright (c) 2002-2013 "Neo Technology,"
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
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Callable;
import java.util.concurrent.CancellationException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.FutureTask;
import java.util.concurrent.TimeUnit;

import javax.transaction.xa.XAException;

import org.neo4j.cluster.ClusterSettings;
import org.neo4j.com.ComException;
import org.neo4j.com.Response;
import org.neo4j.helpers.NamedThreadFactory;
import org.neo4j.helpers.Predicate;
import org.neo4j.helpers.collection.FilteringIterator;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.ha.HaSettings;
import org.neo4j.kernel.ha.com.master.Slave;
import org.neo4j.kernel.ha.com.master.SlavePriorities;
import org.neo4j.kernel.ha.com.master.SlavePriority;
import org.neo4j.kernel.ha.com.master.Slaves;
import org.neo4j.kernel.impl.transaction.xaframework.TxIdGenerator;
import org.neo4j.kernel.impl.transaction.xaframework.XaDataSource;
import org.neo4j.kernel.impl.util.StringLogger;
import org.neo4j.kernel.lifecycle.Lifecycle;

public class MasterTxIdGenerator implements TxIdGenerator, Lifecycle
{
    public interface Configuration
    {
        int getTxPushFactor();

        int getServerId();

        SlavePriority getReplicationStrategy();
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
            public int getServerId()
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
            public int getServerId()
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

    public MasterTxIdGenerator( Configuration config, StringLogger log, Slaves slaves )
    {
        this.config = config;
        this.log = log;
        this.slaves = slaves;
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

        for ( ExecutorService pullUpdateWorker : pullUpdateWorkers )
        {
            pullUpdateWorker.shutdownNow();
            pullUpdateWorker.awaitTermination( 30, TimeUnit.SECONDS );
        }
    }

    @Override
    public void shutdown() throws Throwable
    {
    }

    public long generate( final XaDataSource dataSource, final int identifier ) throws XAException
    {
        return TxIdGenerator.DEFAULT.generate( dataSource, identifier );
    }

    @Override
    public void committed( XaDataSource dataSource, int identifier, long txId, Integer externalAuthorServerId )
    {
        int replicationFactor = desiredReplicationFactor;
        if ( externalAuthorServerId != null )
        {
            replicationFactor--;
        }

        if ( replicationFactor == 0 )
        {
            return;
        }
        Collection<Future<Void>> committers = new HashSet<Future<Void>>();
        try
        {
            // Commit at the configured amount of slaves in parallel.
            int successfulReplications = 0;
            Iterator<Slave> slaveList = filter( replicationStrategy.prioritize( slaves.getSlaves() ).iterator(),
                    externalAuthorServerId );
            CompletionNotifier notifier = new CompletionNotifier();

            // Start as many initial committers as needed
            for ( int i = 0; i < replicationFactor && slaveList.hasNext(); i++ )
            {
                committers.add( slaveCommitters.submit( slaveCommitter( dataSource, slaveList.next(),
                        txId, notifier ) ) );
            }

            // Wait for them and perhaps spawn new ones for failing committers until we're done
            // or until we have no more slaves to try out.
            Collection<Future<Void>> toAdd = new ArrayList<Future<Void>>();
            Collection<Future<Void>> toRemove = new ArrayList<Future<Void>>();
            while ( !committers.isEmpty() && successfulReplications < replicationFactor )
            {
                toAdd.clear();
                toRemove.clear();
                for ( Future<Void> committer : committers )
                {
                    if ( !committer.isDone() )
                    {
                        continue;
                    }

                    if ( isSuccessful( committer ) )
                    // This committer was successful, increment counter
                    {
                        successfulReplications++;
                    }
                    else if ( slaveList.hasNext() )
                    // This committer failed, spawn another one
                    {
                        toAdd.add( slaveCommitters.submit( slaveCommitter( dataSource, slaveList.next(),
                                txId, notifier ) ) );
                    }
                    toRemove.add( committer );
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
                log.logMessage( "Transaction " + txId + " for " + dataSource.getName()
                        + " couldn't commit on enough slaves, desired " + replicationFactor
                        + ", but could only commit at " + successfulReplications );
            }
        }
        catch ( Throwable t )
        {
            log.logMessage( "Unknown error commit master transaction at slave", t );
        }
        finally
        {
            // Cancel all ongoing committers in the executor
            for ( Future<Void> committer : committers )
            {
                committer.cancel( false );
            }
        }
    }

    private Iterator<Slave> filter( Iterator<Slave> slaves, final Integer externalAuthorServerId )
    {
        return externalAuthorServerId == null ? slaves : new FilteringIterator<Slave>( slaves, new Predicate<Slave>()
        {
            @Override
            public boolean accept( Slave item )
            {
                return item.getServerId() != externalAuthorServerId.intValue();
            }
        } );
    }

    private boolean isSuccessful( Future<Void> committer )
    {
        try
        {
            committer.get();
            return true;
        }
        catch ( InterruptedException e )
        {
            return false;
        }
        catch ( ExecutionException e )
        {
            log.error( "Slave commit threw " + (e.getCause() instanceof ComException ? "communication" : "" )
                    + " exception", e );
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
    }

    private Callable<Void> slaveCommitter( final XaDataSource dataSource,
                                           final Slave slave, final long txId, final CompletionNotifier notifier )
    {
        return new Callable<Void>()
        {
            @Override
            public Void call()
            {
                try
                {
                    commitAtSlave( dataSource, slave, txId );
                    return null;
                }
                finally
                {
                    notifier.completed();
                }
            }
        };
    }

    private Map<Integer, BlockingQueue<PullUpdateFuture>> pullUpdateQueues = new HashMap<Integer, BlockingQueue<PullUpdateFuture>>(  );
    private List<ExecutorService> pullUpdateWorkers = new ArrayList<ExecutorService>(  );

    private void commitAtSlave( final XaDataSource dataSource, Slave slave, final long txId )
    {
        PullUpdateFuture pullRequest = new PullUpdateFuture(slave, txId);

        synchronized ( pullUpdateQueues )
        {
            BlockingQueue<PullUpdateFuture> queue = pullUpdateQueues.get( slave.getServerId() );
            if (queue == null)
            {
                // Create queue and worker
                queue = new ArrayBlockingQueue<PullUpdateFuture>( 100 );
                pullUpdateQueues.put(slave.getServerId(), queue);

                final ExecutorService executorService = Executors.newSingleThreadExecutor(new NamedThreadFactory( "pull-worker" ));
                pullUpdateWorkers.add( executorService );
                final BlockingQueue<PullUpdateFuture> finalQueue = queue;
                executorService.submit( new Runnable()
                {
                    List<PullUpdateFuture> currentPulls = new ArrayList<PullUpdateFuture>();

                    @Override
                    public void run()
                    {
                        try
                        {
                            while (true)
                            {
                                // Poll queue and call pullUpdate
                                currentPulls.clear();
                                currentPulls.add( finalQueue.take() );

                                PullUpdateFuture pullRequest;
                                while ( (pullRequest = finalQueue.poll()) != null )
                                {
                                    currentPulls.add( pullRequest );
                                }

                                try
                                {
                                    PullUpdateFuture pullUpdateFuture = currentPulls.get( 0 );
                                    Response<Void> response = pullUpdateFuture.getSlave().pullUpdates( dataSource.getName(), pullUpdateFuture.getTxId() );
                                    response.close();

                                    // Notify the futures
                                    for ( PullUpdateFuture currentPull : currentPulls )
                                    {
                                        currentPull.done();
                                    }
                                }
                                catch ( Exception e )
                                {
                                    // Notify the futures
                                    for ( PullUpdateFuture currentPull : currentPulls )
                                    {
                                        currentPull.setException( e );
                                    }
                                }
                            }
                        }
                        catch ( InterruptedException e )
                        {
                            // Quit
                        }
                    }
                } );
            }

            queue.offer( pullRequest );
        }

        // Wait for pull request to finish
        try
        {
            pullRequest.get();
        }
        catch ( InterruptedException e )
        {
            throw new RuntimeException( e );
        }
        catch ( ExecutionException e )
        {
            if (e.getCause() instanceof RuntimeException)
                throw ((RuntimeException)e.getCause());
            else
                throw new RuntimeException( e.getCause() );
        }
    }

    public int getCurrentMasterId()
    {
        return config.getServerId();
    }

    @Override
    public int getMyId()
    {
        return config.getServerId();
    }

    private static class PullUpdateFuture
        extends FutureTask<Object>
    {
        private Slave slave;
        private long txId;

        public PullUpdateFuture( Slave slave, long txId )
        {
            super( new Callable<Object>()
            {
                @Override
                public Object call() throws Exception
                {
                    return null;
                }
            });
            this.slave = slave;
            this.txId = txId;
        }

        @Override
        public void done()
        {
            super.set( null );
            super.done();
        }

        @Override
        public void setException( Throwable t )
        {
            super.setException( t );
        }

        public Slave getSlave()
        {
            return slave;
        }

        private long getTxId()
        {
            return txId;
        }
    }
}
