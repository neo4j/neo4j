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
package org.neo4j.causalclustering.catchup.tx;

import java.io.IOException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Future;
import java.util.function.Supplier;

import org.neo4j.causalclustering.catchup.CatchUpClient;
import org.neo4j.causalclustering.catchup.CatchUpClientException;
import org.neo4j.causalclustering.catchup.CatchUpResponseAdaptor;
import org.neo4j.causalclustering.catchup.CatchupAddressProvider.SingleAddressProvider;
import org.neo4j.causalclustering.catchup.storecopy.DatabaseShutdownException;
import org.neo4j.causalclustering.catchup.storecopy.LocalDatabase;
import org.neo4j.causalclustering.catchup.storecopy.StoreCopyFailedException;
import org.neo4j.causalclustering.catchup.storecopy.StoreCopyProcess;
import org.neo4j.causalclustering.core.consensus.schedule.Timer;
import org.neo4j.causalclustering.core.consensus.schedule.TimerService;
import org.neo4j.causalclustering.core.consensus.schedule.TimerService.TimerName;
import org.neo4j.causalclustering.core.state.snapshot.TopologyLookupException;
import org.neo4j.causalclustering.discovery.TopologyService;
import org.neo4j.causalclustering.helper.Suspendable;
import org.neo4j.causalclustering.identity.MemberId;
import org.neo4j.causalclustering.identity.StoreId;
import org.neo4j.causalclustering.upstream.UpstreamDatabaseSelectionException;
import org.neo4j.causalclustering.upstream.UpstreamDatabaseStrategySelector;
import org.neo4j.helpers.AdvertisedSocketAddress;
import org.neo4j.kernel.impl.transaction.CommittedTransactionRepresentation;
import org.neo4j.kernel.internal.DatabaseHealth;
import org.neo4j.kernel.lifecycle.LifecycleAdapter;
import org.neo4j.kernel.monitoring.Monitors;
import org.neo4j.logging.Log;
import org.neo4j.logging.LogProvider;
import org.neo4j.scheduler.JobScheduler.Groups;

import static java.lang.String.format;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static org.neo4j.causalclustering.catchup.tx.CatchupPollingProcess.State.CANCELLED;
import static org.neo4j.causalclustering.catchup.tx.CatchupPollingProcess.State.PANIC;
import static org.neo4j.causalclustering.catchup.tx.CatchupPollingProcess.State.STORE_COPYING;
import static org.neo4j.causalclustering.catchup.tx.CatchupPollingProcess.State.TX_PULLING;
import static org.neo4j.causalclustering.catchup.tx.CatchupPollingProcess.Timers.TX_PULLER_TIMER;
import static org.neo4j.causalclustering.core.consensus.schedule.TimeoutFactory.fixedTimeout;
import static org.neo4j.causalclustering.core.consensus.schedule.Timer.CancelMode.SYNC_WAIT;

/**
 * This class is responsible for pulling transactions from a core server and queuing
 * them to be applied with the {@link BatchingTxApplier}. Pull requests are issued on
 * a fixed interval.
 * <p>
 * If the necessary transactions are not remotely available then a fresh copy of the
 * entire store will be pulled down.
 */
public class CatchupPollingProcess extends LifecycleAdapter
{
    enum Timers implements TimerName
    {
        TX_PULLER_TIMER
    }

    enum State
    {
        TX_PULLING,
        STORE_COPYING,
        PANIC,
        CANCELLED
    }

    private final LocalDatabase localDatabase;
    private final Log log;
    private final Suspendable enableDisableOnStoreCopy;
    private final StoreCopyProcess storeCopyProcess;
    private final Supplier<DatabaseHealth> databaseHealthSupplier;
    private final CatchUpClient catchUpClient;
    private final UpstreamDatabaseStrategySelector selectionStrategy;
    private final TimerService timerService;
    private final long txPullIntervalMillis;
    private final BatchingTxApplier applier;
    private final PullRequestMonitor pullRequestMonitor;
    private final TopologyService topologyService;

    private Timer timer;
    private volatile State state = TX_PULLING;
    private DatabaseHealth dbHealth;
    private CompletableFuture<Boolean> upToDateFuture; // we are up-to-date when we are successfully pulling
    private volatile long latestTxIdOfUpStream;

    public CatchupPollingProcess( LogProvider logProvider, LocalDatabase localDatabase, Suspendable enableDisableOnSoreCopy, CatchUpClient catchUpClient,
                                  UpstreamDatabaseStrategySelector selectionStrategy, TimerService timerService, long txPullIntervalMillis,
                                  BatchingTxApplier applier, Monitors monitors, StoreCopyProcess storeCopyProcess,
                                  Supplier<DatabaseHealth> databaseHealthSupplier, TopologyService topologyService )

    {
        this.localDatabase = localDatabase;
        this.log = logProvider.getLog( getClass() );
        this.enableDisableOnStoreCopy = enableDisableOnSoreCopy;
        this.catchUpClient = catchUpClient;
        this.selectionStrategy = selectionStrategy;
        this.timerService = timerService;
        this.txPullIntervalMillis = txPullIntervalMillis;
        this.applier = applier;
        this.pullRequestMonitor = monitors.newMonitor( PullRequestMonitor.class );
        this.storeCopyProcess = storeCopyProcess;
        this.databaseHealthSupplier = databaseHealthSupplier;
        this.topologyService = topologyService;
    }

    @Override
    public synchronized void start()
    {
        state = TX_PULLING;
        timer = timerService.create( TX_PULLER_TIMER, Groups.pullUpdates, timeout -> onTimeout() );
        timer.set( fixedTimeout( txPullIntervalMillis, MILLISECONDS ) );
        dbHealth = databaseHealthSupplier.get();
        upToDateFuture = new CompletableFuture<>();
    }

    public Future<Boolean> upToDateFuture()
    {
        return upToDateFuture;
    }

    @Override
    public void stop()
    {
        state = CANCELLED;
        timer.cancel( SYNC_WAIT );
    }

    public State state()
    {
        return state;
    }

    /**
     * Time to catchup!
     */
    private void onTimeout()
    {
        try
        {
            switch ( state )
            {
            case TX_PULLING:
                pullTransactions();
                break;

            case STORE_COPYING:
                copyStore();
                break;

            default:
                throw new IllegalStateException( "Tried to execute catchup but was in state " + state );
            }
        }
        catch ( Throwable e )
        {
            panic( e );
        }

        if ( state != PANIC && state != CANCELLED )
        {
            timer.reset();
        }
    }

    private synchronized void panic( Throwable e )
    {
        log.error( "Unexpected issue in catchup process. No more catchup requests will be scheduled.", e );
        dbHealth.panic( e );
        upToDateFuture.completeExceptionally( e );
        state = PANIC;
    }

    private void pullTransactions()
    {
        MemberId upstream;
        try
        {
            upstream = selectionStrategy.bestUpstreamDatabase();
        }
        catch ( UpstreamDatabaseSelectionException e )
        {
            log.warn( "Could not find upstream database from which to pull.", e );
            return;
        }

        StoreId localStoreId = localDatabase.storeId();

        boolean moreToPull = true;
        int batchCount = 1;
        while ( moreToPull )
        {
            moreToPull = pullAndApplyBatchOfTransactions( upstream, localStoreId, batchCount );
            batchCount++;
        }
    }

    private synchronized void handleTransaction( CommittedTransactionRepresentation tx )
    {
        if ( state == PANIC )
        {
            return;
        }

        try
        {
            applier.queue( tx );
        }
        catch ( Throwable e )
        {
            panic( e );
        }
    }

    private synchronized void streamComplete()
    {
        if ( state == PANIC )
        {
            return;
        }

        try
        {
            applier.applyBatch();
        }
        catch ( Throwable e )
        {
            panic( e );
        }
    }

    private boolean pullAndApplyBatchOfTransactions( MemberId upstream, StoreId localStoreId, int batchCount )
    {
        long lastQueuedTxId = applier.lastQueuedTxId();
        pullRequestMonitor.txPullRequest( lastQueuedTxId );
        TxPullRequest txPullRequest = new TxPullRequest( lastQueuedTxId, localStoreId );
        log.debug( "Pull transactions from %s where tx id > %d [batch #%d]", upstream, lastQueuedTxId, batchCount );

        TxStreamFinishedResponse response;
        try
        {
            AdvertisedSocketAddress fromAddress = topologyService.findCatchupAddress( upstream ).orElseThrow( () -> new TopologyLookupException( upstream ) );
            response = catchUpClient.makeBlockingRequest( fromAddress, txPullRequest, new CatchUpResponseAdaptor<TxStreamFinishedResponse>()
            {
                @Override
                public void onTxPullResponse( CompletableFuture<TxStreamFinishedResponse> signal, TxPullResponse response )
                {
                    handleTransaction( response.tx() );
                }

                @Override
                public void onTxStreamFinishedResponse( CompletableFuture<TxStreamFinishedResponse> signal, TxStreamFinishedResponse response )
                {
                    streamComplete();
                    signal.complete( response );
                }
            } );
        }
        catch ( CatchUpClientException | TopologyLookupException e )
        {
            log.warn( "Exception occurred while pulling transactions. Will retry shortly.", e );
            streamComplete();
            return false;
        }

        latestTxIdOfUpStream = response.latestTxId();

        switch ( response.status() )
        {
        case SUCCESS_END_OF_BATCH:
            return true;
        case SUCCESS_END_OF_STREAM:
            log.debug( "Successfully pulled transactions from tx id %d", lastQueuedTxId );
            upToDateFuture.complete( Boolean.TRUE );
            return false;
        case E_TRANSACTION_PRUNED:
            log.info( "Tx pull unable to get transactions starting from %d since transactions have been pruned. Attempting a store copy.", lastQueuedTxId );
            state = STORE_COPYING;
            return false;
        default:
            log.info( "Tx pull request unable to get transactions > %d " + lastQueuedTxId );
            return false;
        }
    }

    private void copyStore()
    {
        StoreId localStoreId = localDatabase.storeId();
        downloadDatabase( localStoreId );
    }

    private void downloadDatabase( StoreId localStoreId )
    {
        try
        {
            localDatabase.stopForStoreCopy();
            enableDisableOnStoreCopy.disable();
        }
        catch ( Throwable throwable )
        {
            throw new RuntimeException( throwable );
        }

        try
        {
            MemberId source = selectionStrategy.bestUpstreamDatabase();
            AdvertisedSocketAddress fromAddress = topologyService.findCatchupAddress( source ).orElseThrow( () -> new TopologyLookupException( source ) );
            storeCopyProcess.replaceWithStoreFrom( new SingleAddressProvider( fromAddress ), localStoreId );
        }
        catch ( IOException | StoreCopyFailedException | UpstreamDatabaseSelectionException | TopologyLookupException e )
        {
            log.warn( "Error copying store. Will retry shortly.", e );
            return;
        }
        catch ( DatabaseShutdownException e )
        {
            log.warn( "Store copy aborted due to shutdown.", e );
            return;
        }

        try
        {
            localDatabase.start();
            enableDisableOnStoreCopy.enable();
        }
        catch ( Throwable throwable )
        {
            throw new RuntimeException( throwable );
        }

        latestTxIdOfUpStream = 0; // we will find out on the next pull request response
        state = TX_PULLING;
        applier.refreshFromNewStore();
    }

    public String describeState()
    {
        if ( state == TX_PULLING && applier.lastQueuedTxId() > 0 && latestTxIdOfUpStream > 0 )
        {
            return format( "%s (%d of %d)", TX_PULLING.name(), applier.lastQueuedTxId(), latestTxIdOfUpStream );
        }
        else
        {
            return state.name();
        }
    }
}
