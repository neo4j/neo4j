/*
 * Copyright (c) 2002-2016 "Neo Technology,"
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
package org.neo4j.causalclustering.catchup.tx;

import java.util.concurrent.CompletableFuture;

import org.neo4j.causalclustering.catchup.CatchUpClient;
import org.neo4j.causalclustering.catchup.CatchUpResponseAdaptor;
import org.neo4j.causalclustering.catchup.CatchupResult;
import org.neo4j.causalclustering.catchup.storecopy.CopiedStoreRecovery;
import org.neo4j.causalclustering.catchup.storecopy.LocalDatabase;
import org.neo4j.causalclustering.catchup.storecopy.StoreFetcher;
import org.neo4j.causalclustering.core.consensus.schedule.RenewableTimeoutService;
import org.neo4j.causalclustering.core.consensus.schedule.RenewableTimeoutService.RenewableTimeout;
import org.neo4j.causalclustering.core.consensus.schedule.RenewableTimeoutService.TimeoutName;
import org.neo4j.causalclustering.readreplica.CopyStoreSafely;
import org.neo4j.causalclustering.identity.MemberId;
import org.neo4j.causalclustering.identity.StoreId;
import org.neo4j.causalclustering.messaging.routing.CoreMemberSelectionStrategy;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.kernel.lifecycle.LifecycleAdapter;
import org.neo4j.kernel.monitoring.Monitors;
import org.neo4j.logging.Log;
import org.neo4j.logging.LogProvider;

import static org.neo4j.causalclustering.catchup.tx.TxPollingClient.Timeouts.TX_PULLER_TIMEOUT;

/**
 * This class is responsible for pulling transactions from a core server and queuing
 * them to be applied with the {@link BatchingTxApplier}.
 *
 * Pull requests are issued on a fixed interval, but skipped if the {@link BatchingTxApplier}
 * isn't yet finished with the current work.
 */
public class TxPollingClient extends LifecycleAdapter
{

    enum Timeouts implements TimeoutName
    {
        TX_PULLER_TIMEOUT
    }

    private final FileSystemAbstraction fs;
    private final LocalDatabase localDatabase;
    private final Log log;
    private final StoreFetcher storeFetcher;
    private final CopiedStoreRecovery copiedStoreRecovery;
    private final CatchUpClient catchUpClient;
    private final CoreMemberSelectionStrategy connectionStrategy;
    private final RenewableTimeoutService timeoutService;
    private final long txPullIntervalMillis;
    private final BatchingTxApplier applier;
    private final PullRequestMonitor pullRequestMonitor;

    private RenewableTimeout timeout;

    public TxPollingClient( LogProvider logProvider, FileSystemAbstraction fs, LocalDatabase localDatabase,
            StoreFetcher storeFetcher, CatchUpClient catchUpClient, CoreMemberSelectionStrategy connectionStrategy,
            RenewableTimeoutService timeoutService, long txPullIntervalMillis, BatchingTxApplier applier,
            Monitors monitors, CopiedStoreRecovery copiedStoreRecovery )
    {
        this.fs = fs;
        this.localDatabase = localDatabase;
        this.log = logProvider.getLog( getClass() );
        this.storeFetcher = storeFetcher;
        this.catchUpClient = catchUpClient;
        this.connectionStrategy = connectionStrategy;
        this.timeoutService = timeoutService;
        this.txPullIntervalMillis = txPullIntervalMillis;
        this.applier = applier;
        this.pullRequestMonitor = monitors.newMonitor( PullRequestMonitor.class );
        this.copiedStoreRecovery = copiedStoreRecovery;
    }

    @Override
    public synchronized void start() throws Throwable
    {
        timeout = timeoutService.create( TX_PULLER_TIMEOUT, txPullIntervalMillis, 0, timeout -> onTimeout() );
    }

    /**
     * Time to pull!
     */
    private synchronized void onTimeout()
    {
        timeout.renew();
        if ( applier.workPending() )
        {
            log.info( "Still applying old batch, delay pulling to the next interval. Up to tx %d",
                    applier.lastAppliedTxId() );
            return;
        }

        try
        {
            MemberId core = connectionStrategy.coreMember();
            long lastAppliedTxId = applier.lastAppliedTxId();
            pullRequestMonitor.txPullRequest( lastAppliedTxId );
            StoreId localStoreId = localDatabase.storeId();
            TxPullRequest txPullRequest = new TxPullRequest( lastAppliedTxId, localStoreId );
            log.info( "[" + Thread.currentThread() + "] Starting transaction pull from " + lastAppliedTxId );
            CatchupResult catchupResult =
                    catchUpClient.makeBlockingRequest( core, txPullRequest, new CatchUpResponseAdaptor<CatchupResult>()
                    {
                        @Override
                        public void onTxPullResponse( CompletableFuture<CatchupResult> signal, TxPullResponse response )
                        {
                            applier.queue( response.tx() );
                            timeout.renew();
                        }

                        @Override
                        public void onTxStreamFinishedResponse( CompletableFuture<CatchupResult> signal,
                                TxStreamFinishedResponse response )
                        {
                            signal.complete( response.status() );
                        }
                    } );

            switch ( catchupResult )
            {
            case SUCCESS:
                log.info( "[" + Thread.currentThread() + "] Successfully completed transaction pull from " + lastAppliedTxId );
                break;
            case E_TRANSACTION_PRUNED:
                log.info( "[" + Thread.currentThread() + "] Tx pull unable to get transactions starting from " + lastAppliedTxId +
                        " such transaction have been pruned. Attempting a store copy." );
                downloadDatabase( core, localStoreId );
                break;
            default:
                log.info( "[" + Thread.currentThread() + "] Tx pull unable to get transactions starting from " + lastAppliedTxId );
                break;
            }
        }
        catch ( Throwable e )
        {
            log.warn( "Tx pull attempt failed, will retry at the next regularly scheduled polling attempt.", e );
        }
    }

    private void downloadDatabase( MemberId core, StoreId localStoreId ) throws Throwable
    {
        pause();
        try
        {
            localDatabase.stop();
            new CopyStoreSafely( fs, localDatabase, copiedStoreRecovery, log ).
                    copyWholeStoreFrom( core, localStoreId, storeFetcher );
            localDatabase.start();
            applier.refreshFromNewStore();
        }
        finally
        {
            resume();
        }
    }

    public void pause()
    {
        timeout.cancel();
    }

    public void resume()
    {
        timeout.renew();
    }
}
