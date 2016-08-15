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
package org.neo4j.coreedge.catchup.tx;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;

import org.neo4j.coreedge.catchup.CatchUpClient;
import org.neo4j.coreedge.catchup.CatchUpResponseAdaptor;
import org.neo4j.coreedge.core.consensus.schedule.RenewableTimeoutService;
import org.neo4j.coreedge.core.consensus.schedule.RenewableTimeoutService.RenewableTimeout;
import org.neo4j.coreedge.core.consensus.schedule.RenewableTimeoutService.TimeoutName;
import org.neo4j.coreedge.identity.MemberId;
import org.neo4j.coreedge.identity.StoreId;
import org.neo4j.coreedge.messaging.routing.CoreMemberSelectionStrategy;
import org.neo4j.kernel.lifecycle.LifecycleAdapter;
import org.neo4j.kernel.monitoring.Monitors;
import org.neo4j.logging.Log;
import org.neo4j.logging.LogProvider;

import static org.neo4j.coreedge.catchup.tx.TxPollingClient.Timeouts.TX_PULLER_TIMEOUT;

/**
 * This class is responsible for pulling transactions from a core server and queuing
 * them to be applied with the {@link BatchingTxApplier}.
 *
 * Pull requests are issued on a fixed interval, but skipped if the {@link BatchingTxApplier}
 * isn't yet finished with the current work.
 */
public class TxPollingClient extends LifecycleAdapter
{
    private final Supplier<StoreId> localDatabase;
    private PullRequestMonitor pullRequestMonitor;

    enum Timeouts implements TimeoutName
    {
        TX_PULLER_TIMEOUT
    }

    private final Log log;

    private final CatchUpClient catchUpClient;
    private final CoreMemberSelectionStrategy connectionStrategy;
    private final RenewableTimeoutService timeoutService;

    private final long txPullIntervalMillis;
    private RenewableTimeout timeout;

    private BatchingTxApplier applier;

    public TxPollingClient( LogProvider logProvider, Supplier<StoreId> localDatabase,
                            CatchUpClient catchUpClient, CoreMemberSelectionStrategy connectionStrategy,
                            RenewableTimeoutService timeoutService, long txPullIntervalMillis,
                            BatchingTxApplier applier, Monitors monitors )
    {
        this.localDatabase = localDatabase;
        this.log = logProvider.getLog( getClass() );
        this.catchUpClient = catchUpClient;
        this.connectionStrategy = connectionStrategy;
        this.timeoutService = timeoutService;
        this.txPullIntervalMillis = txPullIntervalMillis;
        this.applier = applier;
        this.pullRequestMonitor = monitors.newMonitor( PullRequestMonitor.class );
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
            return;
        }

        MemberId transactionServer;
        try
        {
            transactionServer = connectionStrategy.coreMember();
            long lastAppliedTxId = applier.lastAppliedTxId();
            pullRequestMonitor.txPullRequest( lastAppliedTxId );
            TxPullRequest txPullRequest = new TxPullRequest( lastAppliedTxId, localDatabase.get() );
            catchUpClient.makeBlockingRequest( transactionServer, txPullRequest, 30, TimeUnit.SECONDS,
                    new CatchUpResponseAdaptor<Long>() {
                        @Override
                        public void onTxPullResponse( CompletableFuture<Long> signal, TxPullResponse response )
                        {
                            applier.queue( response.tx() );
                            timeout.renew();
                        }

                        @Override
                        public void onTxStreamFinishedResponse( CompletableFuture<Long> signal,
                                                                TxStreamFinishedResponse response )
                        {
                            signal.complete( response.lastTransactionIdSent() );
                        }
                    });
        }
        catch ( Exception e )
        {
            log.warn( "Tx pull attempt failed, will retry at the next regularly scheduled polling attempt.", e );
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
