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
package org.neo4j.coreedge.catchup.tx.edge;

import org.neo4j.coreedge.catchup.storecopy.CoreClient;
import org.neo4j.coreedge.raft.schedule.RenewableTimeoutService;
import org.neo4j.coreedge.raft.schedule.RenewableTimeoutService.RenewableTimeout;
import org.neo4j.coreedge.raft.schedule.RenewableTimeoutService.TimeoutName;
import org.neo4j.coreedge.identity.MemberId;
import org.neo4j.coreedge.messaging.CoreMemberSelectionStrategy;
import org.neo4j.kernel.lifecycle.LifecycleAdapter;
import org.neo4j.logging.Log;
import org.neo4j.logging.LogProvider;

import static org.neo4j.coreedge.catchup.tx.edge.TxPollingClient.States.INACTIVE;
import static org.neo4j.coreedge.catchup.tx.edge.TxPollingClient.States.RECEIVING_TRANSACTIONS;
import static org.neo4j.coreedge.catchup.tx.edge.TxPollingClient.States.WAIT_NEXT_PULL;
import static org.neo4j.coreedge.catchup.tx.edge.TxPollingClient.Timeouts.TX_PULLER_TIMEOUT;

/**
 * This class is responsible for pulling transactions from a core server and queuing
 * them to be applied with the {@link BatchingTxApplier}. It operates in two main states:
 *
 *   WAIT_NEXT_PULL:        Waiting for the next "tick" for pulling transactions.
 *   RECEIVING_TRANSACTION: Receiving transactions during the pull.
 *
 * There is also the initial INACTIVE state, also entered when stopping.
 *
 * Pull requests are issued on a fixed interval, but skipped if the {@link BatchingTxApplier}
 * isn't yet finished with the current work.
 */
public class TxPollingClient extends LifecycleAdapter implements TxPullListener
{
    enum Timeouts implements TimeoutName
    {
        TX_PULLER_TIMEOUT
    }

    interface Handler
    {
        default void onTimeout( TxPollingClient ctx ) {}
        default void onTxReceived( TxPollingClient ctx, TxPullResponse tx ) {}
        default void onTxStreamingComplete( TxPollingClient ctx ) {}
    }

    enum States
    {
        INACTIVE( new Inactive() ),
        WAIT_NEXT_PULL( new WaitNextPull() ),
        RECEIVING_TRANSACTIONS( new ReceivingTransactions() );

        Handler handler;

        States( Handler handler )
        {
            this.handler = handler;
        }
    }

    private final Log log;

    private final CoreClient coreClient;
    private final CoreMemberSelectionStrategy connectionStrategy;
    private final RenewableTimeoutService timeoutService;

    private final long txPullIntervalMillis;
    private RenewableTimeout timeout;

    private States state = INACTIVE;
    private BatchingTxApplier applier;

    private long unexpectedCount;
    private boolean streamingCompleted;

    public TxPollingClient( LogProvider logProvider, CoreClient coreClient, CoreMemberSelectionStrategy connectionStrategy,
            RenewableTimeoutService timeoutService, long txPullIntervalMillis, BatchingTxApplier applier )
    {
        this.log = logProvider.getLog( getClass() );
        this.coreClient = coreClient;
        this.connectionStrategy = connectionStrategy;
        this.timeoutService = timeoutService;
        this.txPullIntervalMillis = txPullIntervalMillis;
        this.applier = applier;
    }

    @Override
    public synchronized void start() throws Throwable
    {
        coreClient.addTxPullResponseListener( this );
        coreClient.addTxStreamCompleteListener( this );

        timeout = timeoutService.create( TX_PULLER_TIMEOUT, txPullIntervalMillis, 0, timeout -> onTimeout() );

        state = WAIT_NEXT_PULL;
    }

    @Override
    public synchronized void stop() throws Throwable
    {
        state = INACTIVE;

        coreClient.removeTxPullResponseListener( this );
        coreClient.removeTxStreamCompleteListener( this );
    }

    /**
     * New transaction received off the network.
     *
     * @param tx Contains the transaction.
     */
    @Override
    public synchronized void onTxReceived( TxPullResponse tx )
    {
        state.handler.onTxReceived( this, tx );
    }

    /**
     * End of tx responses received off the network.
     */
    @Override
    public synchronized void onTxStreamingComplete( long ignored )
    {
        state.handler.onTxStreamingComplete( this );
    }

    /**
     * Time to pull!
     */
    private synchronized void onTimeout()
    {
        timeout.renew();
        state.handler.onTimeout( this );
    }

    private static class Inactive implements Handler
    {
        // do nothing
    }

    private static class WaitNextPull implements Handler
    {
        @Override
        public void onTxReceived( TxPollingClient ctx, TxPullResponse tx )
        {
            ctx.unexpectedCount++;
        }

        public void onTimeout( TxPollingClient ctx )
        {
            if ( ctx.applier.workPending() )
            {
                return;
            }

            if ( ctx.unexpectedCount > 0 )
            {
                ctx.log.warn( "Received %d transactions late. Previous stream was " + (ctx.streamingCompleted ? "" : "not ") + "completed." );
                ctx.unexpectedCount = 0;
            }

            MemberId transactionServer;
            try
            {
                transactionServer = ctx.connectionStrategy.coreMember();
                ctx.coreClient.pollForTransactions( transactionServer, ctx.applier.lastAppliedTxId() );
            }
            catch ( Exception e )
            {
                ctx.log.warn( "Tx pull attempt failed, will retry at the next regularly scheduled polling attempt.", e );
            }

            ctx.state = RECEIVING_TRANSACTIONS;
        }
    }

    private static class ReceivingTransactions implements Handler
    {
        @Override
        public void onTimeout( TxPollingClient ctx )
        {
            ctx.streamingCompleted = false;
            ctx.state = WAIT_NEXT_PULL;
        }

        @Override
        public void onTxReceived( TxPollingClient ctx, TxPullResponse tx )
        {
            ctx.applier.queue( tx.tx() );
            ctx.timeout.renew();
        }

        @Override
        public void onTxStreamingComplete( TxPollingClient ctx )
        {
            ctx.streamingCompleted = true;
            ctx.state = WAIT_NEXT_PULL;
        }
    }
}
