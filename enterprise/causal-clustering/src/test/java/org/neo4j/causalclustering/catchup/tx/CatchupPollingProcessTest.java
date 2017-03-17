/*
 * Copyright (c) 2002-2017 "Neo Technology,"
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

import org.junit.Before;
import org.junit.Test;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Future;

import org.neo4j.causalclustering.catchup.CatchUpClient;
import org.neo4j.causalclustering.catchup.CatchUpResponseCallback;
import org.neo4j.causalclustering.catchup.CatchupResult;
import org.neo4j.causalclustering.catchup.storecopy.LocalDatabase;
import org.neo4j.causalclustering.catchup.storecopy.StoreCopyProcess;
import org.neo4j.causalclustering.core.consensus.schedule.ControlledRenewableTimeoutService;
import org.neo4j.causalclustering.identity.MemberId;
import org.neo4j.causalclustering.identity.StoreId;
import org.neo4j.causalclustering.readreplica.UpstreamDatabaseStrategySelector;
import org.neo4j.kernel.impl.transaction.log.TransactionIdStore;
import org.neo4j.kernel.internal.DatabaseHealth;
import org.neo4j.kernel.lifecycle.Lifecycle;
import org.neo4j.kernel.monitoring.Monitors;
import org.neo4j.logging.NullLogProvider;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.neo4j.causalclustering.catchup.tx.CatchupPollingProcess.State.PANIC;
import static org.neo4j.causalclustering.catchup.tx.CatchupPollingProcess.State.STORE_COPYING;
import static org.neo4j.causalclustering.catchup.tx.CatchupPollingProcess.State.TX_PULLING;
import static org.neo4j.causalclustering.catchup.tx.CatchupPollingProcess.Timeouts.TX_PULLER_TIMEOUT;
import static org.neo4j.kernel.impl.transaction.log.TransactionIdStore.BASE_TX_ID;

public class CatchupPollingProcessTest
{
    private final CatchUpClient catchUpClient = mock( CatchUpClient.class );
    private final UpstreamDatabaseStrategySelector strategyPipeline = mock( UpstreamDatabaseStrategySelector.class );
    private final MemberId coreMemberId = mock( MemberId.class );
    private final TransactionIdStore idStore = mock( TransactionIdStore.class );

    private final BatchingTxApplier txApplier = mock( BatchingTxApplier.class );
    private final ControlledRenewableTimeoutService timeoutService = new ControlledRenewableTimeoutService();

    private final long txPullIntervalMillis = 100;
    private final StoreCopyProcess storeCopyProcess = mock( StoreCopyProcess.class );
    private final StoreId storeId = new StoreId( 1, 2, 3, 4 );
    private final LocalDatabase localDatabase = mock( LocalDatabase.class );

    {
        when( localDatabase.storeId() ).thenReturn( storeId );
    }

    private final Lifecycle startStopOnStoreCopy = mock( Lifecycle.class );

    private final CatchupPollingProcess txPuller =
            new CatchupPollingProcess( NullLogProvider.getInstance(), localDatabase, startStopOnStoreCopy,
                    catchUpClient, strategyPipeline, timeoutService, txPullIntervalMillis, txApplier, new Monitors(),
                    storeCopyProcess, () -> mock( DatabaseHealth.class ) );

    @Before
    public void before() throws Throwable
    {
        when( idStore.getLastCommittedTransactionId() ).thenReturn( BASE_TX_ID );
        when( strategyPipeline.bestUpstreamDatabase() ).thenReturn( coreMemberId );
    }

    @Test
    public void shouldSendPullRequestOnTick() throws Throwable
    {
        // given
        txPuller.start();
        long lastAppliedTxId = 99L;
        when( txApplier.lastQueuedTxId() ).thenReturn( lastAppliedTxId );

        // when
        timeoutService.invokeTimeout( TX_PULLER_TIMEOUT );

        // then
        verify( catchUpClient ).makeBlockingRequest( any( MemberId.class ), any( TxPullRequest.class ),
                any( CatchUpResponseCallback.class ) );
    }

    @Test
    public void shouldKeepMakingPullRequestsUntilEndOfStream() throws Throwable
    {
        // given
        txPuller.start();
        long lastAppliedTxId = 99L;
        when( txApplier.lastQueuedTxId() ).thenReturn( lastAppliedTxId );

        // when
        when( catchUpClient.<TxStreamFinishedResponse>makeBlockingRequest( any( MemberId.class ), any( TxPullRequest.class ),
                any( CatchUpResponseCallback.class ) ) )
                .thenReturn(
                        new TxStreamFinishedResponse( CatchupResult.SUCCESS_END_OF_BATCH, 10 ),
                        new TxStreamFinishedResponse( CatchupResult.SUCCESS_END_OF_STREAM, 10 ) );

        timeoutService.invokeTimeout( TX_PULLER_TIMEOUT );

        // then
        verify( catchUpClient, times( 2 ) ).makeBlockingRequest( any( MemberId.class ), any( TxPullRequest.class ),
                any( CatchUpResponseCallback.class ) );
    }

    @Test
    public void shouldRenewTxPullTimeoutOnSuccessfulTxPulling() throws Throwable
    {
        // when
        txPuller.start();
        when( catchUpClient.makeBlockingRequest( any( MemberId.class ), any( TxPullRequest.class ),
                any( CatchUpResponseCallback.class ) ) ).thenReturn(
                new TxStreamFinishedResponse( CatchupResult.SUCCESS_END_OF_STREAM, 0 ) );

        timeoutService.invokeTimeout( TX_PULLER_TIMEOUT );

        // then
        assertEquals( 1, timeoutService.getTimeout( TX_PULLER_TIMEOUT ).renewalCount() );
    }

    @Test
    public void nextStateShouldBeStoreCopyingIfRequestedTransactionHasBeenPrunedAway() throws Throwable
    {
        // when
        txPuller.start();
        when( catchUpClient.makeBlockingRequest( any( MemberId.class ), any( TxPullRequest.class ),
                any( CatchUpResponseCallback.class ) ) ).thenReturn(
                        new TxStreamFinishedResponse( CatchupResult.E_TRANSACTION_PRUNED, 0 ) );

        timeoutService.invokeTimeout( TX_PULLER_TIMEOUT );

        // then
        assertEquals( STORE_COPYING, txPuller.state() );
    }

    @Test
    public void nextStateShouldBeTxPullingAfterASuccessfulStoreCopy() throws Throwable
    {
        // given
        txPuller.start();
        when( catchUpClient.makeBlockingRequest( any( MemberId.class ), any( TxPullRequest.class ),
                any( CatchUpResponseCallback.class ) ) ).thenReturn(
                        new TxStreamFinishedResponse( CatchupResult.E_TRANSACTION_PRUNED, 0 ) );

        // when (tx pull)
        timeoutService.invokeTimeout( TX_PULLER_TIMEOUT );

        // when (store copy)
        timeoutService.invokeTimeout( TX_PULLER_TIMEOUT );

        // then
        verify( localDatabase ).stopForStoreCopy();
        verify( startStopOnStoreCopy ).stop();
        verify( storeCopyProcess ).replaceWithStoreFrom( any( MemberId.class ), eq( storeId ) );
        verify( localDatabase ).start();
        verify( startStopOnStoreCopy ).start();
        verify( txApplier ).refreshFromNewStore();

        // then
        assertEquals( TX_PULLING, txPuller.state() );
    }

    @Test
    public void shouldNotRenewTheTimeoutIfInPanicState() throws Throwable
    {
        // given
        txPuller.start();
        CatchUpResponseCallback callback = mock( CatchUpResponseCallback.class );

        doThrow( new RuntimeException( "Panic all the things" ) ).when( callback )
                .onTxPullResponse( any( CompletableFuture.class ), any( TxPullResponse.class ) );

        // when
        timeoutService.invokeTimeout( TX_PULLER_TIMEOUT );

        // then
        assertEquals( PANIC, txPuller.state() );
        assertEquals( 0, timeoutService.getTimeout( TX_PULLER_TIMEOUT ).renewalCount() );
    }

    @Test
    public void shouldNotSignalOperationalUntilPulling() throws Throwable
    {
        // given
        when( catchUpClient.<TxStreamFinishedResponse>makeBlockingRequest( any( MemberId.class ), any( TxPullRequest.class ),
                any( CatchUpResponseCallback.class ) ) )
                .thenReturn(
                        new TxStreamFinishedResponse( CatchupResult.E_TRANSACTION_PRUNED, 0),
                        new TxStreamFinishedResponse( CatchupResult.SUCCESS_END_OF_BATCH, 10),
                        new TxStreamFinishedResponse( CatchupResult.SUCCESS_END_OF_STREAM, 15) );

        // when
        txPuller.start();
        Future<Boolean> operationalFuture = txPuller.upToDateFuture();
        assertFalse( operationalFuture.isDone() );

        timeoutService.invokeTimeout( TX_PULLER_TIMEOUT ); // realises we need a store copy
        assertFalse( operationalFuture.isDone() );

        timeoutService.invokeTimeout( TX_PULLER_TIMEOUT ); // does the store copy
        assertFalse( operationalFuture.isDone() );

        timeoutService.invokeTimeout( TX_PULLER_TIMEOUT ); // does a pulling
        assertTrue( operationalFuture.isDone() );
        assertTrue( operationalFuture.get() );

        // then
        assertEquals( TX_PULLING, txPuller.state() );
    }
}
