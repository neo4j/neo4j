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

import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Future;

import org.neo4j.causalclustering.catchup.CatchUpClient;
import org.neo4j.causalclustering.catchup.CatchUpResponseCallback;
import org.neo4j.causalclustering.catchup.CatchupAddressProvider;
import org.neo4j.causalclustering.catchup.CatchupResult;
import org.neo4j.causalclustering.catchup.storecopy.LocalDatabase;
import org.neo4j.causalclustering.catchup.storecopy.StoreCopyProcess;
import org.neo4j.causalclustering.core.consensus.schedule.CountingTimerService;
import org.neo4j.causalclustering.core.consensus.schedule.Timer;
import org.neo4j.causalclustering.discovery.TopologyService;
import org.neo4j.causalclustering.helper.Suspendable;
import org.neo4j.causalclustering.identity.MemberId;
import org.neo4j.causalclustering.identity.StoreId;
import org.neo4j.causalclustering.upstream.UpstreamDatabaseStrategySelector;
import org.neo4j.helpers.AdvertisedSocketAddress;
import org.neo4j.kernel.impl.transaction.log.TransactionIdStore;
import org.neo4j.kernel.internal.DatabaseHealth;
import org.neo4j.kernel.monitoring.Monitors;
import org.neo4j.logging.NullLogProvider;
import org.neo4j.test.FakeClockJobScheduler;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.neo4j.causalclustering.catchup.tx.CatchupPollingProcess.State.PANIC;
import static org.neo4j.causalclustering.catchup.tx.CatchupPollingProcess.State.STORE_COPYING;
import static org.neo4j.causalclustering.catchup.tx.CatchupPollingProcess.State.TX_PULLING;
import static org.neo4j.causalclustering.catchup.tx.CatchupPollingProcess.Timers.TX_PULLER_TIMER;
import static org.neo4j.helpers.collection.Iterables.single;
import static org.neo4j.kernel.impl.transaction.log.TransactionIdStore.BASE_TX_ID;

public class CatchupPollingProcessTest
{
    private final CatchUpClient catchUpClient = mock( CatchUpClient.class );
    private final UpstreamDatabaseStrategySelector strategyPipeline = mock( UpstreamDatabaseStrategySelector.class );
    private final MemberId coreMemberId = mock( MemberId.class );
    private final TransactionIdStore idStore = mock( TransactionIdStore.class );

    private final BatchingTxApplier txApplier = mock( BatchingTxApplier.class );
    private final FakeClockJobScheduler scheduler = new FakeClockJobScheduler();
    private final CountingTimerService timerService = new CountingTimerService( scheduler, NullLogProvider.getInstance() );

    private final long txPullIntervalMillis = 100;
    private final StoreCopyProcess storeCopyProcess = mock( StoreCopyProcess.class );
    private final StoreId storeId = new StoreId( 1, 2, 3, 4 );
    private final LocalDatabase localDatabase = mock( LocalDatabase.class );
    private final TopologyService topologyService = mock( TopologyService.class );
    private final AdvertisedSocketAddress coreMemberAddress = new AdvertisedSocketAddress( "hostname", 1234 );
    private final CatchupAddressProvider catchupAddressProvider = CatchupAddressProvider.fromSingleAddress( coreMemberAddress );

    {
        when( localDatabase.storeId() ).thenReturn( storeId );
        when( topologyService.findCatchupAddress( coreMemberId ) ).thenReturn( Optional.of( coreMemberAddress ) );
    }

    private final Suspendable startStopOnStoreCopy = mock( Suspendable.class );

    private final CatchupPollingProcess txPuller =
            new CatchupPollingProcess( NullLogProvider.getInstance(), localDatabase, startStopOnStoreCopy, catchUpClient, strategyPipeline, timerService,
                    txPullIntervalMillis, txApplier, new Monitors(), storeCopyProcess, () -> mock( DatabaseHealth.class ), topologyService );

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
        timerService.invoke( TX_PULLER_TIMER );

        // then
        verify( catchUpClient ).makeBlockingRequest( any( AdvertisedSocketAddress.class ), any( TxPullRequest.class ), any( CatchUpResponseCallback.class ) );
    }

    @Test
    public void shouldKeepMakingPullRequestsUntilEndOfStream() throws Throwable
    {
        // given
        txPuller.start();
        long lastAppliedTxId = 99L;
        when( txApplier.lastQueuedTxId() ).thenReturn( lastAppliedTxId );

        // when
        when( catchUpClient.<TxStreamFinishedResponse>makeBlockingRequest( any( AdvertisedSocketAddress.class ), any( TxPullRequest.class ),
                any( CatchUpResponseCallback.class ) ) ).thenReturn( new TxStreamFinishedResponse( CatchupResult.SUCCESS_END_OF_BATCH, 10 ),
                new TxStreamFinishedResponse( CatchupResult.SUCCESS_END_OF_STREAM, 10 ) );

        timerService.invoke( TX_PULLER_TIMER );

        // then
        verify( catchUpClient, times( 2 ) ).makeBlockingRequest( any( AdvertisedSocketAddress.class ), any( TxPullRequest.class ),
                any( CatchUpResponseCallback.class ) );
    }

    @Test
    public void shouldRenewTxPullTimeoutOnSuccessfulTxPulling() throws Throwable
    {
        // when
        txPuller.start();
        when( catchUpClient.makeBlockingRequest( any( AdvertisedSocketAddress.class ), any( TxPullRequest.class ),
                any( CatchUpResponseCallback.class ) ) ).thenReturn( new TxStreamFinishedResponse( CatchupResult.SUCCESS_END_OF_STREAM, 0 ) );

        timerService.invoke( TX_PULLER_TIMER );

        // then
        assertEquals( 1, timerService.invocationCount( TX_PULLER_TIMER ) );
    }

    @Test
    public void nextStateShouldBeStoreCopyingIfRequestedTransactionHasBeenPrunedAway() throws Throwable
    {
        // when
        txPuller.start();
        when( catchUpClient.makeBlockingRequest( any( AdvertisedSocketAddress.class ), any( TxPullRequest.class ),
                any( CatchUpResponseCallback.class ) ) ).thenReturn( new TxStreamFinishedResponse( CatchupResult.E_TRANSACTION_PRUNED, 0 ) );

        timerService.invoke( TX_PULLER_TIMER );

        // then
        assertEquals( STORE_COPYING, txPuller.state() );
    }

    @Test
    public void nextStateShouldBeTxPullingAfterASuccessfulStoreCopy() throws Throwable
    {
        // given
        txPuller.start();
        when( catchUpClient.makeBlockingRequest( any( AdvertisedSocketAddress.class ), any( TxPullRequest.class ),
                any( CatchUpResponseCallback.class ) ) ).thenReturn( new TxStreamFinishedResponse( CatchupResult.E_TRANSACTION_PRUNED, 0 ) );

        // when (tx pull)
        timerService.invoke( TX_PULLER_TIMER );

        // when (store copy)
        timerService.invoke( TX_PULLER_TIMER );

        // then
        verify( localDatabase ).stopForStoreCopy();
        verify( startStopOnStoreCopy ).disable();
        verify( storeCopyProcess ).replaceWithStoreFrom( any( CatchupAddressProvider.class ), eq( storeId ) );
        verify( localDatabase ).start();
        verify( startStopOnStoreCopy ).enable();
        verify( txApplier ).refreshFromNewStore();

        // then
        assertEquals( TX_PULLING, txPuller.state() );
    }

    @Test
    public void shouldNotRenewTheTimeoutIfInPanicState()
    {
        // given
        txPuller.start();
        CatchUpResponseCallback callback = mock( CatchUpResponseCallback.class );

        doThrow( new RuntimeException( "Panic all the things" ) ).when( callback ).onTxPullResponse( any( CompletableFuture.class ),
                any( TxPullResponse.class ) );
        Timer timer = Mockito.spy( single( timerService.getTimers( TX_PULLER_TIMER ) ) );

        // when
        timerService.invoke( TX_PULLER_TIMER );

        // then
        assertEquals( PANIC, txPuller.state() );
        verify( timer, never() ).reset();
    }

    @Test
    public void shouldNotSignalOperationalUntilPulling() throws Throwable
    {
        // given
        when( catchUpClient.<TxStreamFinishedResponse>makeBlockingRequest( any( AdvertisedSocketAddress.class ), any( TxPullRequest.class ),
                any( CatchUpResponseCallback.class ) ) ).thenReturn( new TxStreamFinishedResponse( CatchupResult.E_TRANSACTION_PRUNED, 0 ),
                new TxStreamFinishedResponse( CatchupResult.SUCCESS_END_OF_BATCH, 10 ),
                new TxStreamFinishedResponse( CatchupResult.SUCCESS_END_OF_STREAM, 15 ) );

        // when
        txPuller.start();
        Future<Boolean> operationalFuture = txPuller.upToDateFuture();
        assertFalse( operationalFuture.isDone() );

        timerService.invoke( TX_PULLER_TIMER ); // realises we need a store copy
        assertFalse( operationalFuture.isDone() );

        timerService.invoke( TX_PULLER_TIMER ); // does the store copy
        assertFalse( operationalFuture.isDone() );

        timerService.invoke( TX_PULLER_TIMER ); // does a pulling
        assertTrue( operationalFuture.isDone() );
        assertTrue( operationalFuture.get() );

        // then
        assertEquals( TX_PULLING, txPuller.state() );
    }
}
