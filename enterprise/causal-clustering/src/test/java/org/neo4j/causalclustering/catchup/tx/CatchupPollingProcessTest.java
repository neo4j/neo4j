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

import java.io.File;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Future;

import org.neo4j.causalclustering.catchup.CatchUpClient;
import org.neo4j.causalclustering.catchup.CatchUpResponseCallback;
import org.neo4j.causalclustering.catchup.CatchupResult;
import org.neo4j.causalclustering.catchup.storecopy.CopiedStoreRecovery;
import org.neo4j.causalclustering.catchup.storecopy.LocalDatabase;
import org.neo4j.causalclustering.catchup.storecopy.StoreFetcher;
import org.neo4j.causalclustering.identity.MemberId;
import org.neo4j.causalclustering.identity.StoreId;
import org.neo4j.causalclustering.messaging.routing.CoreMemberSelectionStrategy;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.kernel.impl.transaction.log.TransactionIdStore;
import org.neo4j.kernel.internal.DatabaseHealth;
import org.neo4j.kernel.lifecycle.Lifecycle;
import org.neo4j.kernel.monitoring.Monitors;
import org.neo4j.logging.NullLogProvider;
import org.neo4j.test.OnDemandJobScheduler;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
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
import static org.neo4j.kernel.impl.transaction.log.TransactionIdStore.BASE_TX_ID;

public class CatchupPollingProcessTest
{
    private final CatchUpClient catchUpClient = mock( CatchUpClient.class );
    private final CoreMemberSelectionStrategy serverSelection = mock( CoreMemberSelectionStrategy.class );
    private final MemberId coreMemberId = mock( MemberId.class );
    private final TransactionIdStore idStore = mock( TransactionIdStore.class );

    private final BatchingTxApplier txApplier = mock( BatchingTxApplier.class );
    private final OnDemandJobScheduler jobScheduler = new OnDemandJobScheduler();

    private final long txPullIntervalMillis = 100;
    private final FileSystemAbstraction fs = mock( FileSystemAbstraction.class );
    private final StoreFetcher storeFetcher = mock( StoreFetcher.class );
    private final CopiedStoreRecovery copiedStoreRecovery = mock( CopiedStoreRecovery.class );
    private final StoreId storeId = new StoreId( 1, 2, 3, 4 );
    private final LocalDatabase localDatabase = mock( LocalDatabase.class );
    {
        when( localDatabase.storeId() ).thenReturn( storeId );
    }
    private final Lifecycle startStopOnStoreCopy = mock( Lifecycle.class );

    private final CatchupPollingProcess txPuller =
            new CatchupPollingProcess( NullLogProvider.getInstance(), fs, localDatabase, startStopOnStoreCopy, storeFetcher,
                    catchUpClient, serverSelection, jobScheduler, txPullIntervalMillis, txApplier, new Monitors(),
                    copiedStoreRecovery, () -> mock( DatabaseHealth.class) );

    @Before
    public void before() throws Throwable
    {
        when( idStore.getLastCommittedTransactionId() ).thenReturn( BASE_TX_ID );
        when( serverSelection.coreMember() ).thenReturn( coreMemberId );
    }

    @Test
    public void shouldSendPullRequestOnTick() throws Throwable
    {
        // given
        txPuller.start();
        long lastAppliedTxId = 99L;
        when( txApplier.lastQueuedTxId() ).thenReturn( lastAppliedTxId );

        // when
        jobScheduler.consumeAndRunJob();

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
        when(catchUpClient.<CatchupResult>makeBlockingRequest(  any( MemberId.class ), any( TxPullRequest.class ),
                any( CatchUpResponseCallback.class )  ))
                .thenReturn( CatchupResult.SUCCESS_END_OF_BATCH, CatchupResult.SUCCESS_END_OF_STREAM );

        jobScheduler.consumeAndRunJob();

        // then
        verify( catchUpClient, times( 2 ) ).makeBlockingRequest( any( MemberId.class ), any( TxPullRequest.class ),
                any( CatchUpResponseCallback.class ) );
    }

    @Test
    public void shouldRenewTxPullTimeoutOnSuccessfulTxPulling() throws Throwable
    {
        // when
        txPuller.start();
        when( catchUpClient .makeBlockingRequest( any( MemberId.class ), any( TxPullRequest.class ),
                any(CatchUpResponseCallback.class)))
                .thenReturn( CatchupResult.SUCCESS_END_OF_STREAM );

        jobScheduler.consumeAndRunJob();

        // then
        assertNotNull( jobScheduler.getJob() );
    }

    @Test
    public void nextStateShouldBeStoreCopyingIfRequestedTransactionHasBeenPrunedAway() throws Throwable
    {
        // when
        txPuller.start();
        when( catchUpClient.makeBlockingRequest(
                any( MemberId.class ), any( TxPullRequest.class ), any( CatchUpResponseCallback.class ) ) )
                .thenReturn( CatchupResult.E_TRANSACTION_PRUNED );

        jobScheduler.consumeAndRunJob();

        // then
        assertEquals( STORE_COPYING, txPuller.state() );
    }

    @Test
    public void nextStateShouldBeTxPullingAfterASuccessfulStoreCopy() throws Throwable
    {
        // given
        txPuller.start();
        when( catchUpClient.makeBlockingRequest( any( MemberId.class ), any( TxPullRequest.class ),
                any( CatchUpResponseCallback.class ) ) ).thenReturn( CatchupResult.E_TRANSACTION_PRUNED );

        // when (tx pull)
        jobScheduler.consumeAndRunJob();

        // when (store copy)
        jobScheduler.consumeAndRunJob();

        // then
        verify( localDatabase ).stop();
        verify( startStopOnStoreCopy ).stop();
        verify( storeFetcher ).copyStore( any( MemberId.class ), eq( storeId ), any( File.class ) );
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
        jobScheduler.consumeAndRunJob();

        // then
        assertEquals( PANIC, txPuller.state() );
        assertNull( jobScheduler.getJob() );
    }

    @Test
    public void shouldNotSignalOperationalUntilPulling() throws Throwable
    {
        // given
        when(catchUpClient.<CatchupResult>makeBlockingRequest(  any( MemberId.class ), any( TxPullRequest.class ),
                any( CatchUpResponseCallback.class )  ))
                .thenReturn(
                        CatchupResult.E_TRANSACTION_PRUNED,
                        CatchupResult.SUCCESS_END_OF_BATCH,
                        CatchupResult.SUCCESS_END_OF_STREAM );

        // when
        txPuller.start();
        Future<Boolean> operationalFuture = txPuller.upToDateFuture();
        assertFalse( operationalFuture.isDone() );

        jobScheduler.consumeAndRunJob(); // realises we need a store copy
        assertFalse( operationalFuture.isDone() );

        jobScheduler.consumeAndRunJob(); // does the store copy
        assertFalse( operationalFuture.isDone() );

        jobScheduler.consumeAndRunJob(); // does a pulling
        assertTrue( operationalFuture.isDone() );
        assertTrue( operationalFuture.get() );

        // then
        assertEquals( TX_PULLING, txPuller.state() );
    }
}
