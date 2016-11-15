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

import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentCaptor;

import java.io.File;
import java.util.concurrent.TimeUnit;

import org.neo4j.causalclustering.catchup.CatchUpClient;
import org.neo4j.causalclustering.catchup.CatchUpResponseCallback;
import org.neo4j.causalclustering.catchup.CatchupResult;
import org.neo4j.causalclustering.catchup.storecopy.CopiedStoreRecovery;
import org.neo4j.causalclustering.catchup.storecopy.LocalDatabase;
import org.neo4j.causalclustering.catchup.storecopy.StoreFetcher;
import org.neo4j.causalclustering.core.consensus.schedule.ControlledRenewableTimeoutService;
import org.neo4j.causalclustering.identity.MemberId;
import org.neo4j.causalclustering.identity.StoreId;
import org.neo4j.causalclustering.messaging.routing.CoreMemberSelectionStrategy;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.kernel.impl.transaction.CommittedTransactionRepresentation;
import org.neo4j.kernel.impl.transaction.log.TransactionIdStore;
import org.neo4j.kernel.impl.transaction.log.checkpoint.TriggerInfo;
import org.neo4j.kernel.lifecycle.Lifecycle;
import org.neo4j.kernel.monitoring.Monitors;
import org.neo4j.logging.NullLogProvider;
import org.neo4j.test.OnDemandJobScheduler;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.neo4j.causalclustering.catchup.tx.TxPollingClient.Timeouts.TX_PULLER_TIMEOUT;
import static org.neo4j.causalclustering.catchup.tx.TxPollingClient.txPolling;
import static org.neo4j.kernel.impl.transaction.log.TransactionIdStore.BASE_TX_ID;
import static org.neo4j.kernel.impl.util.JobScheduler.Groups.checkPoint;

public class TxPollingClientTest
{
    private final CatchUpClient catchUpClient = mock( CatchUpClient.class );
    private final CoreMemberSelectionStrategy serverSelection = mock( CoreMemberSelectionStrategy.class );
    private final MemberId coreMemberId = mock( MemberId.class );
    private final TransactionIdStore idStore = mock( TransactionIdStore.class );

    private final BatchingTxApplier txApplier = mock( BatchingTxApplier.class );
    private final ControlledRenewableTimeoutService timeoutService = new ControlledRenewableTimeoutService();

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

    private final OnDemandJobScheduler scheduler = spy(  new OnDemandJobScheduler());
    private final TxPollingClient txPuller =
            new TxPollingClient( NullLogProvider.getInstance(), fs, localDatabase, startStopOnStoreCopy, storeFetcher,
                    catchUpClient, serverSelection, txPullIntervalMillis, txApplier, new Monitors(),
                    copiedStoreRecovery, scheduler );

    @Before
    public void before() throws Throwable
    {
        when( idStore.getLastCommittedTransactionId() ).thenReturn( BASE_TX_ID );
        when( serverSelection.coreMember() ).thenReturn( coreMemberId );
        txPuller.start();
    }

    @Test
    public void shouldSendPullRequestOnTick() throws Throwable
    {
        // given
        long lastAppliedTxId = 99L;
        when( txApplier.lastQueuedTxId() ).thenReturn( lastAppliedTxId );

        // when
        scheduler.runJob();

        // then
        verify( catchUpClient ).makeBlockingRequest( any( MemberId.class ), any( TxPullRequest.class ),
                any( CatchUpResponseCallback.class ) );
    }

    @Test
    public void shouldKeepMakingPullRequestsUntilEndOfStream() throws Throwable
    {
        // given
        long lastAppliedTxId = 99L;
        when( txApplier.lastQueuedTxId() ).thenReturn( lastAppliedTxId );

        // when
        when(catchUpClient.<CatchupResult>makeBlockingRequest(  any( MemberId.class ), any( TxPullRequest.class ),
                any( CatchUpResponseCallback.class )  ))
                .thenReturn( CatchupResult.SUCCESS_END_OF_BATCH, CatchupResult.SUCCESS_END_OF_STREAM );

        scheduler.runJob();

        // then
        verify( catchUpClient, times( 2 ) ).makeBlockingRequest( any( MemberId.class ), any( TxPullRequest.class ),
                any( CatchUpResponseCallback.class ) );
    }

    @Test
    public void shouldRescheduleTheJobAfterARun() throws Throwable
    {
        // given
        Runnable scheduledJob = scheduler.getJob();
        assertNotNull( scheduledJob );

        // when
        scheduler.runJob();

        // then
        verify( scheduler, times( 2 ) ).schedule( eq( txPolling ), any( Runnable.class ),
                eq( 100L ), eq( TimeUnit.MILLISECONDS ) );
        assertEquals( scheduledJob, scheduler.getJob() );
    }

    @Test
    public void shouldCopyStoreIfCatchUpClientFails() throws Throwable
    {
        // given
        when( catchUpClient.makeBlockingRequest( any( MemberId.class ), any( TxPullRequest.class ),
                any( CatchUpResponseCallback.class ) ) ).thenReturn( CatchupResult.E_TRANSACTION_PRUNED );

        // when
        scheduler.runJob();

        // then
        verify( localDatabase ).stop();
        verify( startStopOnStoreCopy ).stop();
        verify( storeFetcher ).copyStore( any( MemberId.class ), eq( storeId ), any( File.class ) );
        verify( localDatabase ).start();
        verify( startStopOnStoreCopy ).start();
        verify( txApplier ).refreshFromNewStore();
    }
}
