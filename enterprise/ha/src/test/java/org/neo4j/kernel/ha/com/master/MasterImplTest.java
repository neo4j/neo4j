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
package org.neo4j.kernel.ha.com.master;

import org.junit.Rule;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.InOrder;
import org.mockito.hamcrest.MockitoHamcrest;

import java.util.Collection;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Future;

import org.neo4j.cluster.ClusterSettings;
import org.neo4j.com.RequestContext;
import org.neo4j.com.ResourceReleaser;
import org.neo4j.com.Response;
import org.neo4j.com.TransactionNotPresentOnMasterException;
import org.neo4j.com.TransactionObligationResponse;
import org.neo4j.internal.kernel.api.exceptions.TransactionFailureException;
import org.neo4j.kernel.DeadlockDetectedException;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.ha.HaSettings;
import org.neo4j.kernel.ha.cluster.DefaultConversationSPI;
import org.neo4j.kernel.ha.com.master.MasterImpl.Monitor;
import org.neo4j.kernel.ha.com.master.MasterImpl.SPI;
import org.neo4j.kernel.ha.lock.LockResult;
import org.neo4j.kernel.impl.locking.LockTracer;
import org.neo4j.kernel.impl.locking.Locks.Client;
import org.neo4j.kernel.impl.locking.ResourceTypes;
import org.neo4j.kernel.impl.store.StoreId;
import org.neo4j.kernel.impl.transaction.IllegalResourceException;
import org.neo4j.kernel.impl.transaction.TransactionRepresentation;
import org.neo4j.kernel.impl.transaction.log.TransactionIdStore;
import org.neo4j.kernel.impl.util.collection.NoSuchEntryException;
import org.neo4j.storageengine.api.lock.ResourceType;
import org.neo4j.test.rule.concurrent.OtherThreadRule;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.nullValue;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;
import static org.neo4j.com.StoreIdTestFactory.newStoreIdForCurrentVersion;
import static org.neo4j.helpers.collection.MapUtil.stringMap;

public class MasterImplTest
{
    @Test
    public void givenStartedAndInaccessibleWhenNewLockSessionThrowException()
    {
        // Given
        MasterImpl.SPI spi = mock( MasterImpl.SPI.class );
        Config config = config();

        when( spi.isAccessible() ).thenReturn( false );

        MasterImpl instance = new MasterImpl( spi, mock(
                ConversationManager.class ), mock( MasterImpl.Monitor.class ), config );
        instance.start();

        // When
        try
        {
            instance.newLockSession( new RequestContext( 0, 1, 2, 0, 0 ) );
            fail();
        }
        catch ( TransactionFailureException e )
        {
            // Ok
        }
    }

    @Test
    public void givenStartedAndAccessibleWhenNewLockSessionThenSucceeds() throws Throwable
    {
        // Given
        MasterImpl.SPI spi = mockedSpi();
        Config config = config();

        when( spi.isAccessible() ).thenReturn( true );
        when( spi.getTransactionChecksum( anyLong() ) ).thenReturn( 1L );

        MasterImpl instance = new MasterImpl( spi, mock(
                ConversationManager.class ), mock( MasterImpl.Monitor.class ), config );
        instance.start();
        HandshakeResult handshake = instance.handshake( 1, newStoreIdForCurrentVersion() ).response();

        // When
        try
        {
            instance.newLockSession( new RequestContext( handshake.epoch(), 1, 2, 0, 0 ) );
        }
        catch ( Exception e )
        {
            fail( e.getMessage() );
        }
    }

    @Test
    public void failingToStartTxShouldNotLeadToNPE() throws Throwable
    {
        // Given
        MasterImpl.SPI spi = mockedSpi();
        DefaultConversationSPI conversationSpi = mockedConversationSpi();
        Config config = config();
        ConversationManager conversationManager = new ConversationManager( conversationSpi, config );

        when( spi.isAccessible() ).thenReturn( true );
        when( conversationSpi.acquireClient() ).thenThrow( new RuntimeException( "Nope" ) );
        when( spi.getTransactionChecksum( anyLong() ) ).thenReturn( 1L );
        mockEmptyResponse( spi );

        MasterImpl instance = new MasterImpl( spi, conversationManager, mock( MasterImpl.Monitor.class ), config );
        instance.start();
        Response<HandshakeResult> response = instance.handshake( 1, newStoreIdForCurrentVersion() );
        HandshakeResult handshake = response.response();

        // When
        try
        {
            instance.newLockSession( new RequestContext( handshake.epoch(), 1, 2, 0, 0 ) );
            fail("Should have failed.");
        }
        catch ( Exception e )
        {
            // Then
            assertThat(e, instanceOf( RuntimeException.class ) );
            assertThat(e.getMessage(), equalTo( "Nope" ));
        }
    }

    private void mockEmptyResponse( SPI spi )
    {
        when( spi.packEmptyResponse( any() ) ).thenAnswer(
                invocation -> new TransactionObligationResponse<>( invocation.getArgument( 0 ), StoreId.DEFAULT,
                        TransactionIdStore.BASE_TX_ID, ResourceReleaser.NO_OP ) );
    }

    @Test
    public void shouldNotEndLockSessionWhereThereIsAnActiveLockAcquisition() throws Throwable
    {
        // GIVEN
        CountDownLatch latch = new CountDownLatch( 1 );
        try
        {
            Client client = newWaitingLocksClient( latch );
            MasterImpl master = newMasterWithLocksClient( client );
            HandshakeResult handshake = master.handshake( 1, newStoreIdForCurrentVersion() ).response();

            // WHEN
            RequestContext context = new RequestContext( handshake.epoch(), 1, 2, 0, 0 );
            master.newLockSession( context );
            Future<Void> acquireFuture = otherThread.execute( state ->
            {
                master.acquireExclusiveLock( context, ResourceTypes.NODE, 1L );
                return null;
            } );
            otherThread.get().waitUntilWaiting();
            master.endLockSession( context, true );
            verify( client, never() ).stop();
            verify( client, never() ).close();
            latch.countDown();
            acquireFuture.get();

            // THEN
            verify( client ).close();
        }
        finally
        {
            latch.countDown();
        }
    }

    @Test
    public void shouldStopLockSessionOnFailureWhereThereIsAnActiveLockAcquisition() throws Throwable
    {
        // GIVEN
        CountDownLatch latch = new CountDownLatch( 1 );
        try
        {
            Client client = newWaitingLocksClient( latch );
            MasterImpl master = newMasterWithLocksClient( client );
            HandshakeResult handshake = master.handshake( 1, newStoreIdForCurrentVersion() ).response();

            // WHEN
            RequestContext context = new RequestContext( handshake.epoch(), 1, 2, 0, 0 );
            master.newLockSession( context );
            Future<Void> acquireFuture = otherThread.execute( state ->
            {
                master.acquireExclusiveLock( context, ResourceTypes.NODE, 1L );
                return null;
            } );
            otherThread.get().waitUntilWaiting();
            master.endLockSession( context, false );
            verify( client ).stop();
            verify( client, never() ).close();
            latch.countDown();
            acquireFuture.get();

            // THEN
            verify( client ).close();
        }
        finally
        {
            latch.countDown();
        }
    }

    private MasterImpl newMasterWithLocksClient( Client client )
    {
        SPI spi = mockedSpi();
        DefaultConversationSPI conversationSpi = mockedConversationSpi();
        when( spi.isAccessible() ).thenReturn( true );
        when( conversationSpi.acquireClient() ).thenReturn( client );
        Config config = config();
        ConversationManager conversationManager = new ConversationManager( conversationSpi, config );

        MasterImpl master = new MasterImpl( spi, conversationManager, mock( Monitor.class ), config );
        master.start();
        return master;
    }

    private Client newWaitingLocksClient( final CountDownLatch latch )
    {
        Client client = mock( Client.class );

        doAnswer( invocation ->
        {
            latch.await();
            return null;
        } ).when( client ).acquireExclusive( eq( LockTracer.NONE), any( ResourceType.class ), anyLong() );

        return client;
    }

    @Test
    public void shouldNotAllowCommitIfThereIsNoMatchingLockSession() throws Throwable
    {
        // Given
        MasterImpl.SPI spi = mock( MasterImpl.SPI.class );
        DefaultConversationSPI conversationSpi = mockedConversationSpi();
        Config config = config();
        ConversationManager conversationManager = new ConversationManager( conversationSpi, config );

        when( spi.isAccessible() ).thenReturn( true );
        when( spi.getTransactionChecksum( anyLong() ) ).thenReturn( 1L );
        mockEmptyResponse( spi );

        MasterImpl master = new MasterImpl( spi, conversationManager, mock( MasterImpl.Monitor.class ), config );
        master.start();
        HandshakeResult handshake = master.handshake( 1, newStoreIdForCurrentVersion() ).response();

        RequestContext ctx = new RequestContext( handshake.epoch(), 1, 2, 0, 0 );

        // When
        try
        {
            master.commit( ctx, mock( TransactionRepresentation.class ) );
            fail("Should have failed.");
        }
        catch ( TransactionNotPresentOnMasterException e )
        {
            // Then
            assertThat(e.getMessage(), equalTo( new TransactionNotPresentOnMasterException( ctx ).getMessage() ));
        }
    }

    @Test
    public void shouldAllowCommitIfClientHoldsNoLocks() throws Throwable
    {
        // Given
        MasterImpl.SPI spi = mock( MasterImpl.SPI.class );
        Config config = config();
        DefaultConversationSPI conversationSpi = mockedConversationSpi();
        ConversationManager conversationManager = new ConversationManager( conversationSpi, config );

        when( spi.isAccessible() ).thenReturn( true );
        when( spi.getTransactionChecksum( anyLong() ) ).thenReturn( 1L );
        mockEmptyResponse( spi );

        MasterImpl master = new MasterImpl( spi, conversationManager, mock( MasterImpl.Monitor.class ), config );
        master.start();
        HandshakeResult handshake = master.handshake( 1, newStoreIdForCurrentVersion() ).response();

        final int no_lock_session = -1;
        RequestContext ctx = new RequestContext( handshake.epoch(), 1, no_lock_session, 0, 0 );
        TransactionRepresentation tx = mock( TransactionRepresentation.class );

        // When
        master.commit( ctx, tx );

        // Then
        verify(spi).applyPreparedTransaction( tx );
    }

    @Test
    public void shouldAllowStartNewTransactionAfterClientSessionWasRemovedOnTimeout() throws Throwable
    {
        //Given
        MasterImpl.SPI spi = mockedSpi();
        DefaultConversationSPI conversationSpi = mockedConversationSpi();
        Monitor monitor = mock( Monitor.class );
        Config config = config();
        Client client = mock( Client.class );
        ConversationManager conversationManager = new ConversationManager( conversationSpi, config );
        int machineId = 1;
        MasterImpl master = new MasterImpl( spi, conversationManager, monitor, config );

        when( spi.isAccessible() ).thenReturn( true );
        when( conversationSpi.acquireClient() ).thenReturn( client );
        master.start();
        HandshakeResult handshake = master.handshake( 1, newStoreIdForCurrentVersion() ).response();
        RequestContext requestContext = new RequestContext( handshake.epoch(), machineId, 0, 0, 0 );

        // When
        master.newLockSession( requestContext );
        master.acquireSharedLock( requestContext, ResourceTypes.NODE, 1L );
        conversationManager.stop( requestContext );
        master.newLockSession( requestContext );

        //Then
        Map<Integer,Collection<RequestContext>> transactions = master.getOngoingTransactions();
        assertEquals( 1, transactions.size() );
        assertThat( transactions.get( machineId ), org.hamcrest.Matchers.hasItem( requestContext ) );
    }

    @Test
    public void shouldStartStopConversationManager()
    {
        MasterImpl.SPI spi = mockedSpi();
        ConversationManager conversationManager = mock( ConversationManager.class );
        Config config = config();
        MasterImpl master = new MasterImpl( spi, conversationManager, null, config );

        master.start();
        master.stop();

        InOrder order = inOrder(conversationManager);
        order.verify( conversationManager ).start();
        order.verify( conversationManager ).stop();
        verifyNoMoreInteractions( conversationManager );
    }

    @Test
    public void lockResultMustHaveMessageWhenAcquiringExclusiveLockWithoutConversation() throws Exception
    {
        MasterImpl.SPI spi = mockedSpi();
        ConversationManager conversationManager = mock( ConversationManager.class );
        Config config = config();
        MasterImpl master = new MasterImpl( spi, conversationManager, null, config );

        RequestContext context = createRequestContext( master );
        when( conversationManager.acquire( context ) ).thenThrow( new NoSuchEntryException( "" ) );
        master.acquireExclusiveLock( context, ResourceTypes.NODE, 1 );

        ArgumentCaptor<LockResult> captor = ArgumentCaptor.forClass( LockResult.class );
        verify( spi ).packTransactionObligationResponse( MockitoHamcrest.argThat( is( context ) ), captor.capture() );
        assertThat( captor.getValue().getMessage(), is( not( nullValue() ) ) );
    }

    @Test
    public void lockResultMustHaveMessageWhenAcquiringSharedLockWithoutConversation() throws Exception
    {
        MasterImpl.SPI spi = mockedSpi();
        ConversationManager conversationManager = mock( ConversationManager.class );
        Config config = config();
        MasterImpl master = new MasterImpl( spi, conversationManager, null, config );

        RequestContext context = createRequestContext( master );
        when( conversationManager.acquire( context ) ).thenThrow( new NoSuchEntryException( "" ) );
        master.acquireSharedLock( context, ResourceTypes.NODE, 1 );

        ArgumentCaptor<LockResult> captor = ArgumentCaptor.forClass( LockResult.class );
        verify( spi ).packTransactionObligationResponse( MockitoHamcrest.argThat( is( context ) ), captor.capture() );
        assertThat( captor.getValue().getMessage(), is( not( nullValue() ) ) );
    }

    @Test
    public void lockResultMustHaveMessageWhenAcquiringExclusiveLockDeadlocks()
    {
        MasterImpl.SPI spi = mockedSpi();
        DefaultConversationSPI conversationSpi = mockedConversationSpi();
        Config config = config();
        ConversationManager conversationManager = new ConversationManager( conversationSpi, config );
        conversationManager.start();
        Client locks = mock( Client.class );
        MasterImpl master = new MasterImpl( spi, conversationManager, null, config );

        RequestContext context = createRequestContext( master );
        when( conversationSpi.acquireClient() ).thenReturn( locks );
        ResourceTypes type = ResourceTypes.NODE;
        doThrow( new DeadlockDetectedException( "" ) ).when( locks ).acquireExclusive( LockTracer.NONE, type, 1 );
        master.acquireExclusiveLock( context, type, 1 );

        ArgumentCaptor<LockResult> captor = ArgumentCaptor.forClass( LockResult.class );
        verify( spi ).packTransactionObligationResponse( MockitoHamcrest.argThat( is( context ) ), captor.capture() );
        assertThat( captor.getValue().getMessage(), is( not( nullValue() ) ) );
    }

    @Test
    public void lockResultMustHaveMessageWhenAcquiringSharedLockDeadlocks()
    {
        MasterImpl.SPI spi = mockedSpi();
        DefaultConversationSPI conversationSpi = mockedConversationSpi();
        Config config = config();
        ConversationManager conversationManager = new ConversationManager( conversationSpi, config );
        conversationManager.start();
        Client locks = mock( Client.class );
        MasterImpl master = new MasterImpl( spi, conversationManager, null, config );

        RequestContext context = createRequestContext( master );
        when( conversationSpi.acquireClient() ).thenReturn( locks );
        ResourceTypes type = ResourceTypes.NODE;
        doThrow( new DeadlockDetectedException( "" ) ).when( locks ).acquireExclusive( LockTracer.NONE, type, 1 );
        master.acquireSharedLock( context, type, 1 );

        ArgumentCaptor<LockResult> captor = ArgumentCaptor.forClass( LockResult.class );
        verify( spi ).packTransactionObligationResponse( MockitoHamcrest.argThat( is( context ) ), captor.capture() );
        assertThat( captor.getValue().getMessage(), is( not( nullValue() ) ) );
    }

    @Test
    public void lockResultMustHaveMessageWhenAcquiringExclusiveLockThrowsIllegalResource()
    {
        MasterImpl.SPI spi = mockedSpi();
        DefaultConversationSPI conversationSpi = mockedConversationSpi();
        Config config = config();
        ConversationManager conversationManager = new ConversationManager( conversationSpi, config );
        conversationManager.start();
        Client locks = mock( Client.class );
        MasterImpl master = new MasterImpl( spi, conversationManager, null, config );

        RequestContext context = createRequestContext( master );
        when( conversationSpi.acquireClient() ).thenReturn( locks );
        ResourceTypes type = ResourceTypes.NODE;
        doThrow( new IllegalResourceException( "" ) ).when( locks ).acquireExclusive( LockTracer.NONE, type, 1 );
        master.acquireExclusiveLock( context, type, 1 );

        ArgumentCaptor<LockResult> captor = ArgumentCaptor.forClass( LockResult.class );
        verify( spi ).packTransactionObligationResponse( MockitoHamcrest.argThat( is( context ) ), captor.capture() );
        assertThat( captor.getValue().getMessage(), is( not( nullValue() ) ) );
    }

    @Test
    public void lockResultMustHaveMessageWhenAcquiringSharedLockThrowsIllegalResource()
    {
        MasterImpl.SPI spi = mockedSpi();
        DefaultConversationSPI conversationSpi = mockedConversationSpi();
        Config config = config();
        ConversationManager conversationManager = new ConversationManager( conversationSpi, config );
        conversationManager.start();
        Client locks = mock( Client.class );
        MasterImpl master = new MasterImpl( spi, conversationManager, null, config );

        RequestContext context = createRequestContext( master );
        when( conversationSpi.acquireClient() ).thenReturn( locks );
        ResourceTypes type = ResourceTypes.NODE;
        doThrow( new IllegalResourceException( "" ) ).when( locks ).acquireExclusive( LockTracer.NONE, type, 1 );
        master.acquireSharedLock( context, type, 1 );

        ArgumentCaptor<LockResult> captor = ArgumentCaptor.forClass( LockResult.class );
        verify( spi ).packTransactionObligationResponse( MockitoHamcrest.argThat( is( context ) ), captor.capture() );
        assertThat( captor.getValue().getMessage(), is( not( nullValue() ) ) );
    }

    @Rule
    public final OtherThreadRule<Void> otherThread = new OtherThreadRule<>();

    private Config config()
    {
        return Config.defaults( stringMap(
                HaSettings.lock_read_timeout.name(), 20 + "s",
                ClusterSettings.server_id.name(), "1" ) );
    }

    public DefaultConversationSPI mockedConversationSpi()
    {
        return mock( DefaultConversationSPI.class );
    }

    public static SPI mockedSpi()
    {
        return mockedSpi( StoreId.DEFAULT );
    }

    public static SPI mockedSpi( final StoreId storeId )
    {
        MasterImpl.SPI mock = mock( MasterImpl.SPI.class );
        when( mock.storeId() ).thenReturn( storeId );
        when( mock.packEmptyResponse( any() ) ).thenAnswer(
                invocation -> new TransactionObligationResponse<>( invocation.getArgument( 0 ), storeId,
                        TransactionIdStore.BASE_TX_ID, ResourceReleaser.NO_OP ) );
        return mock;
    }

    protected RequestContext createRequestContext( MasterImpl master )
    {
        HandshakeResult handshake = master.handshake( 1, newStoreIdForCurrentVersion() ).response();
        return new RequestContext( handshake.epoch(), 1, 2, 0, 0 );
    }
}
