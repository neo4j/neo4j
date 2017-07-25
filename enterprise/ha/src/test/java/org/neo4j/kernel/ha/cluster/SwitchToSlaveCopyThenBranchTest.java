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
package org.neo4j.kernel.ha.cluster;

import org.junit.Test;
import org.mockito.InOrder;
import org.mockito.Mockito;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

import java.io.File;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.concurrent.TimeUnit;
import java.util.function.Predicate;
import java.util.stream.Stream;

import org.neo4j.backup.OnlineBackupKernelExtension;
import org.neo4j.cluster.ClusterSettings;
import org.neo4j.cluster.InstanceId;
import org.neo4j.cluster.member.ClusterMemberAvailability;
import org.neo4j.com.Response;
import org.neo4j.com.storecopy.MoveAfterCopy;
import org.neo4j.com.storecopy.StoreCopyClient;
import org.neo4j.com.storecopy.TransactionCommittingResponseUnpacker;
import org.neo4j.com.storecopy.TransactionObligationFulfiller;
import org.neo4j.function.Suppliers;
import org.neo4j.helpers.CancellationRequest;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.io.pagecache.PagedFile;
import org.neo4j.kernel.NeoStoreDataSource;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.ha.BranchedDataException;
import org.neo4j.kernel.ha.BranchedDataPolicy;
import org.neo4j.kernel.ha.DelegateInvocationHandler;
import org.neo4j.kernel.ha.PullerFactory;
import org.neo4j.kernel.ha.SlaveUpdatePuller;
import org.neo4j.kernel.ha.UpdatePuller;
import org.neo4j.kernel.ha.UpdatePullerScheduler;
import org.neo4j.kernel.ha.cluster.member.ClusterMember;
import org.neo4j.kernel.ha.cluster.member.ClusterMembers;
import org.neo4j.kernel.ha.cluster.modeswitch.HighAvailabilityModeSwitcher;
import org.neo4j.kernel.ha.com.RequestContextFactory;
import org.neo4j.kernel.ha.com.master.HandshakeResult;
import org.neo4j.kernel.ha.com.slave.MasterClient;
import org.neo4j.kernel.ha.com.slave.MasterClientResolver;
import org.neo4j.kernel.ha.com.slave.SlaveServer;
import org.neo4j.kernel.ha.id.HaIdGeneratorFactory;
import org.neo4j.kernel.impl.index.IndexConfigStore;
import org.neo4j.kernel.impl.logging.NullLogService;
import org.neo4j.kernel.impl.store.MismatchingStoreIdException;
import org.neo4j.kernel.impl.store.StoreId;
import org.neo4j.kernel.impl.store.TransactionId;
import org.neo4j.kernel.impl.transaction.TransactionStats;
import org.neo4j.kernel.impl.transaction.log.TransactionIdStore;
import org.neo4j.kernel.impl.transaction.state.DataSourceManager;
import org.neo4j.kernel.impl.util.Dependencies;
import org.neo4j.kernel.impl.util.JobScheduler;
import org.neo4j.kernel.impl.util.watcher.FileSystemWatcherService;
import org.neo4j.kernel.internal.StoreLockerLifecycleAdapter;
import org.neo4j.kernel.lifecycle.LifeSupport;
import org.neo4j.kernel.lifecycle.Lifecycle;
import org.neo4j.kernel.monitoring.Monitors;
import org.neo4j.logging.NullLogProvider;

import static java.util.Arrays.asList;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.fail;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyInt;
import static org.mockito.Matchers.anyLong;
import static org.mockito.Matchers.anyString;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.mockito.Mockito.withSettings;
import static org.neo4j.com.StoreIdTestFactory.newStoreIdForCurrentVersion;
import static org.neo4j.helpers.collection.MapUtil.stringMap;

public class SwitchToSlaveCopyThenBranchTest
{
    private final UpdatePuller updatePuller = mockWithLifecycle( SlaveUpdatePuller.class );
    private final PullerFactory pullerFactory = mock( PullerFactory.class );
    private final FileSystemAbstraction fs = mock( FileSystemAbstraction.class );
    private final MasterClient masterClient = mock( MasterClient.class );
    private final RequestContextFactory requestContextFactory = mock( RequestContextFactory.class );
    private final StoreId storeId = newStoreIdForCurrentVersion( 42, 42 );

    @Test
    public void shouldRestartServicesIfCopyStoreFails() throws Throwable
    {
        when( updatePuller.tryPullUpdates() ).thenReturn( true );

        PageCache pageCacheMock = mockPageCache();

        StoreCopyClient storeCopyClient = mock( StoreCopyClient.class );

        doThrow( new RuntimeException() ).doNothing().when( storeCopyClient ).copyStore(
                any( StoreCopyClient.StoreCopyRequester.class ),
                any( CancellationRequest.class ),
                any( MoveAfterCopy.class ) );

        SwitchToSlaveCopyThenBranch switchToSlave = newSwitchToSlaveSpy( pageCacheMock, storeCopyClient );

        URI localhost = getLocalhostUri();
        try
        {
            switchToSlave.switchToSlave( mock( LifeSupport.class ), localhost, localhost,
                    mock( CancellationRequest.class ) );
            fail( "Should have thrown an Exception" );
        }
        catch ( RuntimeException e )
        {
            verify( requestContextFactory, never() ).start();
            // Store should have been deleted due to failure in copy
            verify( switchToSlave ).cleanStoreDir();

            // Try again, should succeed
            switchToSlave.switchToSlave( mock( LifeSupport.class ), localhost, localhost,
                    mock( CancellationRequest.class ) );
            verify( requestContextFactory ).start();
        }
    }

    private PageCache mockPageCache() throws IOException
    {
        PageCache pageCacheMock = mock( PageCache.class );
        PagedFile pagedFileMock = mock( PagedFile.class );
        when( pagedFileMock.getLastPageId() ).thenReturn( 1L );
        when( pageCacheMock.map( any( File.class ), anyInt() ) ).thenThrow( new IOException() )
                .thenThrow( new IOException() ).thenReturn( pagedFileMock );
        return pageCacheMock;
    }

    @Test
    @SuppressWarnings( "unchecked" )
    public void shouldHandleBranchedStoreWhenMyStoreIdDiffersFromMasterStoreId() throws Throwable
    {
        // Given
        SwitchToSlaveCopyThenBranch switchToSlave = newSwitchToSlaveSpy();
        URI me = new URI( "cluster://localhost?serverId=2" );

        MasterClient masterClient = mock( MasterClient.class );
        Response<HandshakeResult> response = mock( Response.class );
        when( response.response() ).thenReturn( new HandshakeResult( 1, 2 ) );
        when( masterClient.handshake( anyLong(), any( StoreId.class ) ) ).thenReturn( response );

        StoreId storeId = newStoreIdForCurrentVersion( 1, 2 );

        TransactionIdStore transactionIdStore = mock( TransactionIdStore.class );
        when( transactionIdStore.getLastCommittedTransaction() ).thenReturn( new TransactionId( 42, 42, 42 ) );
        when( transactionIdStore.getLastCommittedTransactionId() ).thenReturn( TransactionIdStore.BASE_TX_ID );

        // When
        try
        {
            switchToSlave.checkDataConsistency( masterClient, transactionIdStore, storeId, new URI("cluster://localhost?serverId=1"), me, CancellationRequest.NEVER_CANCELLED );
            fail( "Should have thrown " + MismatchingStoreIdException.class.getSimpleName() + " exception" );
        }
        catch ( MismatchingStoreIdException e )
        {
            // good we got the expected exception
        }

        // Then
        verify( switchToSlave ).stopServicesAndHandleBranchedStore( any( BranchedDataPolicy.class ), any(URI.class), any(URI.class), any(CancellationRequest.class) );
    }

    @Test
    public void shouldHandleBranchedStoreWhenHandshakeFailsWithBranchedDataException() throws Throwable
    {
        // Given
        SwitchToSlaveCopyThenBranch switchToSlave = newSwitchToSlaveSpy();
        URI masterUri = new URI( "cluster://localhost?serverId=1" );
        URI me = new URI( "cluster://localhost?serverId=2" );

        MasterClient masterClient = mock( MasterClient.class );
        when( masterClient.handshake( anyLong(), any( StoreId.class ) ) ).thenThrow( new BranchedDataException( "" ) );

        TransactionIdStore transactionIdStore = mock( TransactionIdStore.class );
        when( transactionIdStore.getLastCommittedTransaction() ).thenReturn( new TransactionId( 42, 42, 42 ) );
        when( transactionIdStore.getLastCommittedTransactionId() ).thenReturn( TransactionIdStore.BASE_TX_ID );

        // When
        try
        {
            switchToSlave.checkDataConsistency( masterClient, transactionIdStore, storeId, masterUri, me,
                    CancellationRequest.NEVER_CANCELLED );
            fail( "Should have thrown " + BranchedDataException.class.getSimpleName() + " exception" );
        }
        catch ( BranchedDataException e )
        {
            // good we got the expected exception
        }

        // Then
        verify( switchToSlave ).stopServicesAndHandleBranchedStore( any( BranchedDataPolicy.class ), any(URI.class), any(URI.class), any(CancellationRequest.class) );
    }

    @Test
    public void shouldNotBranchStoreUnlessWeHaveCopiedDownAReplacement() throws Throwable
    {
        // Given
        StoreCopyClient storeCopyClient = mock( StoreCopyClient.class );
        doAnswer( invocation ->
        {
            MoveAfterCopy moveAfterCopy = invocation.getArgumentAt( 2, MoveAfterCopy.class );
            moveAfterCopy.move( Stream.empty(), new File( "" ), new File( "" ) );
            return null;
        } ).when( storeCopyClient ).copyStore(
                any( StoreCopyClient.StoreCopyRequester.class ),
                any( CancellationRequest.class ),
                any( MoveAfterCopy.class ) );

        PageCache pageCacheMock = mock( PageCache.class );
        PagedFile pagedFileMock = mock( PagedFile.class );
        when( pagedFileMock.getLastPageId() ).thenReturn( 1L );
        when( pageCacheMock.map( any( File.class ), anyInt() ) ).thenReturn( pagedFileMock );

        SwitchToSlaveCopyThenBranch switchToSlave = newSwitchToSlaveSpy( pageCacheMock, storeCopyClient );

        URI masterUri = new URI( "cluster://localhost?serverId=1" );
        URI me = new URI( "cluster://localhost?serverId=2" );
        CancellationRequest cancellationRequest = CancellationRequest.NEVER_CANCELLED;

        MasterClient masterClient = mock( MasterClient.class );
        when( masterClient.handshake( anyLong(), any( StoreId.class ) ) ).thenThrow( new BranchedDataException( "" ) );

        TransactionIdStore transactionIdStore = mock( TransactionIdStore.class );
        when( transactionIdStore.getLastCommittedTransaction() ).thenReturn( new TransactionId( 42, 42, 42 ) );
        when( transactionIdStore.getLastCommittedTransactionId() ).thenReturn( TransactionIdStore.BASE_TX_ID );

        // When
        BranchedDataPolicy branchPolicy = mock( BranchedDataPolicy.class );
        switchToSlave.stopServicesAndHandleBranchedStore( branchPolicy, masterUri, me, cancellationRequest );

        // Then
        InOrder inOrder = Mockito.inOrder( storeCopyClient, branchPolicy );

        inOrder.verify( storeCopyClient ).copyStore( any( StoreCopyClient.StoreCopyRequester.class ),
                any( CancellationRequest.class ), any( MoveAfterCopy.class ) ) ;
        inOrder.verify( branchPolicy ).handle( new File(""), pageCacheMock, NullLogService.getInstance() );
    }

    @Test
    public void shouldReturnNullIfWhenFailingToPullingUpdatesFromMaster() throws Throwable
    {
        // Given
        SwitchToSlaveCopyThenBranch switchToSlave = newSwitchToSlaveSpy();

        when( fs.fileExists( any( File.class ) ) ).thenReturn( true );
        when( updatePuller.tryPullUpdates() ).thenReturn( false );

        // when
        URI localhost = getLocalhostUri();
        URI uri = switchToSlave.switchToSlave( mock( LifeSupport.class ), localhost, localhost,
                mock( CancellationRequest.class ) );

        // then
        assertNull( uri );
    }

    @Test
    public void updatesPulledAndPullingScheduledOnSwitchToSlave() throws Throwable
    {
        SwitchToSlaveCopyThenBranch switchToSlave = newSwitchToSlaveSpy();

        when( fs.fileExists( any( File.class ) ) ).thenReturn( true );
        JobScheduler jobScheduler = mock( JobScheduler.class );
        LifeSupport communicationLife = mock( LifeSupport.class );
        URI localhost = getLocalhostUri();
        final UpdatePullerScheduler pullerScheduler =
                new UpdatePullerScheduler( jobScheduler, NullLogProvider.getInstance(), updatePuller, 10L );

        when( pullerFactory.createUpdatePullerScheduler( updatePuller ) ).thenReturn( pullerScheduler );
        // emulate lifecycle start call on scheduler
        doAnswer( new Answer()
        {
            @Override
            public Object answer( InvocationOnMock invocationOnMock ) throws Throwable
            {
                pullerScheduler.init();
                return null;
            }
        } ).when( communicationLife ).start();

        switchToSlave.switchToSlave( communicationLife, localhost, localhost, mock( CancellationRequest.class ) );

        verify( updatePuller ).tryPullUpdates();
        verify( communicationLife ).add( pullerScheduler );
        verify( jobScheduler ).scheduleRecurring( eq( JobScheduler.Groups.pullUpdates ), any( Runnable.class ),
                eq( 10L ), eq( 10L ), eq( TimeUnit.MILLISECONDS ) );
    }

    private URI getLocalhostUri() throws URISyntaxException
    {
        return new URI( "cluster://127.0.0.1?serverId=1" );
    }

    private SwitchToSlaveCopyThenBranch newSwitchToSlaveSpy() throws Exception
    {
        PageCache pageCacheMock = mock( PageCache.class );
        PagedFile pagedFileMock = mock( PagedFile.class );
        when( pagedFileMock.getLastPageId() ).thenReturn( 1L );
        when( pageCacheMock.map( any( File.class ), anyInt() ) ).thenReturn( pagedFileMock );

        StoreCopyClient storeCopyClient = mock( StoreCopyClient.class );
        Stream mockStream = mock( Stream.class );
        when( mockStream.filter( any( Predicate.class ) ) ).thenReturn( mock( Stream.class ) );
        when( pageCacheMock.streamFilesRecursive( any( File.class) ) ).thenReturn( mockStream );

        return newSwitchToSlaveSpy( pageCacheMock, storeCopyClient );
    }

    @SuppressWarnings( "unchecked" )
    private SwitchToSlaveCopyThenBranch newSwitchToSlaveSpy( PageCache pageCacheMock, StoreCopyClient storeCopyClient )
            throws IOException
    {
        ClusterMembers clusterMembers = mock( ClusterMembers.class );
        ClusterMember master = mock( ClusterMember.class );
        when( master.getStoreId() ).thenReturn( storeId );
        when( master.getHARole() ).thenReturn( HighAvailabilityModeSwitcher.MASTER );
        when( master.hasRole( eq( HighAvailabilityModeSwitcher.MASTER ) ) ).thenReturn( true );
        when( master.getInstanceId() ).thenReturn( new InstanceId( 1 ) );
        when( clusterMembers.getMembers() ).thenReturn( asList( master ) );

        Dependencies resolver = new Dependencies();
        resolver.satisfyDependencies( requestContextFactory, clusterMembers,
                mock( TransactionObligationFulfiller.class ),
                mock( OnlineBackupKernelExtension.class ),
                mock( IndexConfigStore.class ),
                mock( TransactionCommittingResponseUnpacker.class ),
                mock( DataSourceManager.class ),
                mock( StoreLockerLifecycleAdapter.class ),
                mock( FileSystemWatcherService.class )
                );

        NeoStoreDataSource dataSource = mock( NeoStoreDataSource.class );
        when( dataSource.getStoreId() ).thenReturn( storeId );

        TransactionStats transactionCounters = mock( TransactionStats.class );
        when( transactionCounters.getNumberOfActiveTransactions() ).thenReturn( 0L );

        Response<HandshakeResult> response = mock( Response.class );
        when( response.response() ).thenReturn( new HandshakeResult( 42, 2 ) );
        when( masterClient.handshake( anyLong(), any( StoreId.class ) ) ).thenReturn( response );
        when( masterClient.getProtocolVersion() ).thenReturn( MasterClient.CURRENT );

        TransactionIdStore transactionIdStoreMock = mock( TransactionIdStore.class );
        // note that the checksum (the second member of the array) is the same as the one in the handshake mock above
        when( transactionIdStoreMock.getLastCommittedTransaction() ).thenReturn( new TransactionId( 42, 42, 42 ) );

        MasterClientResolver masterClientResolver = mock( MasterClientResolver.class );
        when( masterClientResolver.instantiate( anyString(), anyInt(), anyString(), any( Monitors.class ),
                any( StoreId.class ), any( LifeSupport.class ) ) ).thenReturn( masterClient );

        return spy( new SwitchToSlaveCopyThenBranch( new File( "" ), NullLogService.getInstance(),
                configMock(), resolver,
                mock( HaIdGeneratorFactory.class ),
                mock( DelegateInvocationHandler.class ),
                mock( ClusterMemberAvailability.class ),
                requestContextFactory,
                pullerFactory, masterClientResolver,
                mock( SwitchToSlave.Monitor.class ),
                storeCopyClient,
                Suppliers.singleton( dataSource ),
                Suppliers.singleton( transactionIdStoreMock ),
                slave ->
                {
                    SlaveServer server = mock( SlaveServer.class );
                    InetSocketAddress inetSocketAddress = InetSocketAddress.createUnresolved( "localhost", 42 );

                    when( server.getSocketAddress() ).thenReturn( inetSocketAddress );
                    return server;
                }, updatePuller, pageCacheMock, mock( Monitors.class ), transactionCounters ) );
    }

    private Config configMock()
    {
        return Config.embeddedDefaults( stringMap( ClusterSettings.server_id.name(), "1" ) );
    }

    private <T> T mockWithLifecycle( Class<T> clazz )
    {
        return mock( clazz, withSettings().extraInterfaces( Lifecycle.class ) );
    }
}
