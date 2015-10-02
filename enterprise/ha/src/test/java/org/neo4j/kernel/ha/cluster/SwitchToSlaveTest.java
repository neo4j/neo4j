/*
 * Copyright (c) 2002-2015 "Neo Technology,"
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
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;
import org.mockito.verification.VerificationMode;

import java.io.File;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.TimeUnit;

import org.neo4j.backup.OnlineBackupKernelExtension;
import org.neo4j.cluster.ClusterSettings;
import org.neo4j.cluster.InstanceId;
import org.neo4j.cluster.member.ClusterMemberAvailability;
import org.neo4j.com.Response;
import org.neo4j.com.storecopy.StoreCopyClient;
import org.neo4j.com.storecopy.TransactionCommittingResponseUnpacker;
import org.neo4j.com.storecopy.TransactionObligationFulfiller;
import org.neo4j.function.Function;
import org.neo4j.function.Suppliers;
import org.neo4j.graphdb.DependencyResolver;
import org.neo4j.helpers.CancellationRequest;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.io.pagecache.PagedFile;
import org.neo4j.kernel.NeoStoreDataSource;
import org.neo4j.kernel.StoreLockerLifecycleAdapter;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.ha.BranchedDataException;
import org.neo4j.kernel.ha.BranchedDataPolicy;
import org.neo4j.kernel.ha.DelegateInvocationHandler;
import org.neo4j.kernel.ha.HaSettings;
import org.neo4j.kernel.ha.MasterClient214;
import org.neo4j.kernel.ha.PullerFactory;
import org.neo4j.kernel.ha.SlaveUpdatePuller;
import org.neo4j.kernel.ha.UpdatePuller;
import org.neo4j.kernel.ha.UpdatePullerScheduler;
import org.neo4j.kernel.ha.cluster.member.ClusterMember;
import org.neo4j.kernel.ha.cluster.member.ClusterMembers;
import org.neo4j.kernel.ha.com.RequestContextFactory;
import org.neo4j.kernel.ha.com.master.HandshakeResult;
import org.neo4j.kernel.ha.com.master.Slave;
import org.neo4j.kernel.ha.com.slave.MasterClient;
import org.neo4j.kernel.ha.com.slave.MasterClientResolver;
import org.neo4j.kernel.ha.com.slave.SlaveServer;
import org.neo4j.kernel.ha.id.HaIdGeneratorFactory;
import org.neo4j.kernel.impl.index.IndexConfigStore;
import org.neo4j.kernel.impl.logging.NullLogService;
import org.neo4j.kernel.impl.store.MismatchingStoreIdException;
import org.neo4j.kernel.impl.store.StoreId;
import org.neo4j.kernel.impl.transaction.TransactionCounters;
import org.neo4j.kernel.impl.transaction.log.TransactionIdStore;
import org.neo4j.kernel.impl.transaction.state.DataSourceManager;
import org.neo4j.kernel.impl.util.JobScheduler;
import org.neo4j.kernel.lifecycle.LifeSupport;
import org.neo4j.kernel.lifecycle.Lifecycle;
import org.neo4j.kernel.monitoring.Monitors;
import org.neo4j.logging.NullLogProvider;

import static org.junit.Assert.assertNotNull;
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
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class SwitchToSlaveTest
{
    private final UpdatePuller updatePuller = mock( SlaveUpdatePuller.class );
    private final PullerFactory pullerFactory = mock( PullerFactory.class );
    private final FileSystemAbstraction fs = mock( FileSystemAbstraction.class );
    private final MasterClient masterClient = mock( MasterClient.class );
    private final RequestContextFactory requestContextFactory = mock( RequestContextFactory.class );

    private final StoreLockerLifecycleAdapter storeLockerLifecycleAdapter = mock( StoreLockerLifecycleAdapter.class );
    private final OnlineBackupKernelExtension onlineBackupKernelExtension = mock( OnlineBackupKernelExtension.class );
    private final DataSourceManager dataSourceManager = mock( DataSourceManager.class );
    private final TransactionCommittingResponseUnpacker transactionCommittingResponseUnpacker = mock(
            TransactionCommittingResponseUnpacker.class );
    private final IndexConfigStore indexConfigStore = mock( IndexConfigStore.class );

    private final List<? extends Lifecycle> servicesUsedBySwitchToSlave = Arrays.asList(
            storeLockerLifecycleAdapter,
            dataSourceManager,
            transactionCommittingResponseUnpacker,
            indexConfigStore,
            onlineBackupKernelExtension );

    @Test
    public void shouldNotRestartRequestContextFactory() throws Throwable
    {
        // Given
        SwitchToSlave switchToSlave = newSwitchToSlaveSpy();
        when( updatePuller.tryPullUpdates() ).thenReturn( true );

        // When
        URI masterUri = switchToSlave.switchToSlave( mock( LifeSupport.class ), getLocalhostUri(), getLocalhostUri(),
                CancellationRequest.NEVER_CANCELLED );

        // Then
        assertNotNull( "Switch to slave failed, master URI is null", masterUri );
        verify( requestContextFactory, never() ).start();
        verify( requestContextFactory, never() ).stop();
    }

    @Test
    public void shouldRestartServicesIfCopyStoreFails() throws Throwable
    {
        when( updatePuller.tryPullUpdates() ).thenReturn( true );

        PageCache pageCacheMock = mock( PageCache.class );
        PagedFile pagedFileMock = mock( PagedFile.class );
        when( pagedFileMock.getLastPageId() ).thenReturn( 1L );
        when( pageCacheMock.map( any( File.class ), anyInt() ) ).thenThrow( new IOException() )
                .thenThrow( new IOException() ).thenReturn( pagedFileMock );

        StoreCopyClient storeCopyClient = mock( StoreCopyClient.class );
        doThrow( new RuntimeException() ).doNothing().when( storeCopyClient )
                .copyStore( any( StoreCopyClient.StoreCopyRequester.class ), any( CancellationRequest.class ) );

        SwitchToSlave switchToSlave = newSwitchToSlaveSpy( pageCacheMock, storeCopyClient );

        URI localhost = getLocalhostUri();
        try
        {
            switchToSlave.switchToSlave( mock( LifeSupport.class ), localhost, localhost,
                    mock( CancellationRequest.class ) );
            fail( "Should have thrown an Exception" );
        }
        catch ( RuntimeException e )
        {
            verifyServicesStarted( never() );
            // Store should have been deleted due to failure in copy
            verify( switchToSlave ).cleanStoreDir();

            // Try again, should succeed
            switchToSlave.switchToSlave( mock( LifeSupport.class ), localhost, localhost,
                    mock( CancellationRequest.class ) );
            verifyServicesStarted( times( 1 ) );
        }
    }

    @Test
    @SuppressWarnings( "unchecked" )
    public void shouldHandleBranchedStoreWhenMyStoreIdDiffersFromMasterStoreId() throws Throwable
    {
        // Given
        SwitchToSlave switchToSlave = newSwitchToSlaveSpy();

        MasterClient masterClient = mock( MasterClient.class );
        Response<HandshakeResult> response = mock( Response.class );
        when( response.response() ).thenReturn( new HandshakeResult( 1, 2 ) );
        when( masterClient.handshake( anyLong(), any( StoreId.class ) ) ).thenReturn( response );

        StoreId storeId = new StoreId( 1, 2, 3, 4 );

        TransactionIdStore transactionIdStore = mock( TransactionIdStore.class );
        when( transactionIdStore.getLastCommittedTransaction() ).thenReturn( new long[]{42, 42} );
        when( transactionIdStore.getLastCommittedTransactionId() ).thenReturn( TransactionIdStore.BASE_TX_ID );

        // When
        try
        {
            switchToSlave.checkDataConsistency( masterClient, transactionIdStore, storeId,
                    new URI( "cluster://localhost?serverId=1" ), false );
            fail( "Should have thrown " + MismatchingStoreIdException.class.getSimpleName() + " exception" );
        }
        catch ( MismatchingStoreIdException e )
        {
            // good we got the expected exception
        }

        // Then
        verify( switchToSlave ).stopServicesAndHandleBranchedStore( any( BranchedDataPolicy.class ) );
    }

    @Test
    public void shouldHandleBranchedStoreWhenHandshakeFailsWithBranchedDataException() throws Throwable
    {
        // Given
        SwitchToSlave switchToSlave = newSwitchToSlaveSpy();

        MasterClient masterClient = mock( MasterClient.class );
        when( masterClient.handshake( anyLong(), any( StoreId.class ) ) ).thenThrow( new BranchedDataException( "" ) );

        TransactionIdStore transactionIdStore = mock( TransactionIdStore.class );
        when( transactionIdStore.getLastCommittedTransaction() ).thenReturn( new long[]{42, 42} );
        when( transactionIdStore.getLastCommittedTransactionId() ).thenReturn( TransactionIdStore.BASE_TX_ID );

        // When
        try
        {
            switchToSlave.checkDataConsistency( masterClient, transactionIdStore, null, null, true );
            fail( "Should have thrown " + BranchedDataException.class.getSimpleName() + " exception" );
        }
        catch ( BranchedDataException e )
        {
            // good we got the expected exception
        }

        // Then
        verify( switchToSlave ).stopServicesAndHandleBranchedStore( any( BranchedDataPolicy.class ) );
    }

    @Test
    public void shouldReturnNullIfWhenFailingToPullingUpdatesFromMaster() throws Throwable
    {
        // Given
        SwitchToSlave switchToSlave = newSwitchToSlaveSpy();

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
        SwitchToSlave switchToSlave = newSwitchToSlaveSpy();

        when( fs.fileExists( any( File.class ) ) ).thenReturn( true );
        JobScheduler jobScheduler = mock( JobScheduler.class );
        LifeSupport communicationLife = mock( LifeSupport.class );
        URI localhost = getLocalhostUri();
        final UpdatePullerScheduler pullerScheduler =
                new UpdatePullerScheduler( jobScheduler, NullLogProvider.getInstance(), updatePuller, 10l );

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
                eq( 10l ), eq( 10l ), eq( TimeUnit.MILLISECONDS ) );
    }

    private URI getLocalhostUri() throws URISyntaxException
    {
        return new URI( "cluster://127.0.0.1?serverId=1" );
    }

    private SwitchToSlave newSwitchToSlaveSpy() throws IOException
    {
        PageCache pageCacheMock = mock( PageCache.class );
        PagedFile pagedFileMock = mock( PagedFile.class );
        when( pagedFileMock.getLastPageId() ).thenReturn( 1l );
        when( pageCacheMock.map( any( File.class ), anyInt() ) ).thenReturn( pagedFileMock );

        return newSwitchToSlaveSpy( pageCacheMock, mock( StoreCopyClient.class ) );
    }

    @SuppressWarnings( "unchecked" )
    private SwitchToSlave newSwitchToSlaveSpy( PageCache pageCacheMock, StoreCopyClient storeCopyClient )
            throws IOException
    {
        ClusterMembers clusterMembers = clusterMembersMock();

        NeoStoreDataSource dataSource = mock( NeoStoreDataSource.class );
        when( dataSource.getStoreId() ).thenReturn( new StoreId( 42, 42, 42, 42 ) );

        TransactionCounters transactionCounters = mock( TransactionCounters.class );
        when( transactionCounters.getNumberOfActiveTransactions() ).thenReturn( 0L );

        Response<HandshakeResult> response = mock( Response.class );
        when( response.response() ).thenReturn( new HandshakeResult( 42, 2 ) );
        when( masterClient.handshake( anyLong(), any( StoreId.class ) ) ).thenReturn( response );
        when( masterClient.getProtocolVersion() ).thenReturn( MasterClient214.PROTOCOL_VERSION );

        TransactionIdStore transactionIdStoreMock = mock( TransactionIdStore.class );
        // note that the checksum (the second member of the array) is the same as the one in the handshake mock above
        when( transactionIdStoreMock.getLastCommittedTransaction() ).thenReturn( new long[]{42, 42} );

        MasterClientResolver masterClientResolver = mock( MasterClientResolver.class );
        when( masterClientResolver.instantiate( anyString(), anyInt(), any( Monitors.class ), any( StoreId.class ),
                any( LifeSupport.class ) ) ).thenReturn( masterClient );

        return spy( new SwitchToSlave( new File( "" ), NullLogService.getInstance(),
                clusterMembers,
                configMock(),
                dependencyResolverMock( clusterMembers ),
                mock( HaIdGeneratorFactory.class ),
                mock( DelegateInvocationHandler.class ),
                mock( ClusterMemberAvailability.class ),
                requestContextFactory,
                pullerFactory, masterClientResolver,
                mock( SwitchToSlave.Monitor.class ),
                storeCopyClient,
                Suppliers.singleton( dataSource ),
                Suppliers.singleton( transactionIdStoreMock ),
                slaveServerFactory(),
                updatePuller,
                pageCacheMock,
                mock( Monitors.class ),
                transactionCounters ) );
    }

    private static ClusterMembers clusterMembersMock()
    {
        ClusterMembers clusterMembers = mock( ClusterMembers.class );
        ClusterMember master = mock( ClusterMember.class );
        when( master.getStoreId() ).thenReturn( new StoreId( 42, 42, 42, 42 ) );
        when( master.getHARole() ).thenReturn( HighAvailabilityModeSwitcher.MASTER );
        when( master.hasRole( eq( HighAvailabilityModeSwitcher.MASTER ) ) ).thenReturn( true );
        when( master.getInstanceId() ).thenReturn( new InstanceId( 1 ) );
        when( clusterMembers.getMembers() ).thenReturn( Collections.singletonList( master ) );
        return clusterMembers;
    }

    private DependencyResolver dependencyResolverMock( ClusterMembers clusterMembers )
    {
        DependencyResolver resolver = mock( DependencyResolver.class );

        when( resolver.resolveDependency( ClusterMembers.class ) ).thenReturn( clusterMembers );
        when( resolver.resolveDependency( TransactionObligationFulfiller.class ) )
                .thenReturn( mock( TransactionObligationFulfiller.class ) );

        when( resolver.resolveDependency( StoreLockerLifecycleAdapter.class ) )
                .thenReturn( storeLockerLifecycleAdapter );
        when( resolver.resolveDependency( OnlineBackupKernelExtension.class ) )
                .thenReturn( onlineBackupKernelExtension );
        when( resolver.resolveDependency( TransactionCommittingResponseUnpacker.class ) )
                .thenReturn( transactionCommittingResponseUnpacker );
        when( resolver.resolveDependency( IndexConfigStore.class ) ).thenReturn( indexConfigStore );
        when( resolver.resolveDependency( DataSourceManager.class ) ).thenReturn( dataSourceManager );

        return resolver;
    }

    private static Config configMock()
    {
        Config config = mock( Config.class );
        when( config.get( HaSettings.branched_data_policy ) ).thenReturn( mock( BranchedDataPolicy.class ) );
        when( config.get( HaSettings.lock_read_timeout ) ).thenReturn( 42L );
        when( config.get( HaSettings.com_chunk_size ) ).thenReturn( 42L );
        when( config.get( HaSettings.state_switch_timeout ) ).thenReturn( 42L );
        when( config.get( ClusterSettings.server_id ) ).thenReturn( new InstanceId( 42 ) );
        return config;
    }

    private static Function<Slave,SlaveServer> slaveServerFactory()
    {
        return new Function<Slave,SlaveServer>()
        {
            @Override
            public SlaveServer apply( Slave slave ) throws RuntimeException
            {
                SlaveServer server = mock( SlaveServer.class );
                InetSocketAddress inetSocketAddress = InetSocketAddress.createUnresolved( "localhost", 42 );
                when( server.getSocketAddress() ).thenReturn( inetSocketAddress );
                return server;
            }
        };
    }

    private void verifyServicesStarted( VerificationMode mode ) throws Throwable
    {
        for ( Lifecycle service : servicesUsedBySwitchToSlave )
        {
            verify( service, mode ).start();
        }
    }
}
