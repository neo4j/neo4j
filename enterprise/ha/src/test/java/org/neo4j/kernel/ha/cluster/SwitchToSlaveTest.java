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
package org.neo4j.kernel.ha.cluster;

import org.junit.Test;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

import java.io.File;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Collections;
import java.util.concurrent.TimeUnit;

import org.neo4j.backup.OnlineBackupKernelExtension;
import org.neo4j.cluster.InstanceId;
import org.neo4j.cluster.member.ClusterMemberAvailability;
import org.neo4j.com.Response;
import org.neo4j.com.monitor.RequestMonitor;
import org.neo4j.com.storecopy.StoreCopyClient;
import org.neo4j.com.storecopy.TransactionCommittingResponseUnpacker;
import org.neo4j.graphdb.DependencyResolver;
import org.neo4j.helpers.CancellationRequest;
import org.neo4j.helpers.collection.Iterables;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.kernel.InternalAbstractGraphDatabase;
import org.neo4j.kernel.NeoStoreDataSource;
import org.neo4j.kernel.StoreLockerLifecycleAdapter;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.extension.KernelExtensionFactory;
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
import org.neo4j.kernel.ha.com.slave.MasterClient;
import org.neo4j.kernel.ha.com.slave.MasterClientResolver;
import org.neo4j.kernel.ha.id.HaIdGeneratorFactory;
import org.neo4j.kernel.impl.index.IndexConfigStore;
import org.neo4j.kernel.impl.store.MismatchingStoreIdException;
import org.neo4j.kernel.impl.store.StoreId;
import org.neo4j.kernel.impl.store.TransactionId;
import org.neo4j.kernel.impl.transaction.log.TransactionIdStore;
import org.neo4j.kernel.impl.transaction.state.DataSourceManager;
import org.neo4j.kernel.impl.util.JobScheduler;
import org.neo4j.kernel.lifecycle.LifeSupport;
import org.neo4j.kernel.lifecycle.Lifecycle;
import org.neo4j.kernel.logging.ConsoleLogger;
import org.neo4j.kernel.logging.DevNullLoggingService;
import org.neo4j.kernel.logging.Logging;
import org.neo4j.kernel.monitoring.ByteCounterMonitor;
import org.neo4j.kernel.monitoring.Monitors;

import static org.junit.Assert.assertNull;
import static org.junit.Assert.fail;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyInt;
import static org.mockito.Matchers.anyLong;
import static org.mockito.Matchers.anyString;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.mockito.Mockito.withSettings;

public class SwitchToSlaveTest
{

    private final StoreId storeId = new StoreId( 1, 2, 3, 4 );
    private final UpdatePuller updatePuller = mockWithLifecycle( SlaveUpdatePuller.class );
    private final PullerFactory pullerFactory = mock( PullerFactory.class );
    private final FileSystemAbstraction fs = mock( FileSystemAbstraction.class );
    private final NeoStoreDataSource neoStoreDataSource = dataSourceMock();
    private final MasterClient masterClient = mock( MasterClient.class );

    @Test
    @SuppressWarnings( "unchecked" )
    public void shouldHandleBranchedStoreWhenMyStoreIdDiffersFromMasterStoreId() throws Throwable
    {
        // Given
        SwitchToSlave switchToSlave = newSwitchToSlaveSpy();

        when( neoStoreDataSource.getStoreId() ).thenReturn( new StoreId( 42, 42, 42, 42 ) );

        // When
        try
        {
            switchToSlave.checkDataConsistency( masterClient, neoStoreDataSource, new URI("cluster://localhost?serverId=1") );
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
        URI masterUri = new URI( "cluster://localhost?serverId=1" );

        MasterClient masterClient = mock( MasterClient.class );
        when( masterClient.handshake( anyLong(), any( StoreId.class ) ) ).thenThrow( new BranchedDataException( "" ) );

        // When
        try
        {
            switchToSlave.checkDataConsistency( masterClient, dataSourceMock(), masterUri );
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
        when( updatePuller.tryPullUpdates()).thenReturn( false );

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
                new UpdatePullerScheduler( updatePuller, jobScheduler, mock( Logging.class ), 10l );

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
        verify( jobScheduler ).scheduleRecurring( eq( JobScheduler.Group.pullUpdates ), any( Runnable.class ),
                eq( 10l ),
                eq( 10l ), eq( TimeUnit.MILLISECONDS ) );
    }

    private URI getLocalhostUri() throws URISyntaxException
    {
        return new URI( "cluster://127.0.0.1?serverId=1" );
    }

    @SuppressWarnings( "unchecked" )
    private SwitchToSlave newSwitchToSlaveSpy()
    {
        Response<HandshakeResult> response = mock( Response.class );
        when( response.response() ).thenReturn( new HandshakeResult( 42, 2 ) );
        when( masterClient.handshake( anyLong(), any( StoreId.class ) ) ).thenReturn( response );
        when( masterClient.getProtocolVersion() ).thenReturn( MasterClient214.PROTOCOL_VERSION );

        MasterClientResolver masterClientResolver = mock( MasterClientResolver.class );
        when( masterClientResolver.instantiate( anyString(), anyInt(), any( Monitors.class ), any( StoreId.class ),
                any( LifeSupport.class ) ) ).thenReturn( masterClient );

        return spy( new SwitchToSlave( ConsoleLogger.DEV_NULL, configMock(), neoStoreDataSource.getDependencyResolver(),
                mock( HaIdGeneratorFactory.class ), new DevNullLoggingService(),
                mock( DelegateInvocationHandler.class ), mock( ClusterMemberAvailability.class ),
                mock(RequestContextFactory.class ), Iterables.<KernelExtensionFactory<?>>empty(), masterClientResolver, updatePuller,
                pullerFactory, ByteCounterMonitor.NULL, mock( RequestMonitor.class ),
                mock( SwitchToSlave.Monitor.class ), new StoreCopyClient.Monitor.Adapter() ) );
    }

    private NeoStoreDataSource dataSourceMock()
    {
        NeoStoreDataSource dataSource = mock( NeoStoreDataSource.class );
        when( dataSource.getStoreId() ).thenReturn( storeId );
        DependencyResolver dependencyResolver = dependencyResolverMock();
        when( dataSource.getDependencyResolver() ).thenReturn( dependencyResolver );
        when( dependencyResolver.resolveDependency( NeoStoreDataSource.class ) ).thenReturn( dataSource );
        return dataSource;
    }

    private Config configMock()
    {
        Config config = mock( Config.class );
        when( config.get( HaSettings.branched_data_policy ) ).thenReturn( mock( BranchedDataPolicy.class ) );
        when( config.get( InternalAbstractGraphDatabase.Configuration.store_dir ) ).thenReturn( mock( File.class ) );
        when( config.get( HaSettings.lock_read_timeout ) ).thenReturn( 42l );
        when( config.get( HaSettings.com_chunk_size ) ).thenReturn( 42l );
        return config;
    }

    private DependencyResolver dependencyResolverMock()
    {
        DependencyResolver resolver = mock( DependencyResolver.class );

        when( resolver.resolveDependency( StoreLockerLifecycleAdapter.class ) ).thenReturn(
                mockWithLifecycle( StoreLockerLifecycleAdapter.class ) );
        when( resolver.resolveDependency( DataSourceManager.class ) ).thenReturn(
                mockWithLifecycle( DataSourceManager.class ) );
        when( resolver.resolveDependency( RequestContextFactory.class ) ).thenReturn(
                mockWithLifecycle( RequestContextFactory.class ) );
        when( resolver.resolveDependency( TransactionCommittingResponseUnpacker.class ) ).thenReturn(
                mockWithLifecycle( TransactionCommittingResponseUnpacker.class ) );
        when( resolver.resolveDependency( IndexConfigStore.class ) ).thenReturn(
                mockWithLifecycle( IndexConfigStore.class ) );
        when( resolver.resolveDependency( OnlineBackupKernelExtension.class ) ).thenReturn(
                mockWithLifecycle( OnlineBackupKernelExtension.class ) );
        when( resolver.resolveDependency( FileSystemAbstraction.class ) ).thenReturn( fs );
        when( resolver.resolveDependency( UpdatePuller.class ) ).thenReturn( updatePuller );

        TransactionIdStore transactionIdStore = mock( TransactionIdStore.class );
        when( transactionIdStore.getLastCommittedTransaction() ).thenReturn( new TransactionId( 42, 42 ) );
        when( transactionIdStore.getLastCommittedTransactionId() ).thenReturn( TransactionIdStore.BASE_TX_ID );
        when( resolver.resolveDependency( TransactionIdStore.class ) ).thenReturn( transactionIdStore );

        ClusterMembers clusterMembers = mock( ClusterMembers.class );
        ClusterMember master = mock( ClusterMember.class );
        when( master.getStoreId() ).thenReturn( storeId );
        when( master.getHARole() ).thenReturn( HighAvailabilityModeSwitcher.MASTER );
        when( master.hasRole( eq( HighAvailabilityModeSwitcher.MASTER ) ) ).thenReturn( true );
        when( master.getInstanceId() ).thenReturn( new InstanceId(1) );
        when( clusterMembers.getMembers() ).thenReturn( Collections.singleton( master ) );
        when( resolver.resolveDependency( ClusterMembers.class ) ).thenReturn( clusterMembers );

        return resolver;
    }

    private <T> T mockWithLifecycle( Class<T> clazz )
    {
        return mock( clazz, withSettings().extraInterfaces( Lifecycle.class ) );
    }
}
