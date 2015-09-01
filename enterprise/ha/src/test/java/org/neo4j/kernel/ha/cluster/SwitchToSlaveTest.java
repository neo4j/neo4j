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

import java.io.File;
import java.io.IOException;
import java.net.URI;

import org.junit.Ignore;
import org.junit.Test;

import org.neo4j.cluster.member.ClusterMemberAvailability;
import org.neo4j.com.Response;
import org.neo4j.com.storecopy.StoreCopyClient;
import org.neo4j.function.Factory;
import org.neo4j.function.Function;
import org.neo4j.function.Suppliers;
import org.neo4j.graphdb.DependencyResolver;
import org.neo4j.helpers.CancellationRequest;
import org.neo4j.helpers.collection.Iterables;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.io.pagecache.PagedFile;
import org.neo4j.kernel.NeoStoreDataSource;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.extension.KernelExtensionFactory;
import org.neo4j.kernel.ha.BranchedDataException;
import org.neo4j.kernel.ha.BranchedDataPolicy;
import org.neo4j.kernel.ha.DelegateInvocationHandler;
import org.neo4j.kernel.ha.HaSettings;
import org.neo4j.kernel.ha.MasterClient214;
import org.neo4j.kernel.ha.UpdatePuller;
import org.neo4j.kernel.ha.cluster.member.ClusterMember;
import org.neo4j.kernel.ha.cluster.member.ClusterMembers;
import org.neo4j.kernel.ha.com.RequestContextFactory;
import org.neo4j.kernel.ha.com.master.HandshakeResult;
import org.neo4j.kernel.ha.com.master.Slave;
import org.neo4j.kernel.ha.com.slave.MasterClient;
import org.neo4j.kernel.ha.com.slave.MasterClientResolver;
import org.neo4j.kernel.ha.com.slave.SlaveServer;
import org.neo4j.kernel.ha.id.HaIdGeneratorFactory;
import org.neo4j.kernel.impl.logging.NullLogService;
import org.neo4j.kernel.impl.store.MismatchingStoreIdException;
import org.neo4j.kernel.impl.store.StoreId;
import org.neo4j.kernel.impl.transaction.TransactionCounters;
import org.neo4j.kernel.impl.transaction.log.TransactionIdStore;
import org.neo4j.kernel.lifecycle.LifeSupport;
import org.neo4j.kernel.lifecycle.Lifecycle;
import org.neo4j.kernel.monitoring.Monitors;

import static java.util.Arrays.asList;

import static org.junit.Assert.assertNull;
import static org.junit.Assert.fail;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyInt;
import static org.mockito.Matchers.anyLong;
import static org.mockito.Matchers.anyString;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.mockito.Mockito.withSettings;

public class SwitchToSlaveTest
{
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
            switchToSlave.checkDataConsistency( masterClient, transactionIdStore, storeId, null, false );
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
    @Ignore
    public void shouldReturnNullIfWhenFailingToPullingUpdatesFromMaster() throws Throwable
    {
        // Given
        SwitchToSlave switchToSlave = newSwitchToSlaveSpy();

        when( fs.fileExists( any( File.class ) ) ).thenReturn( true );
        when( updatePuller.await( UpdatePuller.NEXT_TICKET, true ) ).thenThrow(
                new IllegalStateException( "this should not happen" )
        );
        when( updatePuller.await( UpdatePuller.NEXT_TICKET, false ) ).thenReturn( false );

        // when
        URI localhost = new URI( "127.0.0.1" );
        URI uri = switchToSlave.switchToSlave( mock( LifeSupport.class ), localhost, localhost,
                mock( CancellationRequest.class ) );

        // then
        assertNull( uri );
    }

    private final UpdatePuller updatePuller = mockWithLifecycle( UpdatePuller.class );
    private final FileSystemAbstraction fs = mock( FileSystemAbstraction.class );
    private final MasterClient masterClient = mock( MasterClient.class );

    @SuppressWarnings( "unchecked" )
    private SwitchToSlave newSwitchToSlaveSpy() throws IOException
    {
        ClusterMembers clusterMembers = mock( ClusterMembers.class );
        ClusterMember master = mock( ClusterMember.class );
        when( master.getStoreId() ).thenReturn( new StoreId( 42, 42, 42, 42 ) );
        when( master.getHARole() ).thenReturn( HighAvailabilityModeSwitcher.MASTER );
        when( master.hasRole( eq( HighAvailabilityModeSwitcher.MASTER ) ) ).thenReturn( true );
        when( clusterMembers.getMembers() ).thenReturn( asList( master ) );

        DependencyResolver resolver = mock( DependencyResolver.class );
        when( resolver.resolveDependency( any( Class.class ) ) ).thenReturn( mock( Lifecycle.class ) );
        when( resolver.resolveDependency( ClusterMembers.class ) ).thenReturn( clusterMembers );

        NeoStoreDataSource dataSource = mock( NeoStoreDataSource.class );
        when( dataSource.getStoreId() ).thenReturn( new StoreId( 42, 42, 42, 42 ) );

        TransactionCounters transactionCounters = mock( TransactionCounters.class );
        when( transactionCounters.getNumberOfActiveTransactions() ).thenReturn( 0l );

        Response<HandshakeResult> response = mock( Response.class );
        when( response.response() ).thenReturn( new HandshakeResult( 42, 2 ) );
        when( masterClient.handshake( anyLong(), any( StoreId.class ) ) ).thenReturn( response );
        when( masterClient.getProtocolVersion() ).thenReturn( MasterClient214.PROTOCOL_VERSION );

        PageCache pageCacheMock = mock( PageCache.class );
        PagedFile pagedFileMock = mock( PagedFile.class );
        when( pagedFileMock.getLastPageId() ).thenReturn( 1l );
        when( pageCacheMock.map( any( File.class ), anyInt() ) ).thenReturn( pagedFileMock );

        TransactionIdStore transactionIdStoreMock = mock( TransactionIdStore.class );
        // note that the checksum (the second member of the array) is the same as the one in the handshake mock above
        when( transactionIdStoreMock.getLastCommittedTransaction() ).thenReturn( new long[] {42, 42} );

        MasterClientResolver masterClientResolver = mock( MasterClientResolver.class );
        when( masterClientResolver.instantiate( anyString(), anyInt(), any( Monitors.class ), any( StoreId.class ),
                any( LifeSupport.class ) ) ).thenReturn( masterClient );

        return spy( new SwitchToSlave( new File(""), NullLogService.getInstance(),
                mock( FileSystemAbstraction.class ),
                clusterMembers,
                configMock(), resolver,
                mock( HaIdGeneratorFactory.class ),
                mock( DelegateInvocationHandler.class ),
                mock( ClusterMemberAvailability.class ), mock( RequestContextFactory.class ),
                Iterables.<KernelExtensionFactory<?>>empty(), masterClientResolver,
                mock( SwitchToSlave.Monitor.class ),
                new StoreCopyClient.Monitor.Adapter(),
                Suppliers.singleton( dataSource ),
                Suppliers.singleton( transactionIdStoreMock ),
                new Factory<Slave>()
                {
                    @Override
                    public Slave newInstance()
                    {
                        return mock( Slave.class );
                    }
                }, new Function<Slave, SlaveServer>()
        {
            @Override
            public SlaveServer apply( Slave slave ) throws RuntimeException
            {
                return mock( SlaveServer.class );
            }
        }, mock( UpdatePuller.class ), pageCacheMock, mock( Monitors.class ), transactionCounters ) );
    }

    private Config configMock()
    {
        Config config = mock( Config.class );
        when( config.get( HaSettings.branched_data_policy ) ).thenReturn( mock( BranchedDataPolicy.class ) );
        when( config.get( HaSettings.lock_read_timeout ) ).thenReturn( 42l );
        when( config.get( HaSettings.com_chunk_size ) ).thenReturn( 42l );
        when( config.get( HaSettings.state_switch_timeout ) ).thenReturn( 42l );
        return config;
    }

    private <T> T mockWithLifecycle( Class<T> clazz )
    {
        return mock( clazz, withSettings().extraInterfaces( Lifecycle.class ) );
    }
}
