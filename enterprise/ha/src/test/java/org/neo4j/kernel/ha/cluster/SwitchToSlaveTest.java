/**
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

import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.net.URI;
import javax.transaction.TransactionManager;

import org.neo4j.cluster.member.ClusterMemberAvailability;
import org.neo4j.graphdb.DependencyResolver;
import org.neo4j.kernel.StoreLockerLifecycleAdapter;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.extension.KernelExtensionFactory;
import org.neo4j.kernel.ha.BranchedDataException;
import org.neo4j.kernel.ha.BranchedDataPolicy;
import org.neo4j.kernel.ha.DelegateInvocationHandler;
import org.neo4j.kernel.ha.HaSettings;
import org.neo4j.kernel.ha.HaXaDataSourceManager;
import org.neo4j.kernel.ha.com.RequestContextFactory;
import org.neo4j.kernel.ha.com.master.Master;
import org.neo4j.kernel.ha.com.slave.MasterClient;
import org.neo4j.kernel.ha.com.slave.MasterClientResolver;
import org.neo4j.kernel.ha.id.HaIdGeneratorFactory;
import org.neo4j.kernel.impl.api.UpdateableSchemaState;
import org.neo4j.kernel.impl.core.NodeManager;
import org.neo4j.kernel.impl.index.IndexStore;
import org.neo4j.kernel.impl.nioneo.xa.NeoStoreXaDataSource;
import org.neo4j.kernel.impl.transaction.XaDataSourceManager;
import org.neo4j.kernel.impl.transaction.xaframework.NoSuchLogVersionException;
import org.neo4j.kernel.lifecycle.Lifecycle;
import org.neo4j.kernel.logging.ConsoleLogger;
import org.neo4j.kernel.logging.DevNullLoggingService;
import org.neo4j.kernel.logging.Logging;
import org.neo4j.kernel.monitoring.Monitors;

import static org.junit.Assert.fail;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.mockito.Mockito.withSettings;

public class SwitchToSlaveTest
{
    private SwitchToSlave switchToSlave;

    @Before
    @SuppressWarnings( "unchecked" )
    public void setUp()
    {
        // create a store with a wrong tx
        ConsoleLogger console = mock( ConsoleLogger.class );
        Config config = configMock();
        DependencyResolver resolver = dependencyResolverMock();
        HaIdGeneratorFactory idGeneratorFactory = mock( HaIdGeneratorFactory.class );
        Logging logging = new DevNullLoggingService();
        DelegateInvocationHandler<Master> masterDelegateHandler = mock( DelegateInvocationHandler.class );
        ClusterMemberAvailability clusterMemberAvailability = mock( ClusterMemberAvailability.class );
        RequestContextFactory requestContextFactory = mock( RequestContextFactory.class );
        UpdateableSchemaState updateableSchemaState = mock( UpdateableSchemaState.class );
        MasterClientResolver masterClientResolver = mock( MasterClientResolver.class );
        Monitors monitors = mock( Monitors.class );
        Iterable<KernelExtensionFactory<?>> kernelExtensions = mock( Iterable.class );

        switchToSlave = spy( new SwitchToSlave( console, config, resolver, idGeneratorFactory, logging,
                masterDelegateHandler, clusterMemberAvailability, requestContextFactory, updateableSchemaState,
                masterClientResolver, monitors, kernelExtensions ) );
    }

    @Test
    public void shouldThrowBranchedDataExceptionWhenFailedToGetMasterForCommittedTx() throws Exception
    {
        // Given
        URI masterUri = null;
        Master master = mock( Master.class );
        NeoStoreXaDataSource nioneoDataSource = mock( NeoStoreXaDataSource.class );

        // When
        long fakeTxId = 1L; // any tx id
        when( nioneoDataSource.getLastCommittedTxId() ).thenReturn( fakeTxId );
        when( nioneoDataSource.getMasterForCommittedTx( fakeTxId ) ).thenThrow( new IOException() );

        // Then
        try
        {
            switchToSlave.checkDataConsistencyWithMaster( masterUri, master, nioneoDataSource );
            fail( "Should have thrown " + BranchedDataException.class.getSimpleName() + " exception" );
        }
        catch ( BranchedDataException e )
        {
            // good we got the expected exception
        }
    }

    @Test
    public void shouldHandleBranchedStoreWhenFailedToGetMasterForCommittedTxWithIOException() throws Throwable
    {
        // Given
        NeoStoreXaDataSource nioneoDataSource = mock( NeoStoreXaDataSource.class );

        // When
        long fakeTxId = 2L; // any tx id
        when( nioneoDataSource.getLastCommittedTxId() ).thenReturn( fakeTxId );
        when( nioneoDataSource.getMasterForCommittedTx( fakeTxId ) ).thenThrow( new IOException() );

        try
        {
            switchToSlave.checkDataConsistency( mock( HaXaDataSourceManager.class ), mock( MasterClient.class ),
                    mock( RequestContextFactory.class ), nioneoDataSource, null, true /*set true to skip some check*/ );
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
    public void shouldHandleBranchedStoreWhenFailedToGetMasterForCommittedTxWithNoSuchLogVersionException()
            throws Throwable
    {
        // Given
        NeoStoreXaDataSource nioneoDataSource = mock( NeoStoreXaDataSource.class );

        // When
        long fakeTxId = 2L; // any tx id
        when( nioneoDataSource.getLastCommittedTxId() ).thenReturn( fakeTxId );
        when( nioneoDataSource.getMasterForCommittedTx( fakeTxId ) )
                .thenThrow( new NoSuchLogVersionException( fakeTxId ) );

        try
        {
            switchToSlave.checkDataConsistency( mock( HaXaDataSourceManager.class ), mock( MasterClient.class ),
                    mock( RequestContextFactory.class ), nioneoDataSource, null, true /*set true to skip some check*/ );
            fail( "Should have thrown " + NoSuchLogVersionException.class.getSimpleName() + " exception" );
        }
        catch ( NoSuchLogVersionException e )
        {
            // good we got the expected exception
        }

        // Then
        verify( switchToSlave ).stopServicesAndHandleBranchedStore( any( BranchedDataPolicy.class ) );
    }

    private static Config configMock()
    {
        Config config = mock( Config.class );
        when( config.get( HaSettings.branched_data_policy ) ).thenReturn( mock( BranchedDataPolicy.class ) );
        return config;
    }

    private static DependencyResolver dependencyResolverMock()
    {
        DependencyResolver resolver = mock( DependencyResolver.class );

        when( resolver.resolveDependency( StoreLockerLifecycleAdapter.class ) ).thenReturn(
                mockWithLifecycle( StoreLockerLifecycleAdapter.class ) );
        when( resolver.resolveDependency( XaDataSourceManager.class ) ).thenReturn(
                mockWithLifecycle( XaDataSourceManager.class ) );
        when( resolver.resolveDependency( TransactionManager.class ) ).thenReturn(
                mockWithLifecycle( TransactionManager.class ) );
        when( resolver.resolveDependency( NodeManager.class ) ).thenReturn(
                mockWithLifecycle( NodeManager.class ) );
        when( resolver.resolveDependency( IndexStore.class ) ).thenReturn(
                mockWithLifecycle( IndexStore.class ) );
        return resolver;
    }

    private static <T> T mockWithLifecycle( Class<T> clazz )
    {
        return mock( clazz, withSettings().extraInterfaces( Lifecycle.class ) );
    }
}
