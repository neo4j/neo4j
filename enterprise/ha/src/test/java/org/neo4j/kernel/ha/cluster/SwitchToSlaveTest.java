/**
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

import java.io.IOException;
import java.net.URI;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import org.neo4j.cluster.client.ClusterClient;
import org.neo4j.cluster.member.ClusterMemberAvailability;
import org.neo4j.graphdb.DependencyResolver;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.extension.KernelExtensionFactory;
import org.neo4j.kernel.ha.BranchedDataException;
import org.neo4j.kernel.ha.BranchedDataPolicy;
import org.neo4j.kernel.ha.DelegateInvocationHandler;
import org.neo4j.kernel.ha.HaXaDataSourceManager;
import org.neo4j.kernel.ha.com.RequestContextFactory;
import org.neo4j.kernel.ha.com.master.Master;
import org.neo4j.kernel.ha.com.slave.MasterClient;
import org.neo4j.kernel.ha.id.HaIdGeneratorFactory;
import org.neo4j.kernel.impl.api.UpdateableSchemaState;
import org.neo4j.kernel.impl.nioneo.xa.NeoStoreXaDataSource;
import org.neo4j.kernel.impl.transaction.xaframework.NoSuchLogVersionException;
import org.neo4j.kernel.logging.ConsoleLogger;
import org.neo4j.kernel.logging.Logging;
import org.neo4j.kernel.logging.SystemOutLogging;
import org.neo4j.kernel.monitoring.Monitors;
import org.neo4j.test.Mute;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class SwitchToSlaveTest
{
    private SwitchToSlave switchToSlave;
    private boolean methodCalled;

    @Rule
    public Mute mute = Mute.muteAll();

    @Before
    public void setUp()
    {
        // create a store with a wrong tx
        ConsoleLogger console = mock( ConsoleLogger.class );
        Config config = new Config();
        DependencyResolver resolver = mock( DependencyResolver.class );
        HaIdGeneratorFactory idGeneratorFactory = mock( HaIdGeneratorFactory.class );
        Logging logging = new SystemOutLogging();
        DelegateInvocationHandler<Master> masterDelegateHandler = mock( DelegateInvocationHandler.class );
        ClusterMemberAvailability clusterMemberAvailability = mock( ClusterMemberAvailability.class );
        ClusterClient clusterClient = mock( ClusterClient.class );
        RequestContextFactory requestContextFactory = mock( RequestContextFactory.class );
        UpdateableSchemaState updateableSchemaState = mock( UpdateableSchemaState.class );
        Monitors monitors = mock( Monitors.class );
        Iterable<KernelExtensionFactory<?>> kernelExtensions = mock( Iterable.class );

        switchToSlave = new SwitchToSlave( console, config, resolver, idGeneratorFactory, logging,
                masterDelegateHandler, clusterMemberAvailability, clusterClient, requestContextFactory,
                updateableSchemaState, monitors, kernelExtensions )
        {
            @Override
            void stopServicesAndHandleBranchedStore( BranchedDataPolicy branchPolicy ) throws Throwable
            {
                methodCalled = true;
                return;
            }
        };
        methodCalled = false;
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
            fail();
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
        assertFalse( methodCalled );
        NeoStoreXaDataSource nioneoDataSource = mock( NeoStoreXaDataSource.class );

        // When
        long fakeTxId = 2L; // any tx id
        when( nioneoDataSource.getLastCommittedTxId() ).thenReturn( fakeTxId );
        when( nioneoDataSource.getMasterForCommittedTx( fakeTxId ) ).thenThrow( new IOException() );

        try
        {
            switchToSlave.checkDataConsistency( mock( HaXaDataSourceManager.class ), mock( MasterClient.class ),
                    mock( RequestContextFactory.class ), nioneoDataSource, null, true /*set true to skip some check*/);
            fail();
        }
        catch ( BranchedDataException e )
        {
        }

        // Then
        assertTrue( methodCalled );
    }

    @Test
    public void shouldHandleBranchedStoreWhenFailedToGetMasterForCommittedTxWithNoSuchLogVersionException() throws Throwable
    {
        // Given
        assertFalse( methodCalled );
        NeoStoreXaDataSource nioneoDataSource = mock( NeoStoreXaDataSource.class );

        // When
        long fakeTxId = 2L; // any tx id
        when( nioneoDataSource.getLastCommittedTxId() ).thenReturn( fakeTxId );
        when( nioneoDataSource.getMasterForCommittedTx( fakeTxId ) ).thenThrow( new NoSuchLogVersionException( fakeTxId ) );

        try
        {
            switchToSlave.checkDataConsistency( mock( HaXaDataSourceManager.class ), mock( MasterClient.class ),
                    mock( RequestContextFactory.class ), nioneoDataSource, null, true /*set true to skip some check*/);
            fail();
        }
        catch ( NoSuchLogVersionException e )
        {
        }

        // Then
        assertTrue( methodCalled );
    }

}
