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
package org.neo4j.causalclustering.readreplica;

import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.util.UUID;

import org.neo4j.causalclustering.catchup.storecopy.CopiedStoreRecovery;
import org.neo4j.causalclustering.catchup.storecopy.LocalDatabase;
import org.neo4j.causalclustering.catchup.storecopy.StoreFetcher;
import org.neo4j.causalclustering.catchup.storecopy.StoreIdDownloadFailedException;
import org.neo4j.causalclustering.core.state.machines.tx.ConstantTimeRetryStrategy;
import org.neo4j.causalclustering.discovery.CoreTopology;
import org.neo4j.causalclustering.discovery.TopologyService;
import org.neo4j.causalclustering.identity.MemberId;
import org.neo4j.causalclustering.identity.StoreId;
import org.neo4j.causalclustering.messaging.routing.AlwaysChooseFirstMember;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.kernel.lifecycle.Lifecycle;
import org.neo4j.logging.NullLogProvider;

import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static org.hamcrest.Matchers.containsString;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.neo4j.helpers.collection.Iterators.asSet;

public class ReadReplicaStartupProcessTest
{
    private CopiedStoreRecovery copiedStoreRecovery = mock( CopiedStoreRecovery.class );
    private FileSystemAbstraction fs = mock( FileSystemAbstraction.class );
    private StoreFetcher storeFetcher = mock( StoreFetcher.class );
    private LocalDatabase localDatabase = mock( LocalDatabase.class );
    private TopologyService hazelcastTopology = mock( TopologyService.class );
    private CoreTopology clusterTopology = mock( CoreTopology.class );
    private Lifecycle txPulling = mock( Lifecycle.class );

    private MemberId memberId = new MemberId( UUID.randomUUID() );
    private StoreId localStoreId = new StoreId( 1, 2, 3, 4 );
    private StoreId otherStoreId = new StoreId( 5, 6, 7, 8 );
    private File storeDir = new File( "store-dir" );

    @Before
    public void commonMocking() throws StoreIdDownloadFailedException
    {
        when( localDatabase.storeDir() ).thenReturn( storeDir );
        when( localDatabase.storeId() ).thenReturn( localStoreId );
        when( hazelcastTopology.coreServers() ).thenReturn( clusterTopology );
        when( clusterTopology.members() ).thenReturn( asSet( memberId ) );
    }

    @Test
    public void shouldReplaceEmptyStoreWithRemote() throws Throwable
    {
        // given
        when( localDatabase.isEmpty() ).thenReturn( true );
        when( storeFetcher.getStoreIdOf( any() ) ).thenReturn( otherStoreId );

        ReadReplicaStartupProcess
                readReplicaStartupProcess = new ReadReplicaStartupProcess( fs, storeFetcher, localDatabase, txPulling,
                new AlwaysChooseFirstMember( hazelcastTopology ), new ConstantTimeRetryStrategy( 1, MILLISECONDS ),
                NullLogProvider.getInstance(), copiedStoreRecovery );

        // when
        readReplicaStartupProcess.start();

        // then
        verify( storeFetcher ).copyStore( any(), any(), any() );
        verify( localDatabase ).start();
        verify( txPulling ).start();
    }

    @Test
    public void shouldNotStartWithMismatchedNonEmptyStore() throws Throwable
    {
        // given
        when( localDatabase.isEmpty() ).thenReturn( false );
        when( storeFetcher.getStoreIdOf( any() ) ).thenReturn( otherStoreId );

        ReadReplicaStartupProcess
                readReplicaStartupProcess = new ReadReplicaStartupProcess( fs, storeFetcher, localDatabase, txPulling,
                new AlwaysChooseFirstMember( hazelcastTopology ), new ConstantTimeRetryStrategy( 1, MILLISECONDS ),
                NullLogProvider.getInstance(), copiedStoreRecovery );

        // when
        try
        {
            readReplicaStartupProcess.start();
            fail( "should have thrown" );
        }
        catch ( Exception ex )
        {
            //expected.
            assertThat( ex.getMessage(), containsString(
                    "This read replica cannot join the cluster. The local database is not empty and has a " +
                            "mismatching storeId" ) );
        }

        // then
        verify( txPulling, never() ).start();
    }

    @Test
    public void shouldStartWithMatchingDatabase() throws Throwable
    {
        // given
        when( storeFetcher.getStoreIdOf( any() ) ).thenReturn( localStoreId );
        when( localDatabase.isEmpty() ).thenReturn( false );

        ReadReplicaStartupProcess
                readReplicaStartupProcess = new ReadReplicaStartupProcess( fs, storeFetcher, localDatabase, txPulling,
                new AlwaysChooseFirstMember( hazelcastTopology ), new ConstantTimeRetryStrategy( 1, MILLISECONDS ),
                NullLogProvider.getInstance(), copiedStoreRecovery );

        // when
        readReplicaStartupProcess.start();

        // then
        verify( localDatabase ).start();
        verify( txPulling ).start();
    }

    @Test
    public void stopShouldStopTheDatabaseAndStopPolling() throws Throwable
    {
        // given
        when( storeFetcher.getStoreIdOf( any() ) ).thenReturn( localStoreId );
        when( localDatabase.isEmpty() ).thenReturn( false );

        ReadReplicaStartupProcess
                readReplicaStartupProcess = new ReadReplicaStartupProcess( fs, storeFetcher, localDatabase, txPulling,
                new AlwaysChooseFirstMember( hazelcastTopology ), new ConstantTimeRetryStrategy( 1, MILLISECONDS ),
                NullLogProvider.getInstance(), copiedStoreRecovery );
        readReplicaStartupProcess.start();

        // when
        readReplicaStartupProcess.stop();

        // then
        verify( txPulling ).stop();
        verify( localDatabase ).stop();
    }
}
