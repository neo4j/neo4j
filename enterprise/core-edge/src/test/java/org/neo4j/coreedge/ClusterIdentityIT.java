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
package org.neo4j.coreedge;

import java.io.File;
import java.io.IOException;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.Future;
import java.util.concurrent.TimeoutException;
import java.util.stream.Collectors;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import org.neo4j.coreedge.discovery.Cluster;
import org.neo4j.coreedge.server.CoreEdgeClusterSettings;
import org.neo4j.coreedge.server.core.CoreGraphDatabase;
import org.neo4j.graphdb.Node;
import org.neo4j.io.fs.DefaultFileSystemAbstraction;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.kernel.impl.factory.GraphDatabaseFacade;
import org.neo4j.kernel.impl.pagecache.StandalonePageCacheFactory;
import org.neo4j.kernel.impl.store.MetaDataStore;
import org.neo4j.test.coreedge.ClusterRule;
import org.neo4j.test.rule.SuppressOutput;

import static org.junit.Assert.assertEquals;
import static org.neo4j.graphdb.Label.label;
import static org.neo4j.kernel.impl.store.MetaDataStore.Position.RANDOM_NUMBER;
import static org.neo4j.kernel.impl.store.MetaDataStore.Position.TIME;
import static org.neo4j.kernel.impl.store.MetaDataStore.Position.UPGRADE_TIME;
import static org.neo4j.kernel.impl.store.MetaDataStore.Position.UPGRADE_TRANSACTION_ID;
import static org.neo4j.test.rule.SuppressOutput.suppress;

public class ClusterIdentityIT
{
    @Rule
    public SuppressOutput suppressOutput = suppress( SuppressOutput.System.err );

    @Rule
    public ClusterRule clusterRule = new ClusterRule( ClusterIdentityIT.class )
            .withNumberOfCoreServers( 3 )
            .withNumberOfEdgeServers( 0 )
            .withSharedCoreParam( CoreEdgeClusterSettings.raft_log_pruning_strategy, "3 entries" )
            .withSharedCoreParam( CoreEdgeClusterSettings.raft_log_rotation_size, "1K" );

    private Cluster cluster;
    private DefaultFileSystemAbstraction fs = new DefaultFileSystemAbstraction();

    @Before
    public void setup() throws Exception
    {
        cluster = clusterRule.startCluster();
    }

    @Test
    public void allServersShouldHaveTheSameStoreId() throws Throwable
    {
        // WHEN
        cluster.coreTx( ( db, tx ) -> {
            Node node = db.createNode( label( "boo" ) );
            node.setProperty( "foobar", "baz_bat" );
            tx.success();
        } );

        List<String> coreStoreDirs = storeDirs( cluster.coreServers() );

        cluster.shutdown();

        // THEN
        assertAllStoresHaveTheSameStoreId( coreStoreDirs );
    }

    @Test
    public void whenWeRestartTheClusterAllServersShouldStillHaveTheSameStoreId() throws Throwable
    {
        // GIVEN
        cluster.coreTx( ( db, tx ) -> {
            Node node = db.createNode( label( "boo" ) );
            node.setProperty( "foobar", "baz_bat" );
            tx.success();
        } );

        cluster.shutdown();

        // WHEN
        cluster.start();

        List<String> coreStoreDirs = storeDirs( cluster.coreServers() );

        cluster.coreTx( ( db, tx ) -> {
            Node node = db.createNode( label( "boo" ) );
            node.setProperty( "foobar", "baz_bat" );
            tx.success();
        } );

        cluster.shutdown();

        // THEN
        assertAllStoresHaveTheSameStoreId( coreStoreDirs );
    }

    @Test
    public void shouldNotJoinClusterIfHasDataWithDifferentStoreId() throws Exception
    {
        // GIVEN
        cluster.coreTx( ( db, tx ) -> {
            Node node = db.createNode( label( "boo" ) );
            node.setProperty( "foobar", "baz_bat" );
            tx.success();
        } );

        String storeDir = cluster.getCoreServerById( 0 ).getStoreDir();

        cluster.removeCoreServerWithServerId( 0 );
        changeStoreId( storeDir );

        // WHEN
        Future<?> future = cluster.asyncAddCoreServerWithServerId( 0, 3 );

        cluster.awaitLeader();

        // THEN
        assertEquals( 2, cluster.healthyCoreMembers().size() );

        future.cancel( true );
    }

    @Test
    public void laggingFollowerShouldDownloadSnapshot() throws Exception
    {
        // GIVEN
        cluster.coreTx( ( db, tx ) -> {
            Node node = db.createNode( label( "boo" ) );
            node.setProperty( "foobar", "baz_bat" );
            tx.success();
        } );

        cluster.removeCoreServerWithServerId( 0 );

        createSomeData( 100, cluster );

        for ( CoreGraphDatabase db : cluster.coreServers() )
        {
            db.compact();
        }

        // WHEN
        cluster.addCoreServerWithServerId( 0, 3 );

        cluster.awaitLeader();

        // THEN
        assertEquals( 3, cluster.healthyCoreMembers().size() );

        List<String> coreStoreDirs = storeDirs( cluster.coreServers() );
        cluster.shutdown();
        assertAllStoresHaveTheSameStoreId( coreStoreDirs );
    }

    @Test
    public void badFollowerShouldNotJoinCluster() throws Exception
    {
        // GIVEN
        cluster.coreTx( ( db, tx ) -> {
            Node node = db.createNode( label( "boo" ) );
            node.setProperty( "foobar", "baz_bat" );
            tx.success();
        } );

        String storeDir = cluster.getCoreServerById( 0 ).getStoreDir();
        cluster.removeCoreServerWithServerId( 0 );
        changeStoreId( storeDir );

        createSomeData( 100, cluster );

        for ( CoreGraphDatabase db : cluster.coreServers() )
        {
            db.compact();
        }

        // WHEN
        Future<?> future = cluster.asyncAddCoreServerWithServerId( 0, 3 );
        cluster.awaitLeader();

        // THEN
        assertEquals( 2, cluster.healthyCoreMembers().size() );

        future.cancel( true );
    }

    @Test
    public void aNewServerShouldJoinTheClusterByDownloadingASnapshot() throws Exception
    {
        // GIVEN
        cluster.coreTx( ( db, tx ) -> {
            Node node = db.createNode( label( "boo" ) );
            node.setProperty( "foobar", "baz_bat" );
            tx.success();
        } );

        createSomeData( 100, cluster );

        for ( CoreGraphDatabase db : cluster.coreServers() )
        {
            db.compact();
        }

        // WHEN
        cluster.addCoreServerWithServerId( 4, 4 );

        cluster.awaitLeader();

        // THEN
        assertEquals( 4, cluster.healthyCoreMembers().size() );

        List<String> coreStoreDirs = storeDirs( cluster.coreServers() );
        cluster.shutdown();
        assertAllStoresHaveTheSameStoreId( coreStoreDirs );
    }

    private List<String> storeDirs( Set<CoreGraphDatabase> dbs )
    {
        return dbs.stream().map( GraphDatabaseFacade::getStoreDir ).collect( Collectors.toList() );
    }

    private void createSomeData( int items, Cluster cluster ) throws TimeoutException, InterruptedException
    {
        for ( int i = 0; i < items; i++ )
        {
            cluster.coreTx( ( db, tx ) -> {
                Node node = db.createNode( label( "boo" ) );
                node.setProperty( "foobar", "baz_bat" );
                tx.success();
            } );
        }
    }

    private void changeStoreId( String storeDir ) throws IOException
    {
        File neoStoreFile = new File( storeDir, MetaDataStore.DEFAULT_NAME );
        try ( PageCache pageCache = StandalonePageCacheFactory.createPageCache( fs ) )
        {
            MetaDataStore.setRecord( pageCache, neoStoreFile, TIME, System.currentTimeMillis() );
        }
    }

    private void assertAllStoresHaveTheSameStoreId( List<String> coreStoreDirs ) throws IOException
    {
        Set<StoreId> storeIds = new HashSet<>();
        try ( PageCache pageCache = StandalonePageCacheFactory.createPageCache( fs ) )
        {
            for ( String coreStoreDir : coreStoreDirs )
            {
                File metadataStore = new File( coreStoreDir, MetaDataStore.DEFAULT_NAME );

                long creationTime = MetaDataStore.getRecord( pageCache, metadataStore, TIME );
                long randomNumber = MetaDataStore.getRecord( pageCache, metadataStore, RANDOM_NUMBER );
                long upgradeTime = MetaDataStore.getRecord( pageCache, metadataStore, UPGRADE_TIME );
                long upgradeId = MetaDataStore.getRecord( pageCache, metadataStore, UPGRADE_TRANSACTION_ID );

                storeIds.add( new StoreId( creationTime, randomNumber, upgradeTime, upgradeId ) );
            }
        }

        assertEquals( 1, storeIds.size() );
    }

    private static class StoreId
    {

        private final long creationTime;
        private final long randomNumber;
        private final long upgradeTime;
        private final long upgradeId;

        public StoreId( long creationTime, long randomNumber, long upgradeTime, long upgradeId )
        {

            this.creationTime = creationTime;
            this.randomNumber = randomNumber;
            this.upgradeTime = upgradeTime;
            this.upgradeId = upgradeId;
        }

        @Override
        public boolean equals( Object o )
        {
            if ( this == o )
            {
                return true;
            }
            if ( o == null || getClass() != o.getClass() )
            {
                return false;
            }
            StoreId storeId = (StoreId) o;
            return creationTime == storeId.creationTime &&
                    randomNumber == storeId.randomNumber &&
                    upgradeTime == storeId.upgradeTime &&
                    upgradeId == storeId.upgradeId;
        }

        @Override
        public int hashCode()
        {
            return Objects.hash( creationTime, randomNumber, upgradeTime, upgradeId );
        }
    }
}
