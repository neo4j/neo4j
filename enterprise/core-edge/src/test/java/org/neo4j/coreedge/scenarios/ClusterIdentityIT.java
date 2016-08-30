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
package org.neo4j.coreedge.scenarios;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.util.Collection;
import java.util.List;
import java.util.stream.Collectors;

import org.neo4j.coreedge.core.CoreEdgeClusterSettings;
import org.neo4j.coreedge.discovery.Cluster;
import org.neo4j.coreedge.discovery.CoreClusterMember;
import org.neo4j.graphdb.Node;
import org.neo4j.io.fs.DefaultFileSystemAbstraction;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.kernel.impl.pagecache.StandalonePageCacheFactory;
import org.neo4j.kernel.impl.store.MetaDataStore;
import org.neo4j.kernel.lifecycle.LifecycleException;
import org.neo4j.test.coreedge.ClusterRule;
import org.neo4j.test.rule.SuppressOutput;

import static org.hamcrest.CoreMatchers.instanceOf;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;
import static org.neo4j.coreedge.TestStoreId.assertAllStoresHaveTheSameStoreId;
import static org.neo4j.graphdb.Label.label;
import static org.neo4j.kernel.impl.store.MetaDataStore.Position.RANDOM_NUMBER;
import static org.neo4j.test.rule.SuppressOutput.suppress;

public class ClusterIdentityIT
{
    @Rule
    public SuppressOutput suppressOutput = suppress( SuppressOutput.System.err );

    @Rule
    public ClusterRule clusterRule = new ClusterRule( ClusterIdentityIT.class )
            .withNumberOfCoreMembers( 3 )
            .withNumberOfEdgeMembers( 0 )
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
        cluster.coreTx( ( db, tx ) ->
        {
            Node node = db.createNode( label( "boo" ) );
            node.setProperty( "foobar", "baz_bat" );
            tx.success();
        } );

        List<File> coreStoreDirs = storeDirs( cluster.coreMembers() );

        cluster.shutdown();

        // THEN
        assertAllStoresHaveTheSameStoreId( coreStoreDirs, fs );
    }

    @Test
    public void whenWeRestartTheClusterAllServersShouldStillHaveTheSameStoreId() throws Throwable
    {
        // GIVEN
        cluster.coreTx( ( db, tx ) ->
        {
            Node node = db.createNode( label( "boo" ) );
            node.setProperty( "foobar", "baz_bat" );
            tx.success();
        } );

        cluster.shutdown();
        // WHEN
        cluster.start();

        List<File> coreStoreDirs = storeDirs( cluster.coreMembers() );

        cluster.coreTx( ( db, tx ) ->
        {
            Node node = db.createNode( label( "boo" ) );
            node.setProperty( "foobar", "baz_bat" );
            tx.success();
        } );

        cluster.shutdown();

        // THEN
        assertAllStoresHaveTheSameStoreId( coreStoreDirs, fs );
    }

    @Test
    public void shouldNotJoinClusterIfHasDataWithDifferentStoreId() throws Exception
    {
        // GIVEN
        cluster.coreTx( ( db, tx ) ->
        {
            Node node = db.createNode( label( "boo" ) );
            node.setProperty( "foobar", "baz_bat" );
            tx.success();
        } );

        File storeDir = cluster.getCoreMemberById( 0 ).storeDir();

        cluster.removeCoreMemberWithMemberId( 0 );
        changeStoreId( storeDir );

        // WHEN
        try
        {
            cluster.addCoreMemberWithId( 0 ).start();
            fail( "Should not have joined the cluster" );
        }
        catch ( RuntimeException e )
        {
            assertThat( e.getCause(), instanceOf( LifecycleException.class ) );
        }
    }

    @Test
    public void laggingFollowerShouldDownloadSnapshot() throws Exception
    {
        // GIVEN
        cluster.coreTx( ( db, tx ) ->
        {
            Node node = db.createNode( label( "boo" ) );
            node.setProperty( "foobar", "baz_bat" );
            tx.success();
        } );

        cluster.removeCoreMemberWithMemberId( 0 );

        SampleData.createSomeData( 100, cluster );

        for ( CoreClusterMember db : cluster.coreMembers() )
        {
            db.coreState().prune();
        }

        // WHEN
        cluster.addCoreMemberWithId( 0 ).start();

        cluster.awaitLeader();

        // THEN
        assertEquals( 3, cluster.healthyCoreMembers().size() );

        List<File> coreStoreDirs = storeDirs( cluster.coreMembers() );
        cluster.shutdown();
        assertAllStoresHaveTheSameStoreId( coreStoreDirs, fs );
    }

    @Test
    public void badFollowerShouldNotJoinCluster() throws Exception
    {
        // GIVEN
        cluster.coreTx( ( db, tx ) ->
        {
            Node node = db.createNode( label( "boo" ) );
            node.setProperty( "foobar", "baz_bat" );
            tx.success();
        } );

        File storeDir = cluster.getCoreMemberById( 0 ).storeDir();
        cluster.removeCoreMemberWithMemberId( 0 );
        changeStoreId( storeDir );

        SampleData.createSomeData( 100, cluster );

        for ( CoreClusterMember db : cluster.coreMembers() )
        {
            db.coreState().prune();
        }

        // WHEN
        try
        {
            cluster.addCoreMemberWithId( 0 ).start();
            fail( "Should not have joined the cluster" );
        }
        catch ( RuntimeException e )
        {
            assertThat( e.getCause(), instanceOf( LifecycleException.class ) );
        }
    }

    @Test
    public void aNewServerShouldJoinTheClusterByDownloadingASnapshot() throws Exception
    {
        // GIVEN
        cluster.coreTx( ( db, tx ) ->
        {
            Node node = db.createNode( label( "boo" ) );
            node.setProperty( "foobar", "baz_bat" );
            tx.success();
        } );

        SampleData.createSomeData( 100, cluster );

        for ( CoreClusterMember db : cluster.coreMembers() )
        {
            db.coreState().prune();
        }

        // WHEN
        cluster.addCoreMemberWithId( 4 ).start();

        cluster.awaitLeader();

        // THEN
        assertEquals( 4, cluster.healthyCoreMembers().size() );

        List<File> coreStoreDirs = storeDirs( cluster.coreMembers() );
        cluster.shutdown();
        assertAllStoresHaveTheSameStoreId( coreStoreDirs, fs );
    }

    private List<File> storeDirs( Collection<CoreClusterMember> dbs )
    {
        return dbs.stream().map( CoreClusterMember::storeDir ).collect( Collectors.toList() );
    }

    private void changeStoreId( File storeDir ) throws IOException
    {
        File neoStoreFile = new File( storeDir, MetaDataStore.DEFAULT_NAME );
        try ( PageCache pageCache = StandalonePageCacheFactory.createPageCache( fs ) )
        {
            MetaDataStore.setRecord( pageCache, neoStoreFile, RANDOM_NUMBER, System.currentTimeMillis() );
        }
    }

}
