/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 *
 * This file is part of Neo4j Enterprise Edition. The included source
 * code can be redistributed and/or modified under the terms of the
 * Neo4j Sweden Software License, as found in the associated LICENSE.txt
 * file.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * Neo4j Sweden Software License for more details.
 */
package org.neo4j.causalclustering.scenarios;

import org.junit.Before;
import org.junit.Ignore;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.RuleChain;

import java.io.File;
import java.io.IOException;
import java.util.Collection;
import java.util.List;
import java.util.UUID;
import java.util.stream.Collectors;

import org.neo4j.causalclustering.core.CausalClusteringSettings;
import org.neo4j.causalclustering.core.state.storage.SimpleFileStorage;
import org.neo4j.causalclustering.core.state.storage.SimpleStorage;
import org.neo4j.causalclustering.discovery.Cluster;
import org.neo4j.causalclustering.discovery.CoreClusterMember;
import org.neo4j.causalclustering.identity.ClusterId;
import org.neo4j.graphdb.Node;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.io.pagecache.impl.muninn.StandalonePageCacheFactory;
import org.neo4j.kernel.impl.store.MetaDataStore;
import org.neo4j.kernel.lifecycle.LifecycleException;
import org.neo4j.logging.NullLogProvider;
import org.neo4j.test.causalclustering.ClusterRule;
import org.neo4j.test.rule.fs.DefaultFileSystemRule;

import static org.hamcrest.CoreMatchers.instanceOf;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;
import static org.neo4j.causalclustering.TestStoreId.assertAllStoresHaveTheSameStoreId;
import static org.neo4j.causalclustering.core.server.CoreServerModule.CLUSTER_ID_NAME;
import static org.neo4j.graphdb.Label.label;
import static org.neo4j.kernel.impl.store.MetaDataStore.Position.RANDOM_NUMBER;

public class ClusterBindingIT
{
    private final ClusterRule clusterRule = new ClusterRule()
                        .withNumberOfCoreMembers( 3 )
                        .withNumberOfReadReplicas( 0 )
                        .withSharedCoreParam( CausalClusteringSettings.raft_log_pruning_strategy, "3 entries" )
                        .withSharedCoreParam( CausalClusteringSettings.raft_log_rotation_size, "1K" );
    private final DefaultFileSystemRule fileSystemRule = new DefaultFileSystemRule();

    @Rule
    public RuleChain ruleChain = RuleChain.outerRule( fileSystemRule ).around( clusterRule );

    private Cluster cluster;
    private FileSystemAbstraction fs;

    @Before
    public void setup() throws Exception
    {
        fs = fileSystemRule.get();
        cluster = clusterRule.startCluster();
        cluster.coreTx( ( db, tx ) ->
        {
            SampleData.createSchema( db );
            tx.success();
        } );
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
    @Ignore( "Fix this test by having the bootstrapper augment his store and bind it using store-id on disk." )
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

        cluster.removeCoreMemberWithServerId( 0 );
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

        //TODO: Work out if/why this won't potentially remove a leader?
        cluster.removeCoreMemberWithServerId( 0 );

        SampleData.createSomeData( 100, cluster );

        for ( CoreClusterMember db : cluster.coreMembers() )
        {
            db.raftLogPruner().prune();
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

        CoreClusterMember coreMember = cluster.getCoreMemberById( 0 );
        cluster.removeCoreMemberWithServerId( 0 );
        changeClusterId( coreMember );

        SampleData.createSomeData( 100, cluster );

        for ( CoreClusterMember db : cluster.coreMembers() )
        {
            db.raftLogPruner().prune();
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
            db.raftLogPruner().prune();
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

    private void changeClusterId( CoreClusterMember coreMember ) throws IOException
    {
        SimpleStorage<ClusterId> clusterIdStorage = new SimpleFileStorage<>( fs, coreMember.clusterStateDirectory(),
                CLUSTER_ID_NAME, new ClusterId.Marshal(), NullLogProvider.getInstance() );
        clusterIdStorage.writeState( new ClusterId( UUID.randomUUID() ) );
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
