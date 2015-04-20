/*
 * Copyright (c) 2002-2015 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
 *
 * This file is part of Neo4j.
 *
 * Neo4j is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.ha;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import java.io.File;
import java.io.IOException;

import org.neo4j.consistency.checking.full.ConsistencyCheckIncompleteException;
import org.neo4j.graphdb.Transaction;
import org.neo4j.io.fs.FileUtils;
import org.neo4j.kernel.ha.HighlyAvailableGraphDatabase;
import org.neo4j.kernel.impl.storemigration.LogFiles;
import org.neo4j.test.TargetDirectory;
import org.neo4j.test.TargetDirectory.TestDirectory;
import org.neo4j.test.ha.ClusterManager;

import static org.neo4j.consistency.store.StoreAssertions.assertConsistentStore;
import static org.neo4j.helpers.collection.MapUtil.stringMap;
import static org.neo4j.test.ha.ClusterManager.allSeesAllAsAvailable;
import static org.neo4j.test.ha.ClusterManager.clusterOfSize;

public class HAClusterStartupIT
{
    public final @Rule TestDirectory dir = TargetDirectory.testDirForTest( getClass() );
    private ClusterManager clusterManager;
    private ClusterManager.ManagedCluster cluster;
    private HighlyAvailableGraphDatabase master;
    private HighlyAvailableGraphDatabase slave1;
    private HighlyAvailableGraphDatabase slave2;

    @Before
    public void instantiateClusterManager()
    {
        clusterManager = new ClusterManager( clusterOfSize( 3 ), dir.directory(), stringMap() );
    }

    @Before
    public void setup() throws Throwable
    {
        // setup a cluster with some data and entries in log files in fully functional and shutdown state
        clusterManager.start();

        cluster = clusterManager.getDefaultCluster();
        try
        {
            cluster.await( allSeesAllAsAvailable() );

            master = cluster.getMaster();
            try ( Transaction tx = master.beginTx() )
            {
                master.createNode();
                tx.success();
            }
            cluster.sync();

            slave1 = cluster.getAnySlave();
            slave2 = cluster.getAnySlave( slave1 );
        }
        finally
        {
            clusterManager.shutdown();
        }

        assertAllStoreConsistent();
    }

    @Test
    public void aSlaveWithoutAnyGraphDBFilesShouldBeAbleToJoinACluster() throws Throwable
    {
        // GIVEN a cluster with some data and entry in log files


        // WHEN removing all the files in graphdb on the slave and restarting the cluster
        deleteAllFilesOn( slave1 );

        clusterManager.start();

        // THEN the cluster should work
        cluster = clusterManager.getDefaultCluster();
        try
        {
            cluster.await( allSeesAllAsAvailable() );
        }
        finally
        {
            clusterManager.shutdown();
        }

        assertAllStoreConsistent();
    }

    @Test
    public void bothSlavesWithoutAnyGraphDBFilesShouldBeAbleToJoinACluster() throws Throwable
    {
        // GIVEN a cluster with some data and entry in log files


        // WHEN removing all the files in graphdb on both slaves and restarting the cluster
        deleteAllFilesOn( slave1 );
        deleteAllFilesOn( slave2 );

        clusterManager.start();

        // THEN the cluster should work
        cluster = clusterManager.getDefaultCluster();
        try
        {
            cluster.await( allSeesAllAsAvailable() );
        }
        finally
        {
            clusterManager.shutdown();
        }

        assertAllStoreConsistent();
    }

    @Test
    public void theMasterWithoutAnyGraphDBFilesShouldBeAbleToJoinACluster() throws Throwable
    {
        // GIVEN a cluster with some data and entry in log files


        // WHEN removing all the files in graphdb on the db that was master and restarting the cluster
        deleteAllFilesOn( master );

        clusterManager.start();

        // THEN the cluster should work
        cluster = clusterManager.getDefaultCluster();
        try
        {
            cluster.await( allSeesAllAsAvailable(), 120 );
        }
        finally
        {
            clusterManager.shutdown();
        }

        assertAllStoreConsistent();
    }

    @Test
    public void aSlaveWithoutLogicalLogFilesShouldBeAbleToJoinACluster() throws Throwable
    {
        // GIVEN a cluster with some data and entry in log files

        // WHEN removing all logical log files in graphdb on the slave and restarting the cluster
        deleteAllLogsOn( slave1 );

        clusterManager.start();

        // THEN the cluster should work
        cluster = clusterManager.getDefaultCluster();
        try
        {
            cluster.await( allSeesAllAsAvailable() );
        }
        finally
        {
            clusterManager.shutdown();
        }

        assertAllStoreConsistent();
    }

    @Test
    public void bothSlaveWithoutLogicalLogFilesShouldBeAbleToJoinACluster() throws Throwable
    {
        // GIVEN a cluster with some data and entry in log files

        // WHEN removing all logical log files in graphdb on the slave and restarting the cluster
        deleteAllLogsOn( slave1 );
        deleteAllLogsOn( slave2 );

        clusterManager.start();

        // THEN the cluster should work
        cluster = clusterManager.getDefaultCluster();
        try
        {
            cluster.await( allSeesAllAsAvailable() );
        }
        finally
        {
            clusterManager.shutdown();
        }

        assertAllStoreConsistent();
    }

    @Test
    public void aClusterShouldStartAndRunWhenSeededWithAStoreHavingNoLogicalLogFiles() throws Throwable
    {
        // GIVEN a cluster with some data and entry in log files

        // WHEN removing all logical log files in graphdb on the slave and restarting a new cluster
        File seedDir = deleteAllLogsOn( slave1 );

        File newDir = new File( dir.directory(), "new" );
        FileUtils.deleteRecursively( newDir );
        ClusterManager newClusterManager = new ClusterManager(
                new ClusterManager.Builder( newDir ).withProvider( clusterOfSize( 3 ) ).withSeedDir( seedDir )
        );

        newClusterManager.start();

        // THEN the new cluster should work
        ClusterManager.ManagedCluster newCluster = newClusterManager.getDefaultCluster();
        HighlyAvailableGraphDatabase newMaster;
        HighlyAvailableGraphDatabase newSlave1;
        HighlyAvailableGraphDatabase newSlave2;
        try
        {
            newCluster.await( allSeesAllAsAvailable() );
            newMaster = newCluster.getMaster();
            newSlave1 = newCluster.getAnySlave();
            newSlave2 = newCluster.getAnySlave( newSlave1 );
        }
        finally
        {
            newClusterManager.shutdown();
        }

        assertAllStoreConsistent( newMaster, newSlave1, newSlave2 );

        assertConsistentStore( new File( newMaster.getStoreDir() ) );
        assertConsistentStore( new File( newSlave1.getStoreDir() ) );
        assertConsistentStore( new File( newSlave2.getStoreDir() ) );
    }

    private void deleteAllFilesOn( HighlyAvailableGraphDatabase instance ) throws IOException
    {
        FileUtils.deleteRecursively( new File( instance.getStoreDir() ) );
    }

    private File deleteAllLogsOn( HighlyAvailableGraphDatabase instance )
    {
        File instanceDir = new File( instance.getStoreDir() );
        for ( File file : instanceDir.listFiles( new LogFiles.LogicalLogFilenameFilter() ) )
        {
            FileUtils.deleteFile( file );
        }
        return instanceDir;
    }

    private void assertAllStoreConsistent() throws ConsistencyCheckIncompleteException
    {
        assertAllStoreConsistent( master, slave1, slave2 );
    }

    private void assertAllStoreConsistent( HighlyAvailableGraphDatabase master,
                                           HighlyAvailableGraphDatabase... slaves )
            throws ConsistencyCheckIncompleteException
    {
        assertConsistentStore( new File( master.getStoreDir() ) );
        for ( HighlyAvailableGraphDatabase slave : slaves )
        {
            assertConsistentStore( new File( slave.getStoreDir() ) );
        }
    }
}
