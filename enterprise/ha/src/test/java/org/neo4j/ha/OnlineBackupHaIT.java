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
package org.neo4j.ha;

import java.io.IOException;
import java.util.concurrent.CountDownLatch;

import org.apache.commons.io.FileUtils;
import org.junit.Before;
import org.junit.Test;

import org.neo4j.backup.OnlineBackup;
import org.neo4j.backup.OnlineBackupSettings;
import org.neo4j.cluster.InstanceId;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.factory.GraphDatabaseBuilder;
import org.neo4j.helpers.Settings;
import org.neo4j.helpers.collection.MapUtil;
import org.neo4j.kernel.ha.HighlyAvailableGraphDatabase;
import org.neo4j.test.ha.ClusterManager;
import org.neo4j.test.ha.ClusterManager.ManagedCluster;
import org.neo4j.test.ha.OnlineBackupClusterManager;

import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.junit.Assert.assertThat;

import static org.neo4j.backup.BackupEmbeddedIT.BACKUP_PATH;
import static org.neo4j.backup.BackupEmbeddedIT.PATH;
import static org.neo4j.test.ha.ClusterManager.allSeesAllAsAvailable;
import static org.neo4j.test.ha.ClusterManager.fromXml;

public class OnlineBackupHaIT
{
    private ClusterManager clusterManager;
    private ManagedCluster cluster;

    private boolean doneBackup = false;
    private int nodeCount = 1;

    private final CountDownLatch doneDataWriting = new CountDownLatch( 1 );
    private final CountDownLatch startedDataWriting = new CountDownLatch( 10 );

    private final static int BACKUP_PORT = 6363;

    @Before
    public void cleanUpFolder() throws IOException
    {
        FileUtils.deleteDirectory( PATH );
        FileUtils.deleteDirectory( BACKUP_PATH );
    }

    private void startCluster() throws Throwable
    {
        clusterManager = new ClusterManager( fromXml( getClass().getResource( "/threeinstances.xml" ).toURI() ), PATH,
                MapUtil.stringMap( OnlineBackupSettings.online_backup_enabled.name(), Settings.TRUE ) )
        {
            @Override
            protected void config( GraphDatabaseBuilder builder, String clusterName, InstanceId serverId )
            {
                builder.setConfig( OnlineBackupSettings.online_backup_server,
                        (":" + (BACKUP_PORT - 1 + serverId.toIntegerIndex())) );
            }
        };
        clusterManager.start();
        cluster = clusterManager.getDefaultCluster();
    }

    private void startClusterFromBackup() throws Throwable
    {
        clusterManager = new OnlineBackupClusterManager( fromXml( getClass().getResource( "/threeinstances.xml" )
                .toURI() ), BACKUP_PATH, MapUtil.stringMap( OnlineBackupSettings.online_backup_enabled.name(),
                        Settings.TRUE ) );
        clusterManager.start();
        cluster = clusterManager.getDefaultCluster();
    }

    private void stopCluster() throws Throwable
    {
        clusterManager.stop();
        clusterManager.shutdown();
    }

    @Test
    public void shouldRestartClusterWithOnlineBackupDb() throws Throwable
    {
        // Given
        startCluster();
        performOnlineBackupAndDataWritingConcurrently();
        doneDataWriting.await();
        stopCluster();

        // When
        startClusterFromBackup();

        // Then
        int nodeCountInBackupStore = getNodeCountInStore();
        assertThat( nodeCount, greaterThanOrEqualTo( nodeCountInBackupStore ) );
        assertThat( nodeCountInBackupStore, greaterThanOrEqualTo( 10 ) );
        stopCluster();
    }

    private void performOnlineBackupAndDataWritingConcurrently()
    {
        new Thread( new Runnable()
        {
            private final GraphDatabaseService db = cluster.getMaster();
            @Override
            public void run()
            {
                while ( true )
                {
                    // keep on writing data until backup is done
                    Transaction tx = db.beginTx();
                    Node node = db.createNode();
                    node.setProperty( "name", "node" + nodeCount );
                    tx.success();
                    tx.finish();

                    nodeCount++;
                    startedDataWriting.countDown();

                    if ( doneBackup )
                    {
                        break;
                    }
                }
                doneDataWriting.countDown();

                System.out.println("***Done data writing***");
            }
        } ).start();

        new Thread( new Runnable()
        {
            @Override
            public void run()
            {
                // wait db to have some data first
                try
                {
                    startedDataWriting.await();
                }
                catch ( InterruptedException e )
                {
                    e.printStackTrace();
                }

                // perform backup
                OnlineBackup backup = OnlineBackup.from( "127.0.0.1", BACKUP_PORT );
                backup.backup( BACKUP_PATH.getPath() );
                doneBackup = true;

                System.out.println( "***Done backup***" );
            }
        } ).start();
    }

    private int getNodeCountInStore()
    {
        cluster.await( allSeesAllAsAvailable() );
        HighlyAvailableGraphDatabase db = cluster.getMaster();
        try ( Transaction tx = db.beginTx() )
        {
            int count = 0;
            for ( Node node : db.getAllNodes() )
            {
                count++;
            }
            tx.success();
            return count;
        }
    }
}
