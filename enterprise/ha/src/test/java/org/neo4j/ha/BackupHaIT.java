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
package org.neo4j.ha;

import org.apache.commons.io.FileUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import org.neo4j.backup.OnlineBackupSettings;
import org.neo4j.cluster.InstanceId;
import org.neo4j.graphdb.factory.GraphDatabaseBuilder;
import org.neo4j.helpers.Settings;
import org.neo4j.helpers.collection.MapUtil;
import org.neo4j.test.DbRepresentation;
import org.neo4j.test.TargetDirectory;
import org.neo4j.test.ha.ClusterManager;
import org.neo4j.test.ha.ClusterManager.ManagedCluster;

import static org.hamcrest.Matchers.equalTo;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;
import static org.neo4j.backup.BackupEmbeddedIT.createSomeData;
import static org.neo4j.backup.BackupEmbeddedIT.runBackupToolFromOtherJvmToGetExitCode;
import static org.neo4j.test.ha.ClusterManager.allSeesAllAsAvailable;

public class BackupHaIT
{
    public static final File PATH = TargetDirectory.forTest( BackupHaIT.class ).cleanDirectory( "db" );
    public static final File BACKUP_PATH = TargetDirectory.forTest( BackupHaIT.class ).cleanDirectory( "backup" +
            "-db" );

    private DbRepresentation representation;
    private ClusterManager clusterManager;
    private ManagedCluster cluster;

    @Before
    public void setup() throws Throwable
    {
        FileUtils.deleteDirectory( PATH );
        FileUtils.deleteDirectory( BACKUP_PATH );

        startCluster();

        // Really doesn't matter which instance
        representation = createSomeData( cluster.getMaster() );
    }

    private void startCluster() throws Throwable
    {
        clusterManager = new ClusterManager( ClusterManager.fromXml( getClass().getResource( "/threeinstances.xml" )
                .toURI() ),
                PATH, MapUtil.stringMap( OnlineBackupSettings.online_backup_enabled.name(),
                Settings.TRUE ) )
        {
            @Override
            protected void config( GraphDatabaseBuilder builder, String clusterName, InstanceId serverId )
            {
                builder.setConfig( OnlineBackupSettings.online_backup_server, (":"+(4444 + serverId.toIntegerIndex()) ));
            }
        };
        clusterManager.start();
        cluster = clusterManager.getDefaultCluster();
        cluster.await( allSeesAllAsAvailable() );
    }

    @After
    public void stopCluster() throws Throwable
    {
        clusterManager.stop();
        clusterManager.shutdown();
    }

    @Test
    public void makeSureBackupCanBePerformedFromClusterWithDefaultName() throws Throwable
    {
        testBackupFromCluster( null );
    }

    @Test
    public void makeSureBackupCanBePerformedFromWronglyNamedCluster() throws Throwable
    {
        assertEquals( 0, runBackupToolFromOtherJvmToGetExitCode(
                backupArguments( "localhost:4445", BACKUP_PATH.getPath(), "non.existent" ) ) );
    }

    @Test
    public void makeSureBackupCanBeRestored() throws Throwable
    {
        // Run backup
        assertEquals( 0, runBackupToolFromOtherJvmToGetExitCode( backupArguments( "localhost:4445",
                BACKUP_PATH.getPath(), null ) ) );

        // Add some new data
        DbRepresentation changedData = createSomeData( cluster.getMaster() );

        stopCluster();

        cleanData();

        copyBackup();

        startCluster();

        // Verify that old data is back
        assertThat( changedData.equals( DbRepresentation.of( cluster.getMaster() ) ), equalTo(false) );
    }

    @Test
    public void makeSureBackupCanBePerformedFromAnyInstance() throws Throwable
    {
        Integer[] backupPorts = {4445, 4446, 4447};

        for ( Integer port : backupPorts )
        {
            // Run backup
            assertEquals( 0, runBackupToolFromOtherJvmToGetExitCode( backupArguments( "localhost:" + port,
                    BACKUP_PATH.getPath(), null ) ) );

            // Add some new data
            DbRepresentation changedData = createSomeData( cluster.getMaster() );

            stopCluster();

            cleanData();

            copyBackup();

            startCluster();

            // Verify that old data is back
            assertThat( changedData.equals( DbRepresentation.of( cluster.getMaster() ) ), equalTo(false) );
        }
    }


    private void copyBackup() throws IOException
    {
        FileUtils.copyDirectory( BACKUP_PATH, new File( PATH, "neo4j.ha/server1" ) );
        FileUtils.copyDirectory( BACKUP_PATH, new File(PATH, "neo4j.ha/server2") );
        FileUtils.copyDirectory( BACKUP_PATH, new File( PATH, "neo4j.ha/server3" ) );
    }

    private void cleanData() throws IOException
    {
        FileUtils.cleanDirectory( new File( PATH, "neo4j.ha/server1" ) );
        FileUtils.cleanDirectory( new File(PATH, "neo4j.ha/server2"));
        FileUtils.cleanDirectory( new File( PATH, "neo4j.ha/server3" ) );
    }

    private void testBackupFromCluster( String askForCluster ) throws Throwable
    {
        assertEquals( 0, runBackupToolFromOtherJvmToGetExitCode(
                backupArguments( "localhost:4445", BACKUP_PATH.getPath(), askForCluster ) ) );
        assertEquals( representation, DbRepresentation.of( BACKUP_PATH ) );
        ManagedCluster cluster = clusterManager.getCluster( askForCluster == null ? "neo4j.ha" : askForCluster );
        DbRepresentation newRepresentation = createSomeData( cluster.getAnySlave() );
        assertEquals( 0, runBackupToolFromOtherJvmToGetExitCode(
                backupArguments( "localhost:4445", BACKUP_PATH.getPath(), askForCluster ) ) );
        assertEquals( newRepresentation, DbRepresentation.of( BACKUP_PATH ) );
    }

    private String[] backupArguments( String from, String to, String clusterName )
    {
        List<String> args = new ArrayList<String>();
        args.add( "-from" );
        args.add( from );
        args.add( "-to" );
        args.add( to );
        if ( clusterName != null )
        {
            args.add( "-cluster" );
            args.add( clusterName );
        }
        return args.toArray( new String[args.size()] );
    }
}
