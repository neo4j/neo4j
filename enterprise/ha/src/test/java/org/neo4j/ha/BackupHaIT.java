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

import java.util.ArrayList;
import java.util.List;

import org.apache.commons.io.FileUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

import org.neo4j.backup.OnlineBackupSettings;
import org.neo4j.cluster.InstanceId;
import org.neo4j.graphdb.factory.GraphDatabaseBuilder;
import org.neo4j.helpers.Settings;
import org.neo4j.helpers.collection.MapUtil;
import org.neo4j.test.DbRepresentation;
import org.neo4j.test.ha.ClusterManager;
import org.neo4j.test.ha.ClusterManager.ManagedCluster;

import static org.junit.Assert.assertEquals;

import static org.neo4j.backup.BackupEmbeddedIT.BACKUP_PATH;
import static org.neo4j.backup.BackupEmbeddedIT.PATH;
import static org.neo4j.backup.BackupEmbeddedIT.createSomeData;
import static org.neo4j.backup.BackupEmbeddedIT.runBackupToolFromOtherJvmToGetExitCode;
import static org.neo4j.test.ha.ClusterManager.fromXml;

@Ignore("Breaks occasionally, needs investigation")
public class BackupHaIT
{
    private DbRepresentation representation;
    private ClusterManager clusterManager;
    private ManagedCluster cluster;

    @Before
    public void startCluster() throws Throwable
    {
        FileUtils.deleteDirectory( PATH );
        FileUtils.deleteDirectory( BACKUP_PATH );

        clusterManager = new ClusterManager( fromXml( getClass().getResource( "/threeinstances.xml" ).toURI() ),
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

        // Really doesn't matter which instance
        representation = createSomeData( cluster.getMaster() );
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
                backupArguments( "localhost:5001", BACKUP_PATH.getPath(), "non.existent" ) ) );
    }

    private void testBackupFromCluster( String askForCluster ) throws Throwable
    {
        assertEquals( 0, runBackupToolFromOtherJvmToGetExitCode(
                backupArguments( "localhost:5001", BACKUP_PATH.getPath(), askForCluster ) ) );
        assertEquals( representation, DbRepresentation.of( BACKUP_PATH ) );
        ManagedCluster cluster = clusterManager.getCluster( askForCluster == null ? "neo4j.ha" : askForCluster );
        DbRepresentation newRepresentation = createSomeData( cluster.getAnySlave() );
        assertEquals( 0, runBackupToolFromOtherJvmToGetExitCode(
                backupArguments( "localhost:5001", BACKUP_PATH.getPath(), askForCluster ) ) );
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
