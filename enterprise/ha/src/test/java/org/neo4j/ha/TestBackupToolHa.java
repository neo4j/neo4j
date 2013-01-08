/**
 * Copyright (c) 2002-2013 "Neo Technology,"
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

import static org.junit.Assert.assertEquals;
import static org.neo4j.backup.BackupEmbeddedIT.BACKUP_PATH;
import static org.neo4j.backup.BackupEmbeddedIT.PATH;
import static org.neo4j.backup.BackupEmbeddedIT.createSomeData;
import static org.neo4j.backup.BackupEmbeddedIT.runBackupToolFromOtherJvmToGetExitCode;
import static org.neo4j.helpers.collection.MapUtil.stringMap;

import java.io.File;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.commons.io.FileUtils;
import org.junit.After;
import org.junit.Test;
import org.neo4j.backup.OnlineBackupSettings;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.factory.GraphDatabaseSetting;
import org.neo4j.graphdb.index.IndexProvider;
import org.neo4j.helpers.Service;
import org.neo4j.kernel.HighlyAvailableGraphDatabase;
import org.neo4j.kernel.KernelExtension;
import org.neo4j.kernel.ha.HaSettings;
import org.neo4j.kernel.impl.cache.CacheProvider;
import org.neo4j.test.DbRepresentation;
import org.neo4j.test.ha.LocalhostZooKeeperCluster;

public class TestBackupToolHa
{
    private LocalhostZooKeeperCluster zk;
    private List<GraphDatabaseService> instances;
    private DbRepresentation representation;
    
    public void startCluster( String clusterName ) throws Exception
    {
        FileUtils.deleteDirectory( new File( PATH ) );
        FileUtils.deleteDirectory( new File( BACKUP_PATH ) );

        zk = LocalhostZooKeeperCluster.singleton().clearDataAndVerifyConnection();
        instances = new ArrayList<GraphDatabaseService>();
        for ( int i = 0; i < 3; i++ )
        {
            String storeDir = new File( PATH, "" + i ).getAbsolutePath();
            Map<String, String> config = stringMap(
                HaSettings.server_id.name(), "" + i,
                    HaSettings.server.name(), "localhost:" + (6666+i),
                    HaSettings.coordinators.name(), zk.getConnectionString(),
                    OnlineBackupSettings.online_backup_enabled.name(), GraphDatabaseSetting.TRUE,
                    OnlineBackupSettings.online_backup_port.name(), ""+(4444+i) );
            if ( clusterName != null )
                config.put( HaSettings.cluster_name.name(), clusterName );
            GraphDatabaseService instance = new HighlyAvailableGraphDatabase( storeDir, config,
                    Service.load( IndexProvider.class ), Service.load( KernelExtension.class ), Service.load( CacheProvider.class ) );
            instances.add( instance );
        }
        
        // Really doesn't matter which instance
        representation = createSomeData( instances.get( 1 ) );
    }
    
    @After
    public void after() throws Exception
    {
        if( instances != null ) 
        {
            for ( GraphDatabaseService instance : instances )
            {
                instance.shutdown();
            }
        }
    }
    
    @Test
    public void makeSureBackupCanBePerformedFromClusterWithDefaultName() throws Exception
    {
        testBackupFromCluster( null, null );
    }

    @Test
    public void makeSureBackupCanBePerformedFromClusterWithCustomName() throws Exception
    {
        String clusterName = "local.jvm.cluster";
        testBackupFromCluster( clusterName, clusterName );
    }
    
    @Test
    public void makeSureBackupCannotBePerformedFromNonExistentCluster() throws Exception
    {
        String clusterName = "local.jvm.cluster";
        startCluster( clusterName );
        assertEquals( 1, runBackupToolFromOtherJvmToGetExitCode(
                backupArguments( true, "ha://localhost:2181", BACKUP_PATH, null ) ) );
    }
    
    private void testBackupFromCluster( String clusterName, String askForCluster ) throws Exception
    {
        startCluster( clusterName );
        assertEquals( 0, runBackupToolFromOtherJvmToGetExitCode(
                backupArguments( true, "ha://localhost:2181", BACKUP_PATH, clusterName ) ) );
        assertEquals( representation, DbRepresentation.of( BACKUP_PATH ) );
        DbRepresentation newRepresentation = createSomeData( instances.get( 2 ) );
        assertEquals( 0, runBackupToolFromOtherJvmToGetExitCode(
                backupArguments( false, "ha://localhost:2182", BACKUP_PATH, clusterName ) ) );
        assertEquals( newRepresentation, DbRepresentation.of( BACKUP_PATH ) );
    }
    
    private String[] backupArguments( boolean trueForFull, String from, String to, String clusterName )
    {
        List<String> args = new ArrayList<String>();
        args.add( trueForFull ? "-full" : "-incremental" );
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
