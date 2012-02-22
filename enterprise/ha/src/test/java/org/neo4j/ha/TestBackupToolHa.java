/**
 * Copyright (c) 2002-2012 "Neo Technology,"
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
import static org.neo4j.backup.TestBackupToolEmbedded.BACKUP_PATH;
import static org.neo4j.backup.TestBackupToolEmbedded.PATH;
import static org.neo4j.backup.TestBackupToolEmbedded.createSomeData;
import static org.neo4j.backup.TestBackupToolEmbedded.runBackupToolFromOtherJvmToGetExitCode;
import static org.neo4j.helpers.collection.MapUtil.stringMap;
import static org.neo4j.kernel.Config.ENABLE_ONLINE_BACKUP;
import static org.neo4j.kernel.HaConfig.CONFIG_KEY_COORDINATORS;
import static org.neo4j.kernel.HaConfig.CONFIG_KEY_SERVER;
import static org.neo4j.kernel.HaConfig.CONFIG_KEY_SERVER_ID;

import java.io.File;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.commons.io.FileUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.kernel.HighlyAvailableGraphDatabase;
import org.neo4j.test.DbRepresentation;
import org.neo4j.test.ha.LocalhostZooKeeperCluster;

public class TestBackupToolHa
{
    private LocalhostZooKeeperCluster zk;
    private List<GraphDatabaseService> instances;
    private DbRepresentation representation;
    
    @Before
    public void before() throws Exception
    {
        FileUtils.deleteDirectory( new File( PATH ) );
        FileUtils.deleteDirectory( new File( BACKUP_PATH ) );

        zk = new LocalhostZooKeeperCluster( TestBackupToolHa.class, 2181, 2182, 2183 );
        instances = new ArrayList<GraphDatabaseService>();
        for ( int i = 0; i < 3; i++ )
        {
            String storeDir = new File( PATH, "" + i ).getAbsolutePath();
            Map<String, String> config = stringMap(
                    CONFIG_KEY_SERVER_ID, "" + i,
                    CONFIG_KEY_SERVER, "localhost:" + (6666+i),
                    CONFIG_KEY_COORDINATORS, zk.getConnectionString(),
                    ENABLE_ONLINE_BACKUP, "port=" + (4444+i) );
            GraphDatabaseService instance = new HighlyAvailableGraphDatabase( storeDir, config );
            instances.add( instance );
        }
        
        // Really doesn't matter which instance
        representation = createSomeData( instances.get( 1 ) );
    }
    
    @After
    public void after() throws Exception
    {
        for ( GraphDatabaseService instance : instances )
        {
            instance.shutdown();
        }
        zk.shutdown();
    }
    
    @Test
    public void makeSureBackupCanBePerformedFromCluster() throws Exception
    {
        assertEquals(
                0,
                runBackupToolFromOtherJvmToGetExitCode( "-full", "-from",
                        "ha://localhost:2181", "-to", BACKUP_PATH ) );
        assertEquals( representation, DbRepresentation.of( BACKUP_PATH ) );
        DbRepresentation newRepresentation = createSomeData( instances.get( 2 ) );
        assertEquals(
                0,
                runBackupToolFromOtherJvmToGetExitCode( "-incremental",
                        "-from", "ha://localhost:2182", "-to", BACKUP_PATH ) );
        assertEquals( newRepresentation, DbRepresentation.of( BACKUP_PATH ) );
    }
}
