/**
 * Copyright (c) 2002-2011 "Neo Technology,"
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
package org.neo4j.backup;

import static org.junit.Assert.assertEquals;
import static org.neo4j.helpers.collection.MapUtil.stringMap;
import static org.neo4j.kernel.Config.ENABLE_ONLINE_BACKUP;

import java.io.File;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.commons.io.FileUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.neo4j.graphdb.DynamicRelationshipType;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Transaction;
import org.neo4j.kernel.EmbeddedGraphDatabase;
import org.neo4j.test.DbRepresentation;

public class TestBackupToolEmbedded
{
    static final String PATH = "target/var/db";
    static final String BACKUP_PATH = "target/var/backup-db";
    private GraphDatabaseService db;
    
    @Before
    public void before() throws Exception
    {
        FileUtils.deleteDirectory( new File( PATH ) );
        FileUtils.deleteDirectory( new File( BACKUP_PATH ) );
    }
    
    static DbRepresentation createSomeData( GraphDatabaseService db )
    {
        Transaction tx = db.beginTx();
        Node node = db.createNode();
        node.setProperty( "name", "Neo" );
        db.getReferenceNode().createRelationshipTo( node, DynamicRelationshipType.withName( "KNOWS" ) );
        tx.success();
        tx.finish();
        return DbRepresentation.of( db );
    }
    
    @After
    public void after()
    {
        db.shutdown();
    }
    
    @Test
    public void makeSureBackupCannotBePerformedWithInvalidArgs() throws Exception
    {
        startDb( "true" );
        assertEquals( 1, runBackupToolFromOtherJvmToGetExitCode() );
        assertEquals( 1, runBackupToolFromOtherJvmToGetExitCode( "-full" ) );
        assertEquals( 1, runBackupToolFromOtherJvmToGetExitCode( "-incremental" ) );
        assertEquals( 1, runBackupToolFromOtherJvmToGetExitCode( "-from", "localhost" ) );
        assertEquals( 1, runBackupToolFromOtherJvmToGetExitCode( "-to", "some-dir" ) );
        assertEquals( 1, runBackupToolFromOtherJvmToGetExitCode( "-from-ha", "localhost:2181" ) );
        assertEquals( 1, runBackupToolFromOtherJvmToGetExitCode( "-full", "-from", "localhost", "-to", "some-dir", "-incremental" ) );
        assertEquals( 1, runBackupToolFromOtherJvmToGetExitCode( "-full", "-from", "localhost", "-from-ha", "localhost:2181", "-to", "some-dir" ) );
    }
    
    @Test
    public void makeSureBackupCanBePerformedWithDefaultPort() throws Exception
    {
        startDb( "true" );
        assertEquals( 0, runBackupToolFromOtherJvmToGetExitCode( "-full", "-from", "localhost", "-to", BACKUP_PATH ) );
        assertEquals( DbRepresentation.of( db ), DbRepresentation.of( BACKUP_PATH ) );
        createSomeData( db );
        assertEquals( 0, runBackupToolFromOtherJvmToGetExitCode( "-incremental", "-from", "localhost", "-to", BACKUP_PATH ) );
        assertEquals( DbRepresentation.of( db ), DbRepresentation.of( BACKUP_PATH ) );
    }

    @Test
    public void makeSureBackupCanBePerformedWithCustomPort() throws Exception
    {
        int port = 4445;
        startDb( "port=" + port );
        assertEquals( 1, runBackupToolFromOtherJvmToGetExitCode( "-full", "-from", "localhost", "-to", BACKUP_PATH ) );
        assertEquals( 0, runBackupToolFromOtherJvmToGetExitCode( "-full", "-from", "localhost:" + port, "-to", BACKUP_PATH ) );
        assertEquals( DbRepresentation.of( db ), DbRepresentation.of( BACKUP_PATH ) );
        createSomeData( db );
        assertEquals( 0, runBackupToolFromOtherJvmToGetExitCode( "-incremental", "-from", "localhost:" + port, "-to", BACKUP_PATH ) );
        assertEquals( DbRepresentation.of( db ), DbRepresentation.of( BACKUP_PATH ) );
    }
    
    private void startDb( String backupConfigValue )
    {
        if ( backupConfigValue == null )
        {
            db = new EmbeddedGraphDatabase( PATH );
        }
        else
        {
            db = new EmbeddedGraphDatabase( PATH, stringMap( ENABLE_ONLINE_BACKUP, backupConfigValue ) );
        }
        createSomeData( db );
    }
    
    static int runBackupToolFromOtherJvmToGetExitCode( String... args ) throws Exception
    {
        List<String> allArgs = new ArrayList<String>( Arrays.asList( "java", "-cp", System.getProperty( "java.class.path" ), Backup.class.getName() ) );
        allArgs.addAll( Arrays.asList( args ) );
        return Runtime.getRuntime().exec( allArgs.toArray( new String[allArgs.size()] ) ).waitFor();
    }
}
