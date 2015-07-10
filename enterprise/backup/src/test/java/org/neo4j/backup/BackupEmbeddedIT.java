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
package org.neo4j.backup;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.net.InetAddress;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.neo4j.embedded.CommunityTestGraphDatabase;
import org.neo4j.embedded.TestGraphDatabase;
import org.neo4j.graphdb.DynamicRelationshipType;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Transaction;
import org.neo4j.io.fs.FileUtils;
import org.neo4j.test.DbRepresentation;
import org.neo4j.test.ProcessStreamHandler;

import static org.junit.Assert.assertEquals;
import static org.neo4j.helpers.Settings.osIsWindows;
import static org.neo4j.test.TargetDirectory.forTest;

public class BackupEmbeddedIT
{
    public final File PATH = forTest( BackupEmbeddedIT.class ).cleanDirectory( "db" );
    public final File BACKUP_PATH = forTest( BackupEmbeddedIT.class ).cleanDirectory( "backup-db" );

    private GraphDatabaseService db;
    private String ip;

    @Before
    public void before() throws Exception
    {
        if ( osIsWindows() ) return;
        ip = InetAddress.getLocalHost().getHostAddress();
    }

    @After
    public void after() throws Exception
    {
        if ( db != null )
        {
            db.shutdown();
        }
        FileUtils.deleteRecursively( PATH );
        FileUtils.deleteRecursively( BACKUP_PATH );
    }

    @SuppressWarnings("deprecation")
    public static DbRepresentation createSomeData( GraphDatabaseService db )
    {
        try (Transaction tx = db.beginTx())
        {
            Node node = db.createNode();
            node.setProperty( "name", "Neo" );
            db.createNode().createRelationshipTo( node, DynamicRelationshipType.withName( "KNOWS" ) );
            tx.success();
        }
        return DbRepresentation.of( db );
    }

    @Test
    public void makeSureBackupCanBePerformedWithDefaultPort() throws Exception
    {
        if ( osIsWindows() ) return;
        startDb( null );
        assertEquals(
                0,
                runBackupToolFromOtherJvmToGetExitCode( "-from",
                        BackupTool.DEFAULT_SCHEME + "://"+ ip, "-to",
                        BACKUP_PATH.getPath() ) );
        assertEquals( DbRepresentation.of( db ), DbRepresentation.of( BACKUP_PATH ) );
        createSomeData( db );
        assertEquals(
                0,
                runBackupToolFromOtherJvmToGetExitCode( "-from", BackupTool.DEFAULT_SCHEME + "://"+ ip,
                        "-to", BACKUP_PATH.getPath() ) );
        assertEquals( DbRepresentation.of( db ), DbRepresentation.of( BACKUP_PATH ) );
    }

    @Test
    public void makeSureBackupCanBePerformedWithCustomPort() throws Exception
    {
        if ( osIsWindows() ) return;
        int port = 4445;
        startDb( "" + port );
        assertEquals(
                1,
                runBackupToolFromOtherJvmToGetExitCode( "-from",
                        BackupTool.DEFAULT_SCHEME + "://" + ip, "-to",
                        BACKUP_PATH.getPath() ) );
        assertEquals(
                0,
                runBackupToolFromOtherJvmToGetExitCode( "-from",
                        BackupTool.DEFAULT_SCHEME + "://"+ ip +":" + port,
                        "-to", BACKUP_PATH.getPath() ) );
        assertEquals( DbRepresentation.of( db ), DbRepresentation.of( BACKUP_PATH ) );
        createSomeData( db );
        assertEquals(
                0,
                runBackupToolFromOtherJvmToGetExitCode( "-from", BackupTool.DEFAULT_SCHEME + "://"+ ip +":"
                                 + port, "-to",
                        BACKUP_PATH.getPath() ) );
        assertEquals( DbRepresentation.of( db ), DbRepresentation.of( BACKUP_PATH ) );
    }

    private void startDb( String backupPort )
    {
        TestGraphDatabase.Builder graphDbBuilder = CommunityTestGraphDatabase.build();
        graphDbBuilder.withSetting( OnlineBackupSettings.online_backup_enabled, "true" );
        if ( backupPort != null )
        {
            graphDbBuilder.withSetting( OnlineBackupSettings.online_backup_server, ip + ":" + backupPort );
        }
        db = graphDbBuilder.open( PATH );
        createSomeData( db );
    }

    public static int runBackupToolFromOtherJvmToGetExitCode( String... args )
            throws Exception
    {
        List<String> allArgs = new ArrayList<>( Arrays.asList( "java", "-cp", System.getProperty( "java.class.path" ), BackupTool.class.getName() ) );
        allArgs.addAll( Arrays.asList( args ) );

        Process process = Runtime.getRuntime().exec( allArgs.toArray( new String[allArgs.size()] ));
        return new ProcessStreamHandler( process, false ).waitForResult();
    }
}
