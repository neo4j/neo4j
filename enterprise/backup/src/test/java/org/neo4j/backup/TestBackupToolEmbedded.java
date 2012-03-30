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
package org.neo4j.backup;

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
import org.neo4j.kernel.impl.util.StringLogger;
import org.neo4j.test.DbRepresentation;
import org.neo4j.test.ProcessStreamHandler;

import static org.junit.Assert.*;
import static org.neo4j.graphdb.factory.GraphDatabaseSetting.*;
import static org.neo4j.helpers.collection.MapUtil.*;

public class TestBackupToolEmbedded
{
    public static final String PATH = "target/var/db";
    public static final String BACKUP_PATH = "target/var/backup-db";
    private GraphDatabaseService db;

    @Before
    public void before() throws Exception
    {
        if ( osIsWindows() ) return;
        FileUtils.deleteDirectory( new File( PATH ) );
        FileUtils.deleteDirectory( new File( BACKUP_PATH ) );
    }

    public static DbRepresentation createSomeData( GraphDatabaseService db )
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
        if ( osIsWindows() ) return;
        db.shutdown();
    }

    @Test
    public void makeSureBackupCannotBePerformedWithInvalidArgs() throws Exception
    {
        if ( osIsWindows() ) return;
        startDb( null );
        // No args at all
        assertEquals( 1, runBackupToolFromOtherJvmToGetExitCode() );
        // no targets
        assertEquals( 1, runBackupToolFromOtherJvmToGetExitCode( "-full" ) );
        assertEquals( 1, runBackupToolFromOtherJvmToGetExitCode( "-incremental" ) );
        // Invalid from and no to
        assertEquals(
                1,
                runBackupToolFromOtherJvmToGetExitCode( "-from", "localhost" ) );
        // invalid from and no to
        assertEquals(
                1,
                runBackupToolFromOtherJvmToGetExitCode( "-from",
                        "foo:localhost:123" ) );
        // no from with to
        assertEquals( 1, runBackupToolFromOtherJvmToGetExitCode( "-to", "some-dir" ) );
        // all in place but both modes
        assertEquals(
                1,
                runBackupToolFromOtherJvmToGetExitCode( "-full", "-from",
                        "foo://localhost", "-to", "some-dir", "-incremental" ) );
        // all in place, typo in from uri
        assertEquals(
                1,
                runBackupToolFromOtherJvmToGetExitCode( "-full", "-from",
                        "foo:/localhost", "-to", "some-dir" ) );
    }

    @Test
    public void makeSureBackupCanBePerformedWithDefaultPort() throws Exception
    {
        if ( osIsWindows() ) return;
        startDb( null );
        assertEquals(
                0,
                runBackupToolFromOtherJvmToGetExitCode( "-full", "-from",
                        BackupTool.DEFAULT_SCHEME + "://localhost", "-to",
                        BACKUP_PATH ) );
        assertEquals( DbRepresentation.of( db ), DbRepresentation.of( BACKUP_PATH ) );
        createSomeData( db );
        assertEquals(
                0,
                runBackupToolFromOtherJvmToGetExitCode( "-incremental",
                        "-from", BackupTool.DEFAULT_SCHEME + "://localhost",
                        "-to", BACKUP_PATH ) );
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
                runBackupToolFromOtherJvmToGetExitCode( "-full", "-from",
                        BackupTool.DEFAULT_SCHEME + "://localhost", "-to",
                        BACKUP_PATH ) );
        assertEquals(
                0,
                runBackupToolFromOtherJvmToGetExitCode( "-full", "-from",
                        BackupTool.DEFAULT_SCHEME + "://localhost:" + port,
                        "-to", BACKUP_PATH ) );
        assertEquals( DbRepresentation.of( db ), DbRepresentation.of( BACKUP_PATH ) );
        createSomeData( db );
        assertEquals(
                0,
                runBackupToolFromOtherJvmToGetExitCode( "-incremental",
                        "-from", BackupTool.DEFAULT_SCHEME + "://localhost:"
                                 + port, "-to",
                        BACKUP_PATH ) );
        assertEquals( DbRepresentation.of( db ), DbRepresentation.of( BACKUP_PATH ) );
    }

    private void startDb( String backupPort )
    {
        db = new EmbeddedGraphDatabase( PATH, stringMap( "online_backup_enabled", "true", "online_backup_port", backupPort) )
        {
            @Override
            protected StringLogger createStringLogger()
            {
                return StringLogger.SYSTEM;
            }
        };
        createSomeData( db );
    }

    public static int runBackupToolFromOtherJvmToGetExitCode( String... args )
            throws Exception
    {
        List<String> allArgs = new ArrayList<String>( Arrays.asList( "java", "-cp", System.getProperty( "java.class.path" ), BackupTool.class.getName() ) );
        allArgs.addAll( Arrays.asList( args ) );

        Process process = Runtime.getRuntime().exec( allArgs.toArray( new String[allArgs.size()] ));
        return new ProcessStreamHandler( process, true ).waitForResult();
    }
}
