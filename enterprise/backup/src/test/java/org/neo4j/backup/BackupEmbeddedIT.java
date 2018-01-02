/*
 * Copyright (c) 2002-2018 "Neo Technology,"
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

import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.SystemUtils;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;

import java.io.File;
import java.net.InetAddress;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.neo4j.graphdb.DynamicRelationshipType;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Transaction;
import org.neo4j.kernel.configuration.Settings;
import org.neo4j.test.DbRepresentation;
import org.neo4j.test.EmbeddedDatabaseRule;
import org.neo4j.test.ProcessStreamHandler;
import org.neo4j.test.TargetDirectory;

import static org.junit.Assert.assertEquals;

public class BackupEmbeddedIT
{
    @ClassRule
    public static TargetDirectory.TestDirectory testDirectory =
            TargetDirectory.testDirForTest( BackupEmbeddedIT.class );

    public final File backupPath = testDirectory.directory( "backup-db" );

    @Rule
    public EmbeddedDatabaseRule db = new EmbeddedDatabaseRule( testDirectory.directory( "db" ) ).startLazily();
    private String ip;

    @Before
    public void before() throws Exception
    {
        if ( SystemUtils.IS_OS_WINDOWS ) return;
        FileUtils.deleteDirectory( backupPath );
        ip = InetAddress.getLocalHost().getHostAddress();
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
        if ( SystemUtils.IS_OS_WINDOWS ) return;
        startDb( null );
        assertEquals(
                0,
                runBackupToolFromOtherJvmToGetExitCode( "-from",
                        BackupTool.DEFAULT_SCHEME + "://" + ip, "-to",
                        backupPath.getPath() ) );
        assertEquals( DbRepresentation.of( db ), DbRepresentation.of( backupPath ) );
        createSomeData( db );
        assertEquals(
                0,
                runBackupToolFromOtherJvmToGetExitCode( "-from", BackupTool.DEFAULT_SCHEME + "://"+ ip,
                        "-to", backupPath.getPath() ) );
        assertEquals( DbRepresentation.of( db ), DbRepresentation.of( backupPath ) );
    }

    @Test
    public void makeSureBackupCanBePerformedWithCustomPort() throws Exception
    {
        if ( SystemUtils.IS_OS_WINDOWS ) return;
        int port = 4445;
        startDb( "" + port );
        assertEquals(
                1,
                runBackupToolFromOtherJvmToGetExitCode( "-from",
                        BackupTool.DEFAULT_SCHEME + "://" + ip, "-to",
                        backupPath.getPath() ) );
        assertEquals(
                0,
                runBackupToolFromOtherJvmToGetExitCode( "-from",
                        BackupTool.DEFAULT_SCHEME + "://" + ip + ":" + port,
                        "-to", backupPath.getPath() ) );
        assertEquals( DbRepresentation.of( db ), DbRepresentation.of( backupPath ) );
        createSomeData( db );
        assertEquals(
                0,
                runBackupToolFromOtherJvmToGetExitCode( "-from", BackupTool.DEFAULT_SCHEME + "://"+ ip +":"
                                 + port, "-to",
                        backupPath.getPath() ) );
        assertEquals( DbRepresentation.of( db ), DbRepresentation.of( backupPath ) );
    }

    private void startDb( String backupPort )
    {
        db.setConfig( OnlineBackupSettings.online_backup_enabled, Settings.TRUE );
        if(backupPort != null)
        {
            db.setConfig( OnlineBackupSettings.online_backup_server, ip +":" + backupPort );
        }
        db.ensureStarted();
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
