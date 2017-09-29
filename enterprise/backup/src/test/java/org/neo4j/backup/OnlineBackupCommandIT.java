/*
 * Copyright (c) 2002-2017 "Neo Technology,"
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

import org.apache.commons.lang3.SystemUtils;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.RuleChain;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameter;
import org.junit.runners.Parameterized.Parameters;

import java.io.File;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.neo4j.commandline.admin.AdminTool;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.factory.GraphDatabaseSettings;
import org.neo4j.io.proc.ProcessUtil;
import org.neo4j.kernel.configuration.Settings;
import org.neo4j.kernel.impl.store.format.highlimit.HighLimit;
import org.neo4j.kernel.impl.store.format.standard.Standard;
import org.neo4j.test.DbRepresentation;
import org.neo4j.test.ProcessStreamHandler;
import org.neo4j.test.rule.EmbeddedDatabaseRule;
import org.neo4j.test.rule.SuppressOutput;
import org.neo4j.test.rule.TestDirectory;

import static org.junit.Assert.assertEquals;
import static org.junit.Assume.assumeFalse;

@RunWith( Parameterized.class )
public class OnlineBackupCommandIT
{
    @ClassRule
    public static final TestDirectory testDirectory = TestDirectory.testDirectory();

    private final EmbeddedDatabaseRule db = new EmbeddedDatabaseRule( testDirectory.directory( "db" ) ).startLazily();

    @Rule
    public final RuleChain ruleChain = RuleChain.outerRule( SuppressOutput.suppressAll() ).around( db );

    private static final String ip = "127.0.0.1";
    private final File backupDir = testDirectory.directory( "backups" );

    @Parameter
    public String recordFormat;

    @Parameters( name = "{0}" )
    public static List<String> recordFormats()
    {
        return Arrays.asList( Standard.LATEST_NAME, HighLimit.NAME );
    }

    public static DbRepresentation createSomeData( GraphDatabaseService db )
    {
        try ( Transaction tx = db.beginTx() )
        {
            Node node = db.createNode();
            node.setProperty( "name", "Neo" );
            db.createNode().createRelationshipTo( node, RelationshipType.withName( "KNOWS" ) );
            tx.success();
        }
        return DbRepresentation.of( db );
    }

    @Test
    public void makeSureBackupCanBePerformedWithDefaultPort() throws Exception
    {
        assumeFalse( SystemUtils.IS_OS_WINDOWS );

        startDb( null );
        assertEquals(
                0,
                runBackupToolFromOtherJvmToGetExitCode( "--from", ip,
                        "--cc-report-dir=" + backupDir,
                        "--backup-dir=" + backupDir,
                        "--name=defaultport" ) );
        assertEquals( getDbRepresentation(), getBackupDbRepresentation( "defaultport" ) );
        createSomeData( db );
        assertEquals(
                0,
                runBackupToolFromOtherJvmToGetExitCode( "--from", ip,
                        "--cc-report-dir=" + backupDir,
                        "--backup-dir=" + backupDir,
                        "--name=defaultport" ) );
        assertEquals( getDbRepresentation(), getBackupDbRepresentation( "defaultport" ) );
    }

    @Test
    public void makeSureBackupCanBePerformedWithCustomPort() throws Exception
    {
        assumeFalse( SystemUtils.IS_OS_WINDOWS );

        int port = 4445;
        startDb( "" + port );
        assertEquals(
                1,
                runBackupToolFromOtherJvmToGetExitCode( "--from", ip,
                        "--cc-report-dir=" + backupDir,
                        "--backup-dir=" + backupDir,
                        "--name=customport" ) );
        assertEquals(
                0,
                runBackupToolFromOtherJvmToGetExitCode( "--from",
                        ip + ":" + port,
                        "--cc-report-dir=" + backupDir,
                        "--backup-dir=" + backupDir,
                        "--name=customport" ) );
        assertEquals( getDbRepresentation(), getBackupDbRepresentation( "customport" ) );
        createSomeData( db );
        assertEquals(
                0,
                runBackupToolFromOtherJvmToGetExitCode( "--from", ip + ":" + port,
                        "--cc-report-dir=" + backupDir,
                        "--backup-dir=" + backupDir,
                        "--name=customport" ) );
        assertEquals( getDbRepresentation(), getBackupDbRepresentation( "customport" ) );
    }

    private void startDb( String backupPort )
    {
        db.setConfig( GraphDatabaseSettings.record_format, recordFormat );
        db.setConfig( OnlineBackupSettings.online_backup_enabled, Settings.TRUE );
        if ( backupPort != null )
        {
            db.setConfig( OnlineBackupSettings.online_backup_server, ip + ":" + backupPort );
        }
        db.ensureStarted();
        createSomeData( db );
    }

    private static int runBackupToolFromOtherJvmToGetExitCode( String... args )
            throws Exception
    {
        return runBackupToolFromOtherJvmToGetExitCode( testDirectory.absolutePath(), args );
    }

    public static int runBackupToolFromOtherJvmToGetExitCode( File neo4jHome, String... args )
            throws Exception
    {
        List<String> allArgs = new ArrayList<>( Arrays.asList(
                ProcessUtil.getJavaExecutable().toString(), "-cp", ProcessUtil.getClassPath(),
                AdminTool.class.getName() ) );
        allArgs.add( "backup" );
        allArgs.addAll( Arrays.asList( args ) );

        Process process = Runtime.getRuntime().exec( allArgs.toArray( new String[allArgs.size()] ),
                new String[] {"NEO4J_HOME=" + neo4jHome.getAbsolutePath()} );
        return new ProcessStreamHandler( process, true ).waitForResult();
    }

    private DbRepresentation getDbRepresentation()
    {
        return DbRepresentation.of( db );
    }

    private DbRepresentation getBackupDbRepresentation( String name )
    {
        return DbRepresentation.of( new File( backupDir, name ) );
    }
}
