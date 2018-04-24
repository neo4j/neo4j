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

import org.apache.commons.lang3.SystemUtils;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.RuleChain;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameter;
import org.junit.runners.Parameterized.Parameters;

import java.io.File;
import java.io.IOException;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.function.Supplier;
import java.util.stream.LongStream;

import org.neo4j.com.ports.allocation.PortAuthority;
import org.neo4j.commandline.admin.AdminTool;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.factory.GraphDatabaseSettings;
import org.neo4j.io.ByteUnit;
import org.neo4j.io.proc.ProcessUtil;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.configuration.Settings;
import org.neo4j.kernel.impl.store.format.highlimit.HighLimit;
import org.neo4j.kernel.impl.store.format.standard.Standard;
import org.neo4j.test.DbRepresentation;
import org.neo4j.test.ProcessStreamHandler;
import org.neo4j.test.rule.EmbeddedDatabaseRule;
import org.neo4j.test.rule.TestDirectory;

import static org.junit.Assert.assertEquals;
import static org.junit.Assume.assumeFalse;

@RunWith( Parameterized.class )
public class OnlineBackupCommandIT
{
    @Rule
    public final TestDirectory testDirectory = TestDirectory.testDirectory();

    private final EmbeddedDatabaseRule db = new EmbeddedDatabaseRule().startLazily();

    @Rule
    public final RuleChain ruleChain = RuleChain.outerRule( db );

    private File backupDir;

    @Parameter
    public String recordFormat;

    @Parameters( name = "{0}" )
    public static List<String> recordFormats()
    {
        return Arrays.asList( Standard.LATEST_NAME, HighLimit.NAME );
    }

    @Before
    public void setUp()
    {
        backupDir = testDirectory.directory( "backups" );
    }

    public static Supplier<DbRepresentation> createSomeData( GraphDatabaseService db, long iterations )
    {
        try ( Transaction tx = db.beginTx() )
        {
            for ( int i = 0; i < iterations; i++ )
            {
                Node node = db.createNode();
                node.setProperty( "name", "Neo" );
                db.createNode().createRelationshipTo( node, RelationshipType.withName( "KNOWS" ) );
            }
            tx.success();
        }
        return () -> DbRepresentation.of( db );
    }

    public static Supplier<DbRepresentation> createSomeData( GraphDatabaseService db )
    {
        return createSomeData( db, 1 );
    }

    @Test
    public void makeSureBackupCanBePerformedWithCustomPort() throws Exception
    {
        assumeFalse( SystemUtils.IS_OS_WINDOWS );

        int backupPort = PortAuthority.allocatePort();
        startDb( backupPort );
        assertEquals( "should not be able to do backup when noone is listening",
                1,
                runBackupToolFromOtherJvmToGetExitCode( "--from", "127.0.0.1:" + PortAuthority.allocatePort(),
                        "--cc-report-dir=" + backupDir,
                        "--backup-dir=" + backupDir,
                        "--name=customport" ) );
        assertEquals(
                0,
                runBackupToolFromOtherJvmToGetExitCode( "--from", "127.0.0.1:" + backupPort,
                        "--cc-report-dir=" + backupDir,
                        "--backup-dir=" + backupDir,
                        "--name=customport" ) );
        assertEquals( getDbRepresentation(), getBackupDbRepresentation( "customport" ) );
        createSomeData( db );
        assertEquals(
                0,
                runBackupToolFromOtherJvmToGetExitCode( "--from", "127.0.0.1:" + backupPort,
                        "--cc-report-dir=" + backupDir,
                        "--backup-dir=" + backupDir,
                        "--name=customport" ) );
        assertEquals( getDbRepresentation(), getBackupDbRepresentation( "customport" ) );
    }

    @Test
    public void onlyTheLatestTransactionIsKeptAfterIncrementalBackup() throws Exception
    {
        // given database exists with data
        int port = PortAuthority.allocatePort();
        startDb( port );
        createSomeData( db );

        // and we have a full backup
        String backupName = "backupName" + recordFormat;
        File backupLocation = new File( backupDir, backupName );
        String address = "localhost:" + port;
        assertEquals(
                0,
                runBackupToolFromOtherJvmToGetExitCode(
                        "--from", address,
                        "--cc-report-dir=" + backupDir,
                        "--backup-dir=" + backupDir,
                        "--name=" + backupName ) );

        // and the database contains a few more transactions
        long aMegabyte = ByteUnit.mebiBytes( 1 );
        long numberOfBytesInTx = 100;
        LongStream.range( 0, 205 ).forEach( number -> createSomeData( db, aMegabyte / numberOfBytesInTx ) );
        assertEquals( "Server should have 3 tx log files", 3, transactionFiles( db.getStoreDirFile() ).size() ); // with multiple tx files

        // when we perform an incremental backup
        assertEquals(
                0,
                runBackupToolFromOtherJvmToGetExitCode(
                        "--from", address,
                        "--cc-report-dir=" + backupDir,
                        "--backup-dir=" + backupDir,
                        "--name=" + backupName ) );

        // then there is only 1 transaction file containing 1 transaction
        Collection<File> txLogFiles = transactionFiles( backupLocation );
        File expectedTxLogFile1 = new File( backupLocation, "neostore.transaction.db.1" );
        File expectedTxLogFile2 = new File( backupLocation, "neostore.transaction.db.2" );
        assertEquals( new HashSet<>( Arrays.asList( expectedTxLogFile1, expectedTxLogFile2 ) ), new HashSet<>( txLogFiles ) );
    }

    private Collection<File> transactionFiles( File dbLocation ) throws IOException
    {
        Collection<File> txFiles = new ArrayList<>();
        DirectoryStream<Path> dirStream = Files.newDirectoryStream( dbLocation.toPath(), "neostore.transaction.db.*" );
        dirStream.forEach( path -> txFiles.add( path.toFile() ) );
        dirStream.close();
        return txFiles;
    }

    private void startDb( int backupPort )
    {
        db.setConfig( GraphDatabaseSettings.record_format, recordFormat );
        db.setConfig( OnlineBackupSettings.online_backup_enabled, Settings.TRUE );
        db.setConfig( OnlineBackupSettings.online_backup_server, "127.0.0.1" + ":" + backupPort );
        db.ensureStarted();
        createSomeData( db );
    }

    private int runBackupToolFromOtherJvmToGetExitCode( String... args )
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

        Process process = Runtime.getRuntime().exec( allArgs.toArray( new String[0] ),
                new String[] {"NEO4J_HOME=" + neo4jHome.getAbsolutePath()} );
        return new ProcessStreamHandler( process, false ).waitForResult();
    }

    private DbRepresentation getDbRepresentation()
    {
        return DbRepresentation.of( db );
    }

    private DbRepresentation getBackupDbRepresentation( String name )
    {
        Config config = Config.defaults( OnlineBackupSettings.online_backup_enabled, Settings.FALSE );

        return DbRepresentation.of( new File( backupDir, name ), config );
    }
}
