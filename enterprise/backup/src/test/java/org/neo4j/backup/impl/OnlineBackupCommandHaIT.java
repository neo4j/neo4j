/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 *
 * This file is part of Neo4j Enterprise Edition. The included source
 * code can be redistributed and/or modified under the terms of the
 * GNU AFFERO GENERAL PUBLIC LICENSE Version 3
 * (http://www.fsf.org/licensing/licenses/agpl-3.0.html) with the
 * Commons Clause, as found in the associated LICENSE.txt file.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * Neo4j object code can be licensed independently from the source
 * under separate terms from the AGPL. Inquiries can be directed to:
 * licensing@neo4j.com
 *
 * More information is also available at:
 * https://neo4j.com/licensing/
 */
package org.neo4j.backup.impl;

import org.apache.commons.lang3.SystemUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.RuleChain;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameter;
import org.junit.runners.Parameterized.Parameters;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.PrintStream;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.LongStream;

import org.neo4j.consistency.ConsistencyCheckService;
import org.neo4j.consistency.checking.full.ConsistencyFlags;
import org.neo4j.graphdb.DatabaseShutdownException;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.factory.GraphDatabaseBuilder;
import org.neo4j.graphdb.factory.GraphDatabaseFactory;
import org.neo4j.graphdb.factory.GraphDatabaseSettings;
import org.neo4j.helpers.progress.ProgressMonitorFactory;
import org.neo4j.io.ByteUnit;
import org.neo4j.io.fs.DefaultFileSystemAbstraction;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.configuration.Settings;
import org.neo4j.kernel.impl.enterprise.configuration.OnlineBackupSettings;
import org.neo4j.kernel.impl.recovery.RecoveryRequiredChecker;
import org.neo4j.kernel.impl.store.format.highlimit.HighLimit;
import org.neo4j.kernel.impl.store.format.standard.Standard;
import org.neo4j.kernel.impl.transaction.log.files.LogFiles;
import org.neo4j.kernel.impl.transaction.log.files.LogFilesBuilder;
import org.neo4j.kernel.monitoring.Monitors;
import org.neo4j.logging.NullLogProvider;
import org.neo4j.ports.allocation.PortAuthority;
import org.neo4j.test.DbRepresentation;
import org.neo4j.test.rule.EmbeddedDatabaseRule;
import org.neo4j.test.rule.PageCacheRule;
import org.neo4j.test.rule.SuppressOutput;
import org.neo4j.test.rule.TestDirectory;
import org.neo4j.test.rule.fs.DefaultFileSystemRule;

import static java.lang.String.format;
import static java.util.stream.Collectors.joining;
import static org.hamcrest.Matchers.greaterThan;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.junit.Assume.assumeFalse;
import static org.neo4j.backup.impl.OnlineBackupCommandCcIT.arg;
import static org.neo4j.backup.impl.OnlineBackupCommandCcIT.wrapWithNormalOutput;
import static org.neo4j.backup.impl.OnlineBackupContextBuilder.ARG_NAME_FALLBACK_FULL;
import static org.neo4j.util.TestHelpers.runBackupToolFromOtherJvmToGetExitCode;

@RunWith( Parameterized.class )
public class OnlineBackupCommandHaIT
{
    private final DefaultFileSystemRule fileSystemRule = new DefaultFileSystemRule();
    private final TestDirectory testDirectory = TestDirectory.testDirectory( fileSystemRule );
    private final EmbeddedDatabaseRule db = new EmbeddedDatabaseRule().startLazily();
    private final PageCacheRule pageCacheRule = new PageCacheRule();

    @Rule
    public final RuleChain ruleChain =
            RuleChain.outerRule( SuppressOutput.suppressAll() ).around( fileSystemRule ).around( testDirectory ).around( pageCacheRule ).around( db );

    private File backupDir;

    @Parameter
    public String recordFormat;

    @Parameters( name = "{0}" )
    public static List<String> recordFormats()
    {
        return Arrays.asList( Standard.LATEST_NAME, HighLimit.NAME );
    }

    private List<Runnable> oneOffShutdownTasks;
    private static final Label label = Label.label( "any_label" );
    private static final String PROP_NAME = "name";
    private static final String PROP_RANDOM = "random";

    private static void createSomeData( GraphDatabaseService db )
    {
        try ( Transaction tx = db.beginTx() )
        {
            Node node = db.createNode();
            node.setProperty( "name", "Neo" );
            db.createNode().createRelationshipTo( node, RelationshipType.withName( "KNOWS" ) );
            tx.success();
        }
    }

    private static void createSpecificNodePair( GraphDatabaseService db, String name )
    {
        try ( Transaction tx = db.beginTx() )
        {
            Node left = db.createNode();
            left.setProperty( "name", name + "Left" );
            Node right = db.createNode();
            right.setProperty( "name", name + "Right" );
            right.createRelationshipTo( left, RelationshipType.withName( "KNOWS" ) );
            tx.success();
        }
    }

    @Before
    public void resetTasks()
    {
        backupDir = testDirectory.directory( "backups" );
        oneOffShutdownTasks = new ArrayList<>();
    }

    @After
    public void shutdownTasks()
    {
        oneOffShutdownTasks.forEach( Runnable::run );
    }

    @Test
    public void makeSureBackupCanBePerformedWithCustomPort() throws Exception
    {
        assumeFalse( SystemUtils.IS_OS_WINDOWS );
        String backupName = "customport" + recordFormat; // due to ClassRule not cleaning between tests

        int backupPort = PortAuthority.allocatePort();
        startDb( backupPort );
        assertEquals( "should not be able to do backup when noone is listening",
                1,
                runBackupTool( testDirectory.absolutePath(), "--from", "127.0.0.1:" + PortAuthority.allocatePort(),
                        "--cc-report-dir=" + backupDir,
                        "--backup-dir=" + backupDir,
                        "--name=" + backupName ) );
        assertEquals(
                0,
                runBackupTool( testDirectory.absolutePath(), "--from", "127.0.0.1:" + backupPort,
                        "--cc-report-dir=" + backupDir,
                        "--backup-dir=" + backupDir,
                        "--name=" + backupName ) );
        assertEquals( DbRepresentation.of( db ), getBackupDbRepresentation( backupName ) );
        createSomeData( db );
        assertEquals(
                0,
                runBackupTool( testDirectory.absolutePath(), "--from", "127.0.0.1:" + backupPort,
                        "--cc-report-dir=" + backupDir,
                        "--backup-dir=" + backupDir,
                        "--name=" + backupName ) );
        assertEquals( DbRepresentation.of( db ), getBackupDbRepresentation( backupName ) );
    }

    @Test
    public void fullBackupIsRecoveredAndConsistent() throws Exception
    {
        // given database exists
        int backupPort = PortAuthority.allocatePort();
        startDb( backupPort );
        String ip = ":" + backupPort;

        String name = UUID.randomUUID().toString();
        File backupLocation = new File( backupDir, name );

        // when
        assertEquals( 0,
                runBackupTool( testDirectory.absolutePath(), "--from", ip, "--cc-report-dir=" + backupDir, "--backup-dir=" + backupDir, "--name=" + name ) );

        // then
        assertFalse( "Store should not require recovery",
                new RecoveryRequiredChecker( fileSystemRule, pageCacheRule.getPageCache( fileSystemRule ), Config.defaults(),
                        new Monitors() ).isRecoveryRequiredAt( backupLocation ) );
        ConsistencyFlags consistencyFlags = new ConsistencyFlags( true, true, true, true );
        assertTrue( "Consistency check failed", new ConsistencyCheckService()
                .runFullConsistencyCheck( backupLocation, Config.defaults(), ProgressMonitorFactory.NONE, NullLogProvider.getInstance(), false,
                        consistencyFlags )
                .isSuccessful() );
    }

    @Test
    public void incrementalBackupIsRecoveredAndConsistent() throws Exception
    {
        // given database exists
        int backupPort = PortAuthority.allocatePort();
        startDb( backupPort );
        String ip = ":" + backupPort;

        String name = UUID.randomUUID().toString();
        File backupLocation = new File( backupDir, name );

        // when
        assertEquals( 0,
                runBackupTool( testDirectory.absolutePath(), "--from", ip, "--cc-report-dir=" + backupDir, "--backup-dir=" + backupDir, "--name=" + name ) );

        // and
        createSomeData( db );
        assertEquals( 0,
                runBackupTool( testDirectory.absolutePath(), "--from", ip, "--cc-report-dir=" + backupDir, "--backup-dir=" + backupDir, "--name=" + name,
                        arg( ARG_NAME_FALLBACK_FULL, false ) ) );

        // then
        assertFalse( "Store should not require recovery",
                new RecoveryRequiredChecker( fileSystemRule, pageCacheRule.getPageCache( fileSystemRule ), Config.defaults(),
                        new Monitors() ).isRecoveryRequiredAt( backupLocation ) );
        ConsistencyFlags consistencyFlags = new ConsistencyFlags( true, true, true, true );
        assertTrue( "Consistency check failed", new ConsistencyCheckService()
                .runFullConsistencyCheck( backupLocation, Config.defaults(), ProgressMonitorFactory.NONE, NullLogProvider.getInstance(), false,
                        consistencyFlags )
                .isSuccessful() );
    }

    @Test
    public void dataIsInAUsableStateAfterBackup() throws Exception
    {
        // given database exists
        int backupPort = PortAuthority.allocatePort();
        startDb( backupPort );

        // and the database has indexes
        createIndexes( db );

        // and the database is being populated
        AtomicBoolean continueFlagReference = new AtomicBoolean( true );
        new Thread( () -> repeatedlyPopulateDatabase( db, continueFlagReference ) ).start(); // populate db with number properties etc.
        oneOffShutdownTasks.add( () -> continueFlagReference.set( false ) ); // kill thread

        // then backup is successful
        String ip = ":" + backupPort;
        String backupName = "usableState" + recordFormat;
        assertEquals( 0, runBackupTool( testDirectory.absolutePath(),
                "--from", ip, "--cc-report-dir=" + backupDir, "--backup-dir=" + backupDir, "--name=" + backupName ) );
        db.shutdown();
    }

    @Test
    public void backupDatabaseTransactionLogsStoredWithDatabase() throws Exception
    {
        int backupPort = PortAuthority.allocatePort();
        startDb( backupPort );
        String ip = ":" + backupPort;
        String name = "backupWithTxLogs" + recordFormat;
        assertEquals( 0, runBackupTool( testDirectory.absolutePath(),
                "--from", ip, "--cc-report-dir=" + backupDir, "--backup-dir=" + backupDir,
                "--name=" + name ) );
        db.shutdown();

        try ( DefaultFileSystemAbstraction fileSystem = new DefaultFileSystemAbstraction() )
        {
            LogFiles logFiles = LogFilesBuilder.logFilesBasedOnlyBuilder( new File( backupDir, name ), fileSystem ).build();
            assertTrue( logFiles.versionExists( 0 ) );
            assertThat( logFiles.getLogFileForVersion( 0 ).length(), greaterThan( 50L ) );
        }
    }

    @Test
    public void backupFailsWithCatchupProtoOverride() throws Exception
    {
        String backupName = "portOverride" + recordFormat; // due to ClassRule not cleaning between tests

        int backupPort = PortAuthority.allocatePort();
        startDb( backupPort );

        assertEquals(
                1,
                runBackupTool( testDirectory.absolutePath(), "--from", "127.0.0.1:" + backupPort,
                        "--cc-report-dir=" + backupDir,
                        "--backup-dir=" + backupDir,
                        "--protocol=catchup",
                        "--name=" + backupName ) );
    }

    @Test
    public void backupDoesNotDisplayExceptionWhenSuccessful() throws Exception
    {
        // given
        String backupName = "noErrorTest_" + recordFormat;
        int backupPort = PortAuthority.allocatePort();
        startDb( backupPort );

        // and
        ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
        PrintStream outputStream = wrapWithNormalOutput( System.out, new PrintStream( byteArrayOutputStream ) );

        // and
        ByteArrayOutputStream byteArrayErrorStream = new ByteArrayOutputStream();
        PrintStream errorStream = wrapWithNormalOutput( System.err, new PrintStream( byteArrayErrorStream ) );

        // when
        assertEquals(
                0,
                runBackupTool( testDirectory.absolutePath(), outputStream, errorStream,
                        "--from", "127.0.0.1:" + backupPort,
                        "--cc-report-dir=" + backupDir,
                        "--backup-dir=" + backupDir,
                        "--name=" + backupName ) );

        // then
        assertFalse( byteArrayErrorStream.toString().toLowerCase().contains( "exception" ) );
        assertFalse( byteArrayOutputStream.toString().toLowerCase().contains( "exception" ) );
    }

    @Test
    public void reportsProgress() throws Exception
    {
        // given
        String backupName = "reportsProgress_" + recordFormat;
        int backupPort = PortAuthority.allocatePort();
        startDb( backupPort );

        // and
        ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
        PrintStream outputStream = wrapWithNormalOutput( System.out, new PrintStream( byteArrayOutputStream ) );

        // and
        ByteArrayOutputStream byteArrayErrorStream = new ByteArrayOutputStream();
        PrintStream errorStream = wrapWithNormalOutput( System.err, new PrintStream( byteArrayErrorStream ) );

        // when
        assertEquals(
                0,
                runBackupTool( backupDir, outputStream, errorStream,
                        "--from", "127.0.0.1:" + backupPort,
                        "--protocol=common",
                        "--cc-report-dir=" + backupDir,
                        "--backup-dir=" + backupDir,
                        "--name=" + backupName ) );

        // then
        String output = byteArrayOutputStream.toString();
        String legacyImplementationDetail = "temp-copy";
        String location = Paths.get( backupDir.toString(), backupName, legacyImplementationDetail ).toString();
        assertTrue( output.contains( "Start receiving store files" ) );
        assertTrue( output.contains( "Finish receiving store files" ) );
        String tested = Paths.get( location, "neostore.nodestore.db.labels" ).toString();
        assertTrue( tested, output.contains( format( "Start receiving store file %s", tested ) ) );
        assertTrue( tested, output.contains( format( "Finish receiving store file %s", tested ) ) );
        assertFalse( output.contains( "Start receiving transactions from " ) );
        assertFalse( output.contains( "Finish receiving transactions at " ) );
        assertTrue( output.contains( "Start recovering store" ) );
        assertTrue( output.contains( "Finish recovering store" ) );
        assertFalse( output.contains( "Start receiving index snapshots" ) );
        assertFalse( output.contains( "Start receiving index snapshot id 1" ) );
        assertFalse( output.contains( "Finished receiving index snapshot id 1" ) );
        assertFalse( output.contains( "Finished receiving index snapshots" ) );
    }

    @Test
    public void backupRenamesWork()
    {
        // given a prexisting backup from a different store
        String backupName = "preexistingBackup_" + recordFormat;
        int firstBackupPort = PortAuthority.allocatePort();
        startDb( firstBackupPort );
        createSpecificNodePair( db, "first" );

        assertEquals( 0, runSameJvm( backupDir, backupName,
                "--from", "127.0.0.1:" + firstBackupPort,
                "--cc-report-dir=" + backupDir,
                "--protocol=common",
                "--backup-dir=" + backupDir,
                "--name=" + backupName ) );
        DbRepresentation firstDatabaseRepresentation = DbRepresentation.of( db );

        // and a different database
        int secondBackupPort = PortAuthority.allocatePort();
        GraphDatabaseService db2 = createDb2( secondBackupPort );
        createSpecificNodePair( db2, "second" );
        DbRepresentation secondDatabaseRepresentation = DbRepresentation.of( db2 );

        // when backup is performed
        assertEquals( 0, runSameJvm(backupDir, backupName,
                "--from", "127.0.0.1:" + secondBackupPort,
                "--cc-report-dir=" + backupDir,
                "--backup-dir=" + backupDir,
                "--protocol=common",
                "--name=" + backupName ) );

        // then the new backup has the correct name
        assertEquals( secondDatabaseRepresentation, getBackupDbRepresentation( backupName ) );

        // and the old backup is in a renamed location
        assertEquals( firstDatabaseRepresentation, getBackupDbRepresentation( backupName + ".err.0" ) );

        // and the data isn't equal (sanity check)
        assertNotEquals( firstDatabaseRepresentation, secondDatabaseRepresentation );
        db2.shutdown();
    }

    @Test
    public void onlyTheLatestTransactionIsKeptAfterIncrementalBackup() throws Exception
    {
        // given database exists with data
        int port = PortAuthority.allocatePort();
        startDb( port );
        createSomeData( db );

        // and backup client is told to rotate conveniently
        Config config = Config
                .builder()
                .withSetting( GraphDatabaseSettings.logical_log_rotation_threshold, "1m" )
                .build();
        File configOverrideFile = testDirectory.file( "neo4j-backup.conf" );
        OnlineBackupCommandBuilder.writeConfigToFile( config, configOverrideFile );

        // and we have a full backup
        String backupName = "backupName" + recordFormat;
        String address = "localhost:" + port;
        assertEquals( 0, runBackupToolFromOtherJvmToGetExitCode( backupDir,
                "--from", address,
                "--cc-report-dir=" + backupDir,
                "--backup-dir=" + backupDir,
                "--protocol=common",
                "--additional-config=" + configOverrideFile,
                "--name=" + backupName ) );

        // and the database contains a few more transactions
        transactions1M( db );
        transactions1M( db ); // rotation, second tx log file

        // when we perform an incremental backup
        assertEquals( 0, runBackupToolFromOtherJvmToGetExitCode( backupDir,
                "--from", address,
                "--cc-report-dir=" + backupDir,
                "--backup-dir=" + backupDir,
                "--protocol=common",
                "--additional-config=" + configOverrideFile,
                "--name=" + backupName ) );

        // then there has been a rotation
        BackupTransactionLogFilesHelper backupTransactionLogFilesHelper = new BackupTransactionLogFilesHelper();
        LogFiles logFiles = backupTransactionLogFilesHelper.readLogFiles( backupDir.toPath().resolve( backupName ).toFile() );
        long highestTxIdInLogFiles = logFiles.getHighestLogVersion();
        assertEquals( 2, highestTxIdInLogFiles );

        // and the original log has not been removed since the transactions are applied at start
        long lowestTxIdInLogFiles = logFiles.getLowestLogVersion();
        assertEquals( 0, lowestTxIdInLogFiles );
    }

    private static void transactions1M( GraphDatabaseService db )
    {
        int numberOfTransactions = 500;
        long sizeOfTransaction = (ByteUnit.mebiBytes( 1 ) / numberOfTransactions) + 1;
        for ( int txId = 0; txId < numberOfTransactions; txId++ )
        {
            try ( Transaction tx = db.beginTx() )
            {
                Node node = db.createNode();
                String longString = LongStream.range( 0, sizeOfTransaction ).map( l -> l % 10 ).mapToObj( Long::toString ).collect( joining( "" ) );
                node.setProperty( "name", longString );
                db.createNode().createRelationshipTo( node, RelationshipType.withName( "KNOWS" ) );
                tx.success();
            }
        }
    }

    private void repeatedlyPopulateDatabase( GraphDatabaseService db, AtomicBoolean continueFlagReference )
    {
        while ( continueFlagReference.get() )
        {
            try
            {
                createSomeData( db );
            }
            catch ( DatabaseShutdownException ex )
            {
                break;
            }
        }
    }

    private void createIndexes( GraphDatabaseService db )
    {
        try ( Transaction tx = db.beginTx() )
        {
            db.schema().indexFor( label ).on( PROP_NAME ).on( PROP_RANDOM ).create();
            tx.success();
        }
    }

    private GraphDatabaseService createDb2( Integer backupPort )
    {
        File storeDir = testDirectory.directory("graph-db-2");
        GraphDatabaseFactory factory = new GraphDatabaseFactory();
        GraphDatabaseBuilder builder = factory.newEmbeddedDatabaseBuilder( storeDir );
        builder.setConfig( OnlineBackupSettings.online_backup_server, "0.0.0.0:" + backupPort );
        return builder.newGraphDatabase();
    }

    private void startDb( Integer backupPort )
    {
        startDb( db, backupPort );
    }

    private void startDb( EmbeddedDatabaseRule db, Integer backupPort )
    {
        db.setConfig( GraphDatabaseSettings.record_format, recordFormat );
        db.setConfig( OnlineBackupSettings.online_backup_enabled, Settings.TRUE );
        db.setConfig( OnlineBackupSettings.online_backup_server, "127.0.0.1" + ":" + backupPort );
        db.ensureStarted();
        createSomeData( db );
    }

    private static int runBackupTool( File neo4jHome, PrintStream outputStream, PrintStream errorStream, String... args ) throws Exception
    {
        return runBackupToolFromOtherJvmToGetExitCode( neo4jHome, outputStream, errorStream, false, args );
    }

    private static int runBackupTool( File neo4jHome, String... args )
            throws Exception
    {
        return runBackupToolFromOtherJvmToGetExitCode( neo4jHome, args );
    }

    private static int runSameJvm( File home, String name, String... args )
    {
        try
        {
            new OnlineBackupCommandBuilder().withRawArgs( args ).backup( home, name );
            return 0;
        }
        catch ( Exception e )
        {
            e.printStackTrace();
            return 1;
        }
    }

    private DbRepresentation getBackupDbRepresentation( String name )
    {
        Config config = Config.defaults( OnlineBackupSettings.online_backup_enabled, Settings.FALSE );

        return DbRepresentation.of( new File( backupDir, name ), config );
    }
}
