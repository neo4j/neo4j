/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 *
 * This file is part of Neo4j.
 *
 * Neo4j is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.commandline.dbms;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import picocli.CommandLine;
import picocli.CommandLine.MutuallyExclusiveArgsException;

import java.io.ByteArrayOutputStream;
import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.io.PrintStream;
import java.nio.file.Path;
import java.nio.file.Paths;

import org.neo4j.cli.CommandFailedException;
import org.neo4j.cli.ExecutionContext;
import org.neo4j.configuration.Config;
import org.neo4j.configuration.GraphDatabaseSettings;
import org.neo4j.configuration.LayoutConfig;
import org.neo4j.consistency.CheckConsistencyCommand;
import org.neo4j.consistency.ConsistencyCheckService;
import org.neo4j.consistency.checking.full.ConsistencyCheckIncompleteException;
import org.neo4j.consistency.checking.full.ConsistencyFlags;
import org.neo4j.dbms.api.DatabaseManagementService;
import org.neo4j.internal.helpers.progress.ProgressMonitorFactory;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.layout.DatabaseLayout;
import org.neo4j.kernel.internal.locker.FileLockException;
import org.neo4j.logging.LogProvider;
import org.neo4j.test.TestDatabaseManagementServiceBuilder;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.testdirectory.TestDirectoryExtension;
import org.neo4j.test.rule.TestDirectory;

import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.neo4j.configuration.GraphDatabaseSettings.databases_root_path;
import static org.neo4j.configuration.GraphDatabaseSettings.default_database;
import static org.neo4j.configuration.GraphDatabaseSettings.transaction_logs_root_path;
import static org.neo4j.io.layout.StoreLayoutConfig.NOT_CONFIGURED;

@TestDirectoryExtension
class CheckConsistencyCommandIT
{
    @Inject
    private TestDirectory testDir;
    private Path homeDir;
    private DatabaseLayout databaseLayout;

    @BeforeEach
    void setUp()
    {
        homeDir = testDir.directory( "home" ).toPath();
        databaseLayout = prepareDatabase( homeDir, Config.defaults() );
    }

    @Test
    void printUsageHelp()
    {
        final var baos = new ByteArrayOutputStream();
        final var command = new CheckConsistencyCommand( new ExecutionContext( Path.of( "." ), Path.of( "." ) ) );
        try ( var out = new PrintStream( baos ) )
        {
            CommandLine.usage( command, new PrintStream( out ) );
        }
        assertThat( baos.toString().trim(), equalTo( String.format(
                "Check the consistency of a database.%n" +
                "%n" +
                "USAGE%n" +
                "%n" +
                "check-consistency ([--database=<database>] | [--backup=<path>]) [--verbose]%n" +
                "                  [--additional-config=<path>] [--check-graph=<true/false>]%n" +
                "                  [--check-indexes=<true/false>]%n" +
                "                  [--check-label-scan-store=<true/false>]%n" +
                "                  [--check-property-owners=<true/false>] [--report-dir=<path>]%n" +
                "%n" +
                "DESCRIPTION%n" +
                "%n" +
                "This command allows for checking the consistency of a database or a backup%n" +
                "thereof. It cannot be used with a database which is currently in use.%n" +
                "%n" +
                "All checks except 'check-graph' can be quite expensive so it may be useful to%n" +
                "turn them off for very large databases. Increasing the heap size can also be a%n" +
                "good idea. See 'neo4j-admin help' for details.%n" +
                "%n" +
                "OPTIONS%n" +
                "%n" +
                "      --verbose             Enable verbose output.%n" +
                "      --database=<database> Name of the database.%n" +
                "      --backup=<path>       Path to backup to check consistency of. Cannot be%n" +
                "                              used together with --database.%n" +
                "      --additional-config=<path>%n" +
                "                            Configuration file to supply additional%n" +
                "                              configuration in.%n" +
                "      --report-dir=<path>   Directory where consistency report will be written.%n" +
                "                              Default: .%n" +
                "      --check-graph=<true/false>%n" +
                "                            Perform consistency checks between nodes,%n" +
                "                              relationships, properties, types and tokens.%n" +
                "                              Default: true%n" +
                "      --check-indexes=<true/false>%n" +
                "                            Perform consistency checks on indexes.%n" +
                "                              Default: true%n" +
                "      --check-label-scan-store=<true/false>%n" +
                "                            Perform consistency checks on the label scan store.%n" +
                "                              Default: true%n" +
                "      --check-property-owners=<true/false>%n" +
                "                            Perform additional consistency checks on property%n" +
                "                              ownership. This check is very expensive in time%n" +
                "                              and memory.%n" +
                "                              Default: false"
        ) ) );
    }

    @Test
    void runsConsistencyChecker() throws Exception
    {
        ConsistencyCheckService consistencyCheckService = mock( ConsistencyCheckService.class );

        File databasesFolder = getDatabasesFolder( homeDir );
        CheckConsistencyCommand checkConsistencyCommand =
                new CheckConsistencyCommand( new ExecutionContext( homeDir, testDir.directory( "conf" ).toPath() ), consistencyCheckService );
        Config config = Config.defaults( GraphDatabaseSettings.neo4j_home, homeDir );

        DatabaseLayout databaseLayout = DatabaseLayout.of( databasesFolder, LayoutConfig.of( config ), "mydb" );

        when( consistencyCheckService
                .runFullConsistencyCheck( eq( databaseLayout ), any( Config.class ), any( ProgressMonitorFactory.class ),
                        any( LogProvider.class ), any( FileSystemAbstraction.class ), eq( false ), any(),
                        any( ConsistencyFlags.class ) ) )
                .thenReturn( ConsistencyCheckService.Result.success( null ) );

        CommandLine.populateCommand( checkConsistencyCommand, "--database=mydb" );
        checkConsistencyCommand.execute();

        verify( consistencyCheckService )
                .runFullConsistencyCheck( eq( databaseLayout ), any( Config.class ), any( ProgressMonitorFactory.class ),
                        any( LogProvider.class ), any( FileSystemAbstraction.class ), eq( false ), any(),
                        any( ConsistencyFlags.class ) );
    }

    @Test
    void consistencyCheckerRespectDatabaseLock() throws CannotWriteException, IOException
    {
        ConsistencyCheckService consistencyCheckService = mock( ConsistencyCheckService.class );

        File databasesFolder = getDatabasesFolder( homeDir );
        CheckConsistencyCommand checkConsistencyCommand =
                new CheckConsistencyCommand( new ExecutionContext( homeDir, testDir.directory( "conf" ).toPath() ), consistencyCheckService );
        Config config = Config.defaults( GraphDatabaseSettings.neo4j_home, homeDir );
        DatabaseLayout databaseLayout = DatabaseLayout.of( databasesFolder, LayoutConfig.of( config ), "mydb" );
        testDir.getFileSystem().mkdirs( databaseLayout.databaseDirectory() );

        try ( Closeable lock = DatabaseLockChecker.check( databaseLayout ) )
        {
            CommandLine.populateCommand( checkConsistencyCommand, "--database=mydb", "--verbose" );
            CommandFailedException exception = assertThrows( CommandFailedException.class, checkConsistencyCommand::execute );
            assertThat( exception.getCause(), instanceOf( FileLockException.class ) );
            assertThat( exception.getMessage(), equalTo( "The database is in use. Stop database 'mydb' and try again." ) );
        }
    }

    @Test
    void enablesVerbosity() throws Exception
    {
        ConsistencyCheckService consistencyCheckService = mock( ConsistencyCheckService.class );

        File databasesFolder = getDatabasesFolder( homeDir );
        CheckConsistencyCommand checkConsistencyCommand =
                new CheckConsistencyCommand( new ExecutionContext( homeDir, testDir.directory( "conf" ).toPath() ), consistencyCheckService );
        Config config = Config.defaults( GraphDatabaseSettings.neo4j_home, homeDir );

        DatabaseLayout databaseLayout = DatabaseLayout.of( databasesFolder, LayoutConfig.of( config ), "mydb" );

        when( consistencyCheckService
                .runFullConsistencyCheck( eq( databaseLayout ), any( Config.class ), any( ProgressMonitorFactory.class ),
                        any( LogProvider.class ), any( FileSystemAbstraction.class ), eq( true ), any(),
                        any( ConsistencyFlags.class ) ) )
                .thenReturn( ConsistencyCheckService.Result.success( null ) );

        CommandLine.populateCommand( checkConsistencyCommand, "--database=mydb", "--verbose" );
        checkConsistencyCommand.execute();

        verify( consistencyCheckService )
                .runFullConsistencyCheck( eq( databaseLayout ), any( Config.class ), any( ProgressMonitorFactory.class ),
                        any( LogProvider.class ), any( FileSystemAbstraction.class ), eq( true ), any(),
                        any( ConsistencyFlags.class ) );
    }

    @Test
    void failsWhenInconsistenciesAreFound() throws Exception
    {
        ConsistencyCheckService consistencyCheckService = mock( ConsistencyCheckService.class );

        CheckConsistencyCommand checkConsistencyCommand =
                new CheckConsistencyCommand( new ExecutionContext( homeDir, testDir.directory( "conf" ).toPath() ), consistencyCheckService );
        Config config = Config.defaults( GraphDatabaseSettings.neo4j_home, homeDir );
        when( consistencyCheckService
                .runFullConsistencyCheck( any( DatabaseLayout.class ), any( Config.class ), any( ProgressMonitorFactory.class ),
                        any( LogProvider.class ), any( FileSystemAbstraction.class ), eq( true ), any(),
                        any( ConsistencyFlags.class ) ) )
                .thenReturn( ConsistencyCheckService.Result.failure( new File( "/the/report/path" ) ) );

        CommandFailedException commandFailed =
                assertThrows( CommandFailedException.class, () ->
                {
                    CommandLine.populateCommand( checkConsistencyCommand, "--database=mydb", "--verbose" );
                    checkConsistencyCommand.execute();
                } );
        assertThat( commandFailed.getMessage(), containsString( new File( "/the/report/path" ).toString() ) );
    }

    @Test
    void shouldWriteReportFileToCurrentDirectoryByDefault()
            throws IOException, ConsistencyCheckIncompleteException, CommandFailedException

    {
        ConsistencyCheckService consistencyCheckService = mock( ConsistencyCheckService.class );

        CheckConsistencyCommand checkConsistencyCommand =
                new CheckConsistencyCommand( new ExecutionContext( homeDir, testDir.directory( "conf" ).toPath() ), consistencyCheckService );

        when( consistencyCheckService.runFullConsistencyCheck( any(), any(), any(), any(),
                any(), anyBoolean(), any(), any( ConsistencyFlags.class ) ) )
                .thenReturn( ConsistencyCheckService.Result.success( null ) );

        CommandLine.populateCommand( checkConsistencyCommand, "--database=mydb" );
        checkConsistencyCommand.execute();

        verify( consistencyCheckService )
                .runFullConsistencyCheck( any(), any(), any(), any(), any(), anyBoolean(),
                        eq( new File( "." ).getCanonicalFile() ), any( ConsistencyFlags.class ) );
    }

    @Test
    void shouldWriteReportFileToSpecifiedDirectory()
            throws IOException, ConsistencyCheckIncompleteException, CommandFailedException

    {
        ConsistencyCheckService consistencyCheckService = mock( ConsistencyCheckService.class );

        CheckConsistencyCommand checkConsistencyCommand =
                new CheckConsistencyCommand( new ExecutionContext( homeDir, testDir.directory( "conf" ).toPath() ), consistencyCheckService );

        when( consistencyCheckService.runFullConsistencyCheck( any(), any(), any(), any(),
                any(), anyBoolean(), any(), any( ConsistencyFlags.class ) ) )
                .thenReturn( ConsistencyCheckService.Result.success( null ) );

        CommandLine.populateCommand( checkConsistencyCommand, "--database=mydb", "--report-dir=some-dir-or-other" );
        checkConsistencyCommand.execute();

        verify( consistencyCheckService )
                .runFullConsistencyCheck( any(), any(), any(), any(), any(),
                        anyBoolean(), eq( new File( "some-dir-or-other" ).getCanonicalFile() ),
                        any( ConsistencyFlags.class ) );
    }

    @Test
    void shouldCanonicalizeReportDirectory()
            throws IOException, ConsistencyCheckIncompleteException, CommandFailedException
    {
        ConsistencyCheckService consistencyCheckService = mock( ConsistencyCheckService.class );

        CheckConsistencyCommand checkConsistencyCommand =
                new CheckConsistencyCommand( new ExecutionContext( homeDir, testDir.directory( "conf" ).toPath() ), consistencyCheckService );

        when( consistencyCheckService.runFullConsistencyCheck( any(), any(), any(), any(),
                any(), anyBoolean(), any(), any( ConsistencyFlags.class ) ) )
                .thenReturn( ConsistencyCheckService.Result.success( null ) );

        CommandLine.populateCommand( checkConsistencyCommand, "--database=mydb", "--report-dir=" + Paths.get( "..", "bar" ) );
        checkConsistencyCommand.execute();

        verify( consistencyCheckService )
                .runFullConsistencyCheck( any(), any(), any(), any(), any(),
                        anyBoolean(), eq( new File( "../bar" ).getCanonicalFile() ),
                        any( ConsistencyFlags.class ) );
    }

    @Test
    void passesOnCheckParameters() throws Exception
    {
        ConsistencyCheckService consistencyCheckService = mock( ConsistencyCheckService.class );

        CheckConsistencyCommand checkConsistencyCommand =
                new CheckConsistencyCommand( new ExecutionContext( homeDir, testDir.directory( "conf" ).toPath() ), consistencyCheckService );

        when( consistencyCheckService.runFullConsistencyCheck( any(), any(), any(), any(),
                any(), anyBoolean(), any(), any( ConsistencyFlags.class ) ) )
                .thenReturn( ConsistencyCheckService.Result.success( null ) );

        CommandLine.populateCommand( checkConsistencyCommand, "--database=mydb", "--check-graph=false",
            "--check-indexes=false", "--check-label-scan-store=false", "--check-property-owners=true" );
        checkConsistencyCommand.execute();

        verify( consistencyCheckService )
                .runFullConsistencyCheck( any(), any(), any(), any(), any(), anyBoolean(),
                        any(), eq( new ConsistencyFlags( false, false, false, true ) ) );
    }

    @Test
    void databaseAndBackupAreMutuallyExclusive() throws Exception
    {
        ConsistencyCheckService consistencyCheckService = mock( ConsistencyCheckService.class );

        CheckConsistencyCommand checkConsistencyCommand =
                new CheckConsistencyCommand( new ExecutionContext( homeDir, testDir.directory( "conf" ).toPath() ), consistencyCheckService );

        when( consistencyCheckService.runFullConsistencyCheck( any(), any(), any(), any(),
                any(), anyBoolean(), any( ConsistencyFlags.class ) ) )
                .thenReturn( ConsistencyCheckService.Result.success( null ) );

        MutuallyExclusiveArgsException incorrectUsage =
                assertThrows( MutuallyExclusiveArgsException.class, () ->
                {
                    CommandLine.populateCommand( checkConsistencyCommand, "--database=foo", "--backup=bar" );
                    checkConsistencyCommand.execute();
                } );
        assertThat( incorrectUsage.getMessage(), containsString( "--database=<database>, --backup=<path> are mutually exclusive (specify only one)" ) );
    }

    @Test
    void backupNeedsToBePath()
    {
        ConsistencyCheckService consistencyCheckService = mock( ConsistencyCheckService.class );

        CheckConsistencyCommand checkConsistencyCommand =
                new CheckConsistencyCommand( new ExecutionContext( homeDir, testDir.directory( "conf" ).toPath() ), consistencyCheckService );

        File backupPath = new File( homeDir.toFile(), "dir/does/not/exist" );

        CommandFailedException commandFailed = assertThrows( CommandFailedException.class, () ->
        {
            CommandLine.populateCommand( checkConsistencyCommand, "--backup=" + backupPath );
            checkConsistencyCommand.execute();
        } );
        assertThat( commandFailed.getMessage(), containsString( "Report directory path doesn't exist or not a directory" ) );
    }

    @Test
    void canRunOnBackup() throws Exception
    {
        ConsistencyCheckService consistencyCheckService = mock( ConsistencyCheckService.class );

        File databasesFolder = getDatabasesFolder( homeDir );
        DatabaseLayout backupLayout = DatabaseLayout.of( databasesFolder, NOT_CONFIGURED, "backup" );
        prepareBackupDatabase( backupLayout );
        CheckConsistencyCommand checkConsistencyCommand =
                new CheckConsistencyCommand( new ExecutionContext( homeDir, testDir.directory( "conf" ).toPath() ), consistencyCheckService );

        when( consistencyCheckService
                .runFullConsistencyCheck( eq( backupLayout ), any( Config.class ),
                        any( ProgressMonitorFactory.class ),
                        any( LogProvider.class ), any( FileSystemAbstraction.class ), eq( false ), any(),
                        any( ConsistencyFlags.class ) ) )
                .thenReturn( ConsistencyCheckService.Result.success( null ) );

        CommandLine.populateCommand( checkConsistencyCommand, "--backup=" + backupLayout.databaseDirectory() );
        checkConsistencyCommand.execute();

        verify( consistencyCheckService )
                .runFullConsistencyCheck( eq( backupLayout ), any( Config.class ),
                        any( ProgressMonitorFactory.class ),
                        any( LogProvider.class ), any( FileSystemAbstraction.class ), eq( false ), any(),
                        any( ConsistencyFlags.class ) );
    }

    private void prepareBackupDatabase( DatabaseLayout backupLayout ) throws IOException
    {
        testDir.getFileSystem().deleteRecursively( homeDir.toFile() );
        prepareDatabase( backupLayout, Config.defaults( transaction_logs_root_path, backupLayout.getStoreLayout().storeDirectory().toPath() ) );
    }

    private static File getDatabasesFolder( Path homeDir )
    {
        return Config.defaults( GraphDatabaseSettings.neo4j_home, homeDir.toAbsolutePath() ).get( databases_root_path ).toFile();
    }

    private DatabaseLayout prepareDatabase( Path homeDir, Config config )
    {
        File databasesFolder = getDatabasesFolder( homeDir );
        DatabaseLayout databaseLayout = DatabaseLayout.of( databasesFolder, LayoutConfig.of( config ), "mydb" );
        prepareDatabase( databaseLayout, config );
        return databaseLayout;
    }

    private void prepareDatabase( DatabaseLayout databaseLayout, Config config )
    {
        File databaseDirectory = databaseLayout.databaseDirectory();
        String databaseName = databaseDirectory.getName();
        DatabaseManagementService managementService =
                new TestDatabaseManagementServiceBuilder( databaseDirectory.getParentFile().getParentFile().getParentFile() )
                        .setConfig( config )
                        .setConfig( default_database, databaseName )
                        .setConfig( databases_root_path, databases_root_path.defaultValue() )
                        .build();
        managementService.shutdown();
    }
}
