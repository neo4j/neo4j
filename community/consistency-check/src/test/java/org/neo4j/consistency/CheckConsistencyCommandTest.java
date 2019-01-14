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
package org.neo4j.consistency;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.PrintStream;
import java.nio.file.Path;
import java.nio.file.Paths;

import org.neo4j.commandline.admin.CommandFailed;
import org.neo4j.commandline.admin.CommandLocator;
import org.neo4j.commandline.admin.IncorrectUsage;
import org.neo4j.commandline.admin.OutsideWorld;
import org.neo4j.commandline.admin.Usage;
import org.neo4j.consistency.checking.full.ConsistencyCheckIncompleteException;
import org.neo4j.consistency.checking.full.ConsistencyFlags;
import org.neo4j.helpers.progress.ProgressMonitorFactory;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.logging.LogProvider;
import org.neo4j.test.rule.TestDirectory;

import static org.hamcrest.CoreMatchers.containsString;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class CheckConsistencyCommandTest
{
    @Rule
    public TestDirectory testDir = TestDirectory.testDirectory( getClass() );

    @Rule
    public ExpectedException expect = ExpectedException.none();

    @Test
    public void runsConsistencyChecker() throws Exception
    {
        ConsistencyCheckService consistencyCheckService = mock( ConsistencyCheckService.class );

        Path homeDir = testDir.directory( "home" ).toPath();
        OutsideWorld outsideWorld = mock( OutsideWorld.class );
        CheckConsistencyCommand checkConsistencyCommand =
                new CheckConsistencyCommand( homeDir, testDir.directory( "conf" ).toPath(), outsideWorld,
                        consistencyCheckService );

        File databasePath = new File( homeDir.toFile(), "data/databases/mydb" );

        when( consistencyCheckService
                .runFullConsistencyCheck( eq( databasePath ), any( Config.class ), any( ProgressMonitorFactory.class ),
                        any( LogProvider.class ), any( FileSystemAbstraction.class ), eq( false ), any(),
                        any( ConsistencyFlags.class ) ) )
                .thenReturn( ConsistencyCheckService.Result.success( null ) );

        checkConsistencyCommand.execute( new String[]{"--database=mydb"} );

        verify( consistencyCheckService )
                .runFullConsistencyCheck( eq( databasePath ), any( Config.class ), any( ProgressMonitorFactory.class ),
                        any( LogProvider.class ), any( FileSystemAbstraction.class ), eq( false ), any(),
                        any( ConsistencyFlags.class ) );
    }

    @Test
    public void enablesVerbosity() throws Exception
    {
        ConsistencyCheckService consistencyCheckService = mock( ConsistencyCheckService.class );

        Path homeDir = testDir.directory( "home" ).toPath();
        OutsideWorld outsideWorld = mock( OutsideWorld.class );
        CheckConsistencyCommand checkConsistencyCommand =
                new CheckConsistencyCommand( homeDir, testDir.directory( "conf" ).toPath(), outsideWorld,
                        consistencyCheckService );

        File databasePath = new File( homeDir.toFile(), "data/databases/mydb" );

        when( consistencyCheckService
                .runFullConsistencyCheck( eq( databasePath ), any( Config.class ), any( ProgressMonitorFactory.class ),
                        any( LogProvider.class ), any( FileSystemAbstraction.class ), eq( true ), any(),
                        any( ConsistencyFlags.class ) ) )
                .thenReturn( ConsistencyCheckService.Result.success( null ) );

        checkConsistencyCommand.execute( new String[]{"--database=mydb", "--verbose"} );

        verify( consistencyCheckService )
                .runFullConsistencyCheck( eq( databasePath ), any( Config.class ), any( ProgressMonitorFactory.class ),
                        any( LogProvider.class ), any( FileSystemAbstraction.class ), eq( true ), any(),
                        any( ConsistencyFlags.class ) );
    }

    @Test
    public void failsWhenInconsistenciesAreFound() throws Exception
    {
        ConsistencyCheckService consistencyCheckService = mock( ConsistencyCheckService.class );

        Path homeDir = testDir.directory( "home" ).toPath();
        OutsideWorld outsideWorld = mock( OutsideWorld.class );
        CheckConsistencyCommand checkConsistencyCommand =
                new CheckConsistencyCommand( homeDir, testDir.directory( "conf" ).toPath(), outsideWorld,
                        consistencyCheckService );
        File databasePath = new File( homeDir.toFile(), "data/databases/mydb" );

        when( consistencyCheckService
                .runFullConsistencyCheck( eq( databasePath ), any( Config.class ), any( ProgressMonitorFactory.class ),
                        any( LogProvider.class ), any( FileSystemAbstraction.class ), eq( true ), any(),
                        any( ConsistencyFlags.class ) ) )
                .thenReturn( ConsistencyCheckService.Result.failure( new File( "/the/report/path" ) ) );

        try
        {
            checkConsistencyCommand.execute( new String[]{"--database=mydb", "--verbose"} );
        }
        catch ( CommandFailed e )
        {
            assertThat( e.getMessage(), containsString( new File( "/the/report/path" ).toString() ) );
        }
    }

    @Test
    public void shouldWriteReportFileToCurrentDirectoryByDefault()
            throws IOException, ConsistencyCheckIncompleteException, CommandFailed, IncorrectUsage

    {
        ConsistencyCheckService consistencyCheckService = mock( ConsistencyCheckService.class );

        Path homeDir = testDir.directory( "home" ).toPath();
        OutsideWorld outsideWorld = mock( OutsideWorld.class );
        CheckConsistencyCommand checkConsistencyCommand =
                new CheckConsistencyCommand( homeDir, testDir.directory( "conf" ).toPath(), outsideWorld,
                        consistencyCheckService );

        when( consistencyCheckService.runFullConsistencyCheck( any(), any(), any(), any(),
                any(), anyBoolean(), any(), any( ConsistencyFlags.class ) ) )
                .thenReturn( ConsistencyCheckService.Result.success( null ) );

        checkConsistencyCommand.execute( new String[]{"--database=mydb"} );

        verify( consistencyCheckService )
                .runFullConsistencyCheck( any(), any(), any(), any(), any(), anyBoolean(),
                        eq( new File( "." ).getCanonicalFile() ), any( ConsistencyFlags.class ) );
    }

    @Test
    public void shouldWriteReportFileToSpecifiedDirectory()
            throws IOException, ConsistencyCheckIncompleteException, CommandFailed, IncorrectUsage

    {
        ConsistencyCheckService consistencyCheckService = mock( ConsistencyCheckService.class );

        Path homeDir = testDir.directory( "home" ).toPath();
        OutsideWorld outsideWorld = mock( OutsideWorld.class );
        CheckConsistencyCommand checkConsistencyCommand =
                new CheckConsistencyCommand( homeDir, testDir.directory( "conf" ).toPath(), outsideWorld,
                        consistencyCheckService );

        when( consistencyCheckService.runFullConsistencyCheck( any(), any(), any(), any(),
                any(), anyBoolean(), any(), any( ConsistencyFlags.class ) ) )
                .thenReturn( ConsistencyCheckService.Result.success( null ) );

        checkConsistencyCommand.execute( new String[]{"--database=mydb", "--report-dir=some-dir-or-other"} );

        verify( consistencyCheckService )
                .runFullConsistencyCheck( any(), any(), any(), any(), any(),
                        anyBoolean(), eq( new File( "some-dir-or-other" ).getCanonicalFile() ),
                        any( ConsistencyFlags.class ) );
    }

    @Test
    public void shouldCanonicalizeReportDirectory()
            throws IOException, ConsistencyCheckIncompleteException, CommandFailed, IncorrectUsage

    {
        ConsistencyCheckService consistencyCheckService = mock( ConsistencyCheckService.class );

        Path homeDir = testDir.directory( "home" ).toPath();
        OutsideWorld outsideWorld = mock( OutsideWorld.class );
        CheckConsistencyCommand checkConsistencyCommand =
                new CheckConsistencyCommand( homeDir, testDir.directory( "conf" ).toPath(), outsideWorld,
                        consistencyCheckService );

        when( consistencyCheckService.runFullConsistencyCheck( any(), any(), any(), any(),
                any(), anyBoolean(), any(), any( ConsistencyFlags.class ) ) )
                .thenReturn( ConsistencyCheckService.Result.success( null ) );

        checkConsistencyCommand.execute( new String[]{"--database=mydb", "--report-dir=" + Paths.get( "..", "bar" )} );

        verify( consistencyCheckService )
                .runFullConsistencyCheck( any(), any(), any(), any(), any(),
                        anyBoolean(), eq( new File( "../bar" ).getCanonicalFile() ),
                        any( ConsistencyFlags.class ) );
    }

    @Test
    public void passesOnCheckParameters() throws Exception

    {
        ConsistencyCheckService consistencyCheckService = mock( ConsistencyCheckService.class );

        Path homeDir = testDir.directory( "home" ).toPath();
        OutsideWorld outsideWorld = mock( OutsideWorld.class );
        CheckConsistencyCommand checkConsistencyCommand =
                new CheckConsistencyCommand( homeDir, testDir.directory( "conf" ).toPath(), outsideWorld,
                        consistencyCheckService );

        when( consistencyCheckService.runFullConsistencyCheck( any(), any(), any(), any(),
                any(), anyBoolean(), any(), any( ConsistencyFlags.class ) ) )
                .thenReturn( ConsistencyCheckService.Result.success( null ) );

        checkConsistencyCommand.execute( new String[]{"--database=mydb", "--check-graph=false",
                "--check-indexes=false", "--check-label-scan-store=false", "--check-property-owners=true"} );

        verify( consistencyCheckService )
                .runFullConsistencyCheck( any(), any(), any(), any(), any(), anyBoolean(),
                        any(), eq( new ConsistencyFlags( false, false, false, true ) ) );
    }

    @Test
    public void databaseAndBackupAreMutuallyExclusive() throws Exception
    {
        ConsistencyCheckService consistencyCheckService = mock( ConsistencyCheckService.class );

        Path homeDir = testDir.directory( "home" ).toPath();
        OutsideWorld outsideWorld = mock( OutsideWorld.class );
        CheckConsistencyCommand checkConsistencyCommand =
                new CheckConsistencyCommand( homeDir, testDir.directory( "conf" ).toPath(), outsideWorld,
                        consistencyCheckService );

        when( consistencyCheckService.runFullConsistencyCheck( any(), any(), any(), any(),
                any(), anyBoolean(), any( ConsistencyFlags.class ) ) )
                .thenReturn( ConsistencyCheckService.Result.success( null ) );

        expect.expect( IncorrectUsage.class );
        expect.expectMessage( "Only one of '--database' and '--backup' can be specified." );

        checkConsistencyCommand.execute( new String[]{"--database=foo", "--backup=bar"} );
    }

    @Test
    public void backupNeedsToBePath() throws Exception
    {
        ConsistencyCheckService consistencyCheckService = mock( ConsistencyCheckService.class );

        Path homeDir = testDir.directory( "home" ).toPath();
        OutsideWorld outsideWorld = mock( OutsideWorld.class );
        CheckConsistencyCommand checkConsistencyCommand =
                new CheckConsistencyCommand( homeDir, testDir.directory( "conf" ).toPath(), outsideWorld,
                        consistencyCheckService );

        File backupPath = new File( homeDir.toFile(), "dir/does/not/exist" );

        expect.expect( CommandFailed.class );
        expect.expectMessage( "Specified backup should be a directory: " + backupPath );

        checkConsistencyCommand.execute( new String[]{"--backup=" + backupPath} );
    }

    @Test
    public void canRunOnBackup() throws Exception
    {
        ConsistencyCheckService consistencyCheckService = mock( ConsistencyCheckService.class );

        Path backupDir = testDir.directory( "backup" ).toPath();
        Path homeDir = testDir.directory( "home" ).toPath();
        OutsideWorld outsideWorld = mock( OutsideWorld.class );
        CheckConsistencyCommand checkConsistencyCommand =
                new CheckConsistencyCommand( homeDir, testDir.directory( "conf" ).toPath(), outsideWorld,
                        consistencyCheckService );

        when( consistencyCheckService
                .runFullConsistencyCheck( eq( backupDir.toFile() ), any( Config.class ),
                        any( ProgressMonitorFactory.class ),
                        any( LogProvider.class ), any( FileSystemAbstraction.class ), eq( false ), any(),
                        any( ConsistencyFlags.class ) ) )
                .thenReturn( ConsistencyCheckService.Result.success( null ) );

        checkConsistencyCommand.execute( new String[]{"--backup=" + backupDir} );

        verify( consistencyCheckService )
                .runFullConsistencyCheck( eq( backupDir.toFile() ), any( Config.class ),
                        any( ProgressMonitorFactory.class ),
                        any( LogProvider.class ), any( FileSystemAbstraction.class ), eq( false ), any(),
                        any( ConsistencyFlags.class ) );
    }

    @Test
    public void shouldPrintNiceHelp() throws Throwable
    {
        try ( ByteArrayOutputStream baos = new ByteArrayOutputStream() )
        {
            PrintStream ps = new PrintStream( baos );

            Usage usage = new Usage( "neo4j-admin", mock( CommandLocator.class ) );
            usage.printUsageForCommand( new CheckConsistencyCommandProvider(), ps::println );

            assertEquals( String.format( "usage: neo4j-admin check-consistency [--database=<name>]%n" +
                            "                                     [--backup=</path/to/backup>]%n" +
                            "                                     [--verbose[=<true|false>]]%n" +
                            "                                     [--report-dir=<directory>]%n" +
                            "                                     [--additional-config=<config-file-path>]%n" +
                            "                                     [--check-graph[=<true|false>]]%n" +
                            "                                     [--check-indexes[=<true|false>]]%n" +
                            "                                     [--check-label-scan-store[=<true|false>]]%n" +
                            "                                     [--check-property-owners[=<true|false>]]%n" +
                            "%n" +
                            "environment variables:%n" +
                            "    NEO4J_CONF    Path to directory which contains neo4j.conf.%n" +
                            "    NEO4J_DEBUG   Set to anything to enable debug output.%n" +
                            "    NEO4J_HOME    Neo4j home directory.%n" +
                            "    HEAP_SIZE     Set JVM maximum heap size during command execution.%n" +
                            "                  Takes a number and a unit, for example 512m.%n" +
                            "%n" +
                            "This command allows for checking the consistency of a database or a backup%n" +
                            "thereof. It cannot be used with a database which is currently in use.%n" +
                            "%n" +
                            "All checks except 'check-graph' can be quite expensive so it may be useful to%n" +
                            "turn them off for very large databases. Increasing the heap size can also be a%n" +
                            "good idea. See 'neo4j-admin help' for details.%n" +
                            "%n" +
                            "options:%n" +
                            "  --database=<name>                        Name of database. [default:graph.db]%n" +
                            "  --backup=</path/to/backup>               Path to backup to check consistency%n" +
                            "                                           of. Cannot be used together with%n" +
                            "                                           --database. [default:]%n" +
                            "  --verbose=<true|false>                   Enable verbose output.%n" +
                            "                                           [default:false]%n" +
                            "  --report-dir=<directory>                 Directory to write report file in.%n" +
                            "                                           [default:.]%n" +
                            "  --additional-config=<config-file-path>   Configuration file to supply%n" +
                            "                                           additional configuration in. This%n" +
                            "                                           argument is DEPRECATED. [default:]%n" +
                            "  --check-graph=<true|false>               Perform checks between nodes,%n" +
                            "                                           relationships, properties, types and%n" +
                            "                                           tokens. [default:true]%n" +
                            "  --check-indexes=<true|false>             Perform checks on indexes.%n" +
                            "                                           [default:true]%n" +
                            "  --check-label-scan-store=<true|false>    Perform checks on the label scan%n" +
                            "                                           store. [default:true]%n" +
                            "  --check-property-owners=<true|false>     Perform additional checks on property%n" +
                            "                                           ownership. This check is *very*%n" +
                            "                                           expensive in time and memory.%n" +
                            "                                           [default:false]%n" ),
                    baos.toString() );
        }
    }
}
