/*
 * Copyright (c) 2002-2016 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
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
import org.neo4j.helpers.progress.ProgressMonitorFactory;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.logging.LogProvider;
import org.neo4j.test.rule.TestDirectory;

import static org.hamcrest.CoreMatchers.containsString;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyBoolean;
import static org.mockito.Matchers.anyObject;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.stub;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class CheckConsistencyCommandTest
{
    @Rule
    public TestDirectory testDir = TestDirectory.testDirectory( getClass() );

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
                        any( LogProvider.class ), any( FileSystemAbstraction.class ), eq( false ), anyObject() ) )
                .thenReturn( ConsistencyCheckService.Result.success( null ) );

        checkConsistencyCommand.execute( new String[]{"--database=mydb"} );

        verify( consistencyCheckService )
                .runFullConsistencyCheck( eq( databasePath ), any( Config.class ), any( ProgressMonitorFactory.class ),
                        any( LogProvider.class ), any( FileSystemAbstraction.class ), eq( false ), anyObject() );
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
                        any( LogProvider.class ), any( FileSystemAbstraction.class ), eq( true ), anyObject() ) )
                .thenReturn( ConsistencyCheckService.Result.success( null ) );

        checkConsistencyCommand.execute( new String[]{"--database=mydb", "--verbose"} );

        verify( consistencyCheckService )
                .runFullConsistencyCheck( eq( databasePath ), any( Config.class ), any( ProgressMonitorFactory.class ),
                        any( LogProvider.class ), any( FileSystemAbstraction.class ), eq( true ), anyObject() );
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
                        any( LogProvider.class ), any( FileSystemAbstraction.class ), eq( true ), anyObject() ) )
                .thenReturn( ConsistencyCheckService.Result.failure( new File( "/the/report/path" ) ) );

        try
        {
            checkConsistencyCommand.execute( new String[]{"--database=mydb", "--verbose"} );
        }
        catch ( CommandFailed e )
        {
            assertThat( e.getMessage(), containsString( new File("/the/report/path").toString() ) );
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

        stub( consistencyCheckService.runFullConsistencyCheck( anyObject(), anyObject(), anyObject(), anyObject(),
                anyObject(), anyBoolean(), anyObject() ) ).toReturn( ConsistencyCheckService.Result.success( null ) );

        checkConsistencyCommand.execute( new String[]{"--database=mydb"} );

        verify( consistencyCheckService )
                .runFullConsistencyCheck( anyObject(), anyObject(), anyObject(), anyObject(), anyObject(),
                        anyBoolean(), eq( new File( "." ).getCanonicalFile() ) );
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

        stub( consistencyCheckService.runFullConsistencyCheck( anyObject(), anyObject(), anyObject(), anyObject(),
                anyObject(), anyBoolean(), anyObject() ) ).toReturn( ConsistencyCheckService.Result.success( null ) );

        checkConsistencyCommand.execute( new String[]{"--database=mydb", "--report-dir=some-dir-or-other"} );

        verify( consistencyCheckService )
                .runFullConsistencyCheck( anyObject(), anyObject(), anyObject(), anyObject(), anyObject(),
                        anyBoolean(), eq( new File( "some-dir-or-other" ).getCanonicalFile() ) );
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

        stub( consistencyCheckService.runFullConsistencyCheck( anyObject(), anyObject(), anyObject(), anyObject(),
                anyObject(), anyBoolean(), anyObject() ) ).toReturn( ConsistencyCheckService.Result.success( null ) );

        checkConsistencyCommand.execute( new String[]{"--database=mydb", "--report-dir=" + Paths.get( "..", "bar" )} );

        verify( consistencyCheckService )
                .runFullConsistencyCheck( anyObject(), anyObject(), anyObject(), anyObject(), anyObject(),
                        anyBoolean(), eq( new File( "../bar" ).getCanonicalFile() ) );
    }

    @Test
    public void shouldPrintNiceHelp() throws Throwable
    {
        try ( ByteArrayOutputStream baos = new ByteArrayOutputStream() )
        {
            PrintStream ps = new PrintStream( baos );

            Usage usage = new Usage( "neo4j-admin", mock( CommandLocator.class ) );
            usage.printUsageForCommand( new CheckConsistencyCommand.Provider(), ps::println );

            assertEquals( String.format( "usage: neo4j-admin check-consistency [--database=<name>]%n" +
                            "                                     [--additional-config=<config-file-path>]%n" +
                            "                                     [--verbose[=<true|false>]]%n" +
                            "                                     [--report-dir=<directory>]%n" +
                            "%n" +
                            "Check the consistency of a database.%n" +
                            "%n" +
                            "options:%n" +
                            "  --database=<name>                        Name of database. [default:graph.db]%n" +
                            "  --additional-config=<config-file-path>   Configuration file to supply%n" +
                            "                                           additional configuration in.%n" +
                            "                                           [default:]%n" +
                            "  --verbose=<true|false>                   Enable verbose output.%n" +
                            "                                           [default:false]%n" +
                            "  --report-dir=<directory>                 Directory to write report file in.%n" +
                            "                                           [default:.]%n" ),
                    baos.toString() );
        }
    }
}
