/*
 * Copyright (c) "Neo4j"
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

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.parallel.ResourceLock;
import org.junit.jupiter.api.parallel.Resources;
import picocli.CommandLine;

import java.io.IOException;
import java.net.URI;
import java.nio.file.FileSystem;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Collections;

import org.neo4j.cli.CommandFailedException;
import org.neo4j.cli.ExecutionContext;
import org.neo4j.configuration.GraphDatabaseSettings;
import org.neo4j.dbms.api.DatabaseManagementService;
import org.neo4j.io.fs.FileUtils;
import org.neo4j.test.extension.DbmsExtension;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.SuppressOutputExtension;
import org.neo4j.test.extension.testdirectory.TestDirectoryExtension;
import org.neo4j.test.rule.TestDirectory;

import static java.util.Collections.singletonList;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertTrue;

@TestDirectoryExtension
@ExtendWith( SuppressOutputExtension.class )
@ResourceLock( Resources.SYSTEM_OUT )
@DbmsExtension
class DiagnosticsReportCommandIT
{
    @Inject
    private TestDirectory testDirectory;
    @Inject
    private DatabaseManagementService managementService;

    @Test
    void shouldBeAbleToAttachToPidAndRunThreadDump() throws IOException
    {
        long pid = getPID();
        assertThat( pid ).isNotEqualTo( 0 );

        // Write config file
        Files.createFile( testDirectory.file( "neo4j.conf" ) );

        // write neo4j.pid file
        Path run = testDirectory.directory( "run" );
        Files.write( run.resolve( "neo4j.pid" ), String.valueOf( pid ).getBytes() );

        // Run command, should detect running instance
        try
        {
            String[] args = {"threads", "--to=" + testDirectory.absolutePath() + "/reports"};
            Path homeDir = testDirectory.homePath();
            var ctx = new ExecutionContext( homeDir, homeDir, System.out, System.err, testDirectory.getFileSystem() );
            DiagnosticsReportCommand diagnosticsReportCommand = new DiagnosticsReportCommand( ctx );
            CommandLine.populateCommand( diagnosticsReportCommand, args );
            diagnosticsReportCommand.execute();
        }
        catch ( CommandFailedException e )
        {
            if ( e.getMessage().equals( "Unknown classifier: threads" ) )
            {
                return; // If we get attach API is not available for example in some IBM jdk installs, ignore this test
            }
            throw e;
        }

        // Verify that we took a thread dump
        Path reports = testDirectory.directory( "reports" );
        Path[] files = FileUtils.listPaths( reports );
        assertThat( files ).isNotNull();
        assertThat( files.length ).isEqualTo( 1 );

        Path report = files[0];
        final URI uri = URI.create( "jar:file:" + report.toUri().getRawPath() );

        try ( FileSystem fs = FileSystems.newFileSystem( uri, Collections.emptyMap() ) )
        {
            String threadDump = Files.readString( fs.getPath( "threaddump.txt" ) );
            assertThat( threadDump ).contains( DiagnosticsReportCommandIT.class.getCanonicalName() );
        }
    }

    @Test
    void shouldBeAbleToAttachToPidAndRunHeapDump() throws IOException
    {
        long pid = getPID();
        assertThat( pid ).isNotEqualTo( 0 );

        // Write config file
        Files.createFile( testDirectory.file( "neo4j.conf" ) );

        // write neo4j.pid file
        Path run = testDirectory.directory( "run" );
        Files.write( run.resolve( "neo4j.pid" ), String.valueOf( pid ).getBytes() );

        // Run command, should detect running instance
        try
        {
            String[] args = {"heap", "--to=" + testDirectory.absolutePath() + "/reports"};
            Path homeDir = testDirectory.homePath();
            var ctx = new ExecutionContext( homeDir, homeDir, System.out, System.err, testDirectory.getFileSystem() );
            DiagnosticsReportCommand diagnosticsReportCommand = new DiagnosticsReportCommand( ctx );
            CommandLine.populateCommand( diagnosticsReportCommand, args );
            diagnosticsReportCommand.execute();
        }
        catch ( CommandFailedException e )
        {
            if ( e.getMessage().equals( "Unknown classifier: heap" ) )
            {
                return; // If we get attach API is not available for example in some IBM jdk installs, ignore this test
            }
            throw e;
        }

        // Verify that we took a heap dump
        Path reports = testDirectory.directory( "reports" );
        Path[] files = FileUtils.listPaths( reports );
        assertThat( files ).isNotNull();
        assertThat( files.length ).isEqualTo( 1 );

        try ( FileSystem fs = FileSystems.newFileSystem( files[0], null ) )
        {
            assertTrue( Files.exists( fs.getPath( "heapdump.hprof" ) ) );
        }
    }

    @Test
    void shouldHandleRotatedLogFiles() throws IOException
    {
        // Write config file and specify a custom name for the neo4j.log file.
        Path confFile = testDirectory.createFile( "neo4j.conf" );
        Files.write( confFile, singletonList( GraphDatabaseSettings.store_user_log_path.name() + "=custom.neo4j.log.name" ) );

        // Create some log files that should be found. debug.log has already been created during setup.
        Files.createFile( testDirectory.homePath().resolve( "logs/debug.log.1.zip" ) );
        Files.createFile( testDirectory.homePath().resolve( "logs/custom.neo4j.log.name" ) );
        Files.createFile( testDirectory.homePath().resolve( "logs/custom.neo4j.log.name.1" ) );

        String[] args = {"logs", "--to=" + testDirectory.absolutePath() + "/reports"};
        Path homeDir = testDirectory.homePath();
        var ctx = new ExecutionContext( homeDir, homeDir, System.out, System.err, testDirectory.getFileSystem() );
        DiagnosticsReportCommand diagnosticsReportCommand = new DiagnosticsReportCommand( ctx );
        CommandLine.populateCommand( diagnosticsReportCommand, args );
        diagnosticsReportCommand.execute();

        Path reports = testDirectory.directory( "reports" );
        Path[] files = FileUtils.listPaths( reports );
        assertThat( files.length ).isEqualTo( 1 );

        try ( FileSystem fileSystem = FileSystems.newFileSystem( files[0], null ) )
        {
            Path logsDir = fileSystem.getPath( "logs" );
            assertTrue( Files.exists( logsDir.resolve( "debug.log" ) ) );
            assertTrue( Files.exists( logsDir.resolve( "debug.log.1.zip" ) ) );
            assertTrue( Files.exists( logsDir.resolve( "custom.neo4j.log.name" ) ) );
            assertTrue( Files.exists( logsDir.resolve( "custom.neo4j.log.name.1" ) ) );
        }
    }

    private static long getPID()
    {
        String processName = java.lang.management.ManagementFactory.getRuntimeMXBean().getName();
        if ( processName != null && !processName.isEmpty() )
        {
            try
            {
                return Long.parseLong( processName.split( "@" )[0] );
            }
            catch ( Exception ignored )
            {
            }
        }

        return 0;
    }
}
