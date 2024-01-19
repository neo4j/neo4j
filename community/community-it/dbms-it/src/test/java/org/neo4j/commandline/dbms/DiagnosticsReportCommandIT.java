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

import java.nio.file.StandardOpenOption;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.parallel.ResourceLock;
import org.junit.jupiter.api.parallel.Resources;
import picocli.CommandLine;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.PrintStream;
import java.net.URI;
import java.nio.file.FileSystem;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Collections;

import org.neo4j.cli.CommandFailedException;
import org.neo4j.cli.ExecutionContext;
import org.neo4j.configuration.GraphDatabaseSettings;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.fs.FileUtils;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.SuppressOutputExtension;
import org.neo4j.test.extension.testdirectory.TestDirectoryExtension;
import org.neo4j.test.utils.TestDirectory;

import static java.util.Collections.singletonList;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.neo4j.commandline.dbms.DiagnosticsReportCommand.DEFAULT_CLASSIFIERS;
import static org.neo4j.commandline.dbms.DiagnosticsReportCommand.describeClassifier;

@TestDirectoryExtension
@ExtendWith( SuppressOutputExtension.class )
@ResourceLock( Resources.SYSTEM_OUT )
class DiagnosticsReportCommandIT
{
    @Inject
    private TestDirectory testDirectory;
    @Inject
    private FileSystemAbstraction fs;

    private Path homeDir;
    private Path configDir;
    private Path configFile;
    private String originalUserDir;
    private ExecutionContext ctx;

    @BeforeEach
    void setUp() throws Exception
    {
        homeDir = testDirectory.directory( "home-dir" );
        createDatabaseDir(homeDir);
        configDir = testDirectory.directory( "config-dir" );

        // Touch config
        configFile = configDir.resolve( "neo4j.conf" );
        Files.createFile( configFile );

        // To make sure files are resolved from the working directory
        originalUserDir = System.setProperty( "user.dir", testDirectory.absolutePath().toString() );

        ctx = new ExecutionContext( homeDir, configDir, System.out, System.err, fs );
    }

    private void createDatabaseDir( Path homeDir ) throws IOException
    {
        // Database directory needed for command to be able to collect anything
        Files.createDirectories( homeDir.resolve( "data" ).resolve( "databases" ).resolve( "neo4j" ));
    }

    @AfterEach
    void tearDown()
    {
        // Restore directory
        System.setProperty( "user.dir", originalUserDir );
    }

    @Test
    void shouldBeAbleToAttachToPidAndRunThreadDump() throws IOException
    {
        long pid = getPID();
        assertThat( pid ).isNotEqualTo( 0 );

        // write neo4j.pid file
        Path run = testDirectory.directory( "run", homeDir.getFileName().toString());
        Files.write( run.resolve( "neo4j.pid" ), String.valueOf( pid ).getBytes() );

        // Run command, should detect running instance
        try
        {
            String[] args = {"threads", "--to=" + testDirectory.absolutePath() + "/reports"};
            var ctx = new ExecutionContext( homeDir, configDir, System.out, System.err, testDirectory.getFileSystem() );
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

        // write neo4j.pid file
        Path run = testDirectory.directory( "run", homeDir.getFileName().toString() );
        Files.write( run.resolve( "neo4j.pid" ), String.valueOf( pid ).getBytes(), StandardOpenOption.CREATE );

        // Run command, should detect running instance
        try
        {
            String[] args = {"heap", "--to=" + testDirectory.absolutePath() + "/reports"};
            var ctx = new ExecutionContext( homeDir, configDir, System.out, System.err, testDirectory.getFileSystem() );
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
        Files.write(
                configDir.resolve("neo4j.conf"), singletonList(GraphDatabaseSettings.store_user_log_path.name() + "=custom.neo4j.log.name"));

        // Create some log files that should be found.
        Path logDir =
                testDirectory.directory( "logs", homeDir.getFileName().toString() );
        FileSystemAbstraction fs = testDirectory.getFileSystem();
        fs.write( logDir.resolve( "debug.log" ) );
        fs.write( logDir.resolve( "debug.log.1.zip" ) );
        fs.write( logDir.resolve( "custom.neo4j.log.name" ) );
        fs.write( logDir.resolve( "custom.neo4j.log.name.1" ) );

        String[] args = {"logs", "--to=" + testDirectory.absolutePath() + "/reports"};
        var ctx = new ExecutionContext( homeDir, configDir, System.out, System.err, testDirectory.getFileSystem() );
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

    @Test
    void exitIfConfigFileIsMissing() throws IOException
    {
        Files.delete( configFile );
        String[] args = {"--list"};
        DiagnosticsReportCommand diagnosticsReportCommand = new DiagnosticsReportCommand( ctx );
        CommandLine.populateCommand( diagnosticsReportCommand, args );
        CommandFailedException commandFailed = assertThrows( CommandFailedException.class, diagnosticsReportCommand::execute );
        assertThat( commandFailed.getMessage() ).contains( "Unable to find config file, tried: " );
    }

    @Test
    void allHasToBeOnlyClassifier()
    {
        String[] args = {"all", "logs", "tx"};
        DiagnosticsReportCommand diagnosticsReportCommand = new DiagnosticsReportCommand( ctx );
        CommandLine.populateCommand( diagnosticsReportCommand, args );

        CommandFailedException incorrectUsage = assertThrows( CommandFailedException.class, diagnosticsReportCommand::execute );
        assertEquals( "If you specify 'all' this has to be the only classifier. Found ['logs','tx'] as well.", incorrectUsage.getMessage() );
    }

    @Test
    void printUnrecognizedClassifiers()
    {
        String[] args = {"logs", "tx", "invalid"};
        DiagnosticsReportCommand diagnosticsReportCommand = new DiagnosticsReportCommand( ctx );
        CommandLine.populateCommand( diagnosticsReportCommand, args );

        CommandFailedException incorrectUsage = assertThrows( CommandFailedException.class, diagnosticsReportCommand::execute );
        assertEquals( "Unknown classifier: invalid", incorrectUsage.getMessage() );
    }

    @SuppressWarnings( "ResultOfMethodCallIgnored" )
    @Test
    void defaultValuesShouldBeValidClassifiers()
    {
        for ( String classifier : DEFAULT_CLASSIFIERS )
        {
            describeClassifier( classifier );
        }

        // Make sure the above actually catches bad classifiers
        IllegalArgumentException exception = assertThrows( IllegalArgumentException.class, () -> describeClassifier( "invalid" ) );
        assertEquals( "Unknown classifier: invalid", exception.getMessage() );
    }

    @Test
    void listShouldDisplayAllClassifiers() throws Exception
    {
        try ( ByteArrayOutputStream baos = new ByteArrayOutputStream() )
        {
            PrintStream ps = new PrintStream( baos );
            String[] args = {"--list"};

            ctx = new ExecutionContext( homeDir, configDir, ps, System.err, fs );
            DiagnosticsReportCommand diagnosticsReportCommand = new DiagnosticsReportCommand( ctx );
            CommandLine.populateCommand( diagnosticsReportCommand, args );
            diagnosticsReportCommand.execute();

            assertThat( baos.toString() ).isEqualTo( String.format(
                    "Finding running instance of neo4j%n" +
                            "No running instance of neo4j was found. Online reports will be omitted.%n" +
                            "If neo4j is running but not detected, you can supply the process id of the running instance with --pid%n" +
                            "All available classifiers:%n" +
                            "  config     include configuration file%n" +
                            "  logs       include log files%n" +
                            "  plugins    include a view of the plugin directory%n" +
                            "  ps         include a list of running processes%n" +
                            "  tree       include a view of the tree structure of the data directory%n" +
                            "  tx         include transaction logs%n" +
                            "  version    include version of neo4j%n" ) );
        }
    }

    @Test
    void overrideDestination() throws Exception
    {
        String toArgument = "--to=" + System.getProperty( "user.dir" ) + "/other/";
        String[] args = {toArgument, "all"};

        DiagnosticsReportCommand diagnosticsReportCommand = new DiagnosticsReportCommand( ctx );
        CommandLine.populateCommand( diagnosticsReportCommand, args );
        diagnosticsReportCommand.execute();

        Path other = testDirectory.directory( "other" );
        assertThat( ctx.fs().fileExists( other ) ).isEqualTo( true );
        assertThat( ctx.fs().listFiles( other ).length ).isEqualTo( 1 );

        // Default should be empty
        Path reports = testDirectory.homePath().resolve( "reports" );
        assertThat( ctx.fs().fileExists( reports ) ).isEqualTo( false );
    }

    private static long getPID()
    {
        return ProcessHandle.current().pid();
    }
}
