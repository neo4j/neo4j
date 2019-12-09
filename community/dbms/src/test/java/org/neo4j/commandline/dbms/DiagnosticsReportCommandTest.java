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

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.parallel.ResourceLock;
import org.junit.jupiter.api.parallel.Resources;
import picocli.CommandLine;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.PrintStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import javax.annotation.Nonnull;

import org.neo4j.annotations.service.ServiceProvider;
import org.neo4j.cli.CommandFailedException;
import org.neo4j.cli.ExecutionContext;
import org.neo4j.configuration.Config;
import org.neo4j.io.fs.DefaultFileSystemAbstraction;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.kernel.diagnostics.DiagnosticsOfflineReportProvider;
import org.neo4j.kernel.diagnostics.DiagnosticsReportSource;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.SuppressOutputExtension;
import org.neo4j.test.extension.testdirectory.TestDirectoryExtension;
import org.neo4j.test.rule.TestDirectory;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.neo4j.commandline.dbms.DiagnosticsReportCommand.DEFAULT_CLASSIFIERS;
import static org.neo4j.commandline.dbms.DiagnosticsReportCommand.describeClassifier;

@TestDirectoryExtension
@ExtendWith( SuppressOutputExtension.class )
@ResourceLock( Resources.SYSTEM_OUT )
public class DiagnosticsReportCommandTest
{
    @Inject
    private TestDirectory testDirectory;
    @Inject
    private DefaultFileSystemAbstraction fs;

    private Path homeDir;
    private Path configDir;
    private Path configFile;
    private String originalUserDir;
    private ExecutionContext ctx;

    @ServiceProvider
    public static class MyDiagnosticsOfflineReportProvider extends DiagnosticsOfflineReportProvider
    {
        public MyDiagnosticsOfflineReportProvider()
        {
            super( "logs", "tx" );
        }

        @Override
        public void init( FileSystemAbstraction fs, String defaultDatabaseName, Config config, File storeDirectory )
        {
        }

        @Nonnull
        @Override
        protected List<DiagnosticsReportSource> provideSources( Set<String> classifiers )
        {
            return Collections.emptyList();
        }
    }

    @BeforeEach
    void setUp() throws Exception
    {
        homeDir = testDirectory.directory( "home-dir" ).toPath();
        configDir = testDirectory.directory( "config-dir" ).toPath();

        // Touch config
        configFile = configDir.resolve( "neo4j.conf" );
        Files.createFile( configFile );

        // To make sure files are resolved from the working directory
        originalUserDir = System.setProperty( "user.dir", testDirectory.absolutePath().getAbsolutePath() );

        ctx = new ExecutionContext( homeDir, configDir, System.out, System.err, fs );
    }

    @AfterEach
    void tearDown()
    {
        // Restore directory
        System.setProperty( "user.dir", originalUserDir );
    }

    @Test
    void printUsageHelp()
    {
        final var baos = new ByteArrayOutputStream();
        final var command = new DiagnosticsReportCommand( new ExecutionContext( Path.of( "." ), Path.of( "." ) ) );
        try ( var out = new PrintStream( baos ) )
        {
            CommandLine.usage( command, new PrintStream( out ) );
        }
        assertThat( baos.toString().trim() ).isEqualTo( String.format(
                "Produces a zip/tar of the most common information needed for remote assessments.%n" +
                "%n" +
                "USAGE%n" +
                "%n" +
                "report [--force] [--list] [--verbose] [--pid=<pid>] [--to=<path>]%n" +
                "       [<classifier>...]%n" +
                "%n" +
                "DESCRIPTION%n" +
                "%n" +
                "Will collect information about the system and package everything in an archive.%n" +
                "If you specify 'all', everything will be included. You can also fine tune the%n" +
                "selection by passing classifiers to the tool, e.g 'logs tx threads'.%n" +
                "%n" +
                "PARAMETERS%n" +
                "%n" +
                "      [<classifier>...]     Default: [config, logs, metrics, plugins, ps,%n" +
                "                            sysprop, threads, tree]%n" +
                "%n" +
                "OPTIONS%n" +
                "%n" +
                "      --verbose           Enable verbose output.%n" +
                "      --list              List all available classifiers%n" +
                "      --force             Ignore disk full warning%n" +
                "      --to=<path>         Destination directory for reports. Defaults to a%n" +
                "                            system tmp directory.%n" +
                "      --pid=<pid>         Specify process id of running neo4j instance"
        ) );
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
    void allHasToBeOnlyClassifier() throws Exception
    {
        String[] args = {"all", "logs", "tx"};
        DiagnosticsReportCommand diagnosticsReportCommand = new DiagnosticsReportCommand( ctx );
        CommandLine.populateCommand( diagnosticsReportCommand, args );

        CommandFailedException incorrectUsage = assertThrows( CommandFailedException.class, diagnosticsReportCommand::execute );
        assertEquals( "If you specify 'all' this has to be the only classifier. Found ['logs','tx'] as well.", incorrectUsage.getMessage() );
    }

    @Test
    void printUnrecognizedClassifiers() throws Exception
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
                            "  tx         include transaction logs%n" ) );
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

        File other = testDirectory.directory( "other" );
        assertThat( ctx.fs().fileExists( other ) ).isEqualTo( true );
        assertThat( ctx.fs().listFiles( other ).length ).isEqualTo( 1 );

        // Default should be empty
        File reports = new File( testDirectory.homeDir(), "reports" );
        assertThat( ctx.fs().fileExists( reports ) ).isEqualTo( false );
    }
}
