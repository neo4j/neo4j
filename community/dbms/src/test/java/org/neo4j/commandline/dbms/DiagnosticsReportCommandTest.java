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
import org.junit.jupiter.api.condition.JRE;
import org.junit.jupiter.api.extension.ExtendWith;

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

import org.neo4j.commandline.admin.CommandFailed;
import org.neo4j.commandline.admin.IncorrectUsage;
import org.neo4j.commandline.admin.OutsideWorld;
import org.neo4j.commandline.admin.RealOutsideWorld;
import org.neo4j.diagnostics.DiagnosticsOfflineReportProvider;
import org.neo4j.diagnostics.DiagnosticsReportSource;
import org.neo4j.io.fs.DefaultFileSystemAbstraction;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.test.extension.DefaultFileSystemExtension;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.SuppressOutputExtension;
import org.neo4j.test.extension.TestDirectoryExtension;
import org.neo4j.test.rule.TestDirectory;

import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.neo4j.commandline.dbms.DiagnosticsReportCommand.DEFAULT_CLASSIFIERS;
import static org.neo4j.commandline.dbms.DiagnosticsReportCommand.describeClassifier;

@ExtendWith( {TestDirectoryExtension.class, DefaultFileSystemExtension.class, SuppressOutputExtension.class} )
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

    public static class MyDiagnosticsOfflineReportProvider extends DiagnosticsOfflineReportProvider
    {
        public MyDiagnosticsOfflineReportProvider()
        {
            super( "my-provider", "logs", "tx" );
        }

        @Override
        public void init( FileSystemAbstraction fs, Config config, File storeDirectory )
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
    }

    @AfterEach
    void tearDown()
    {
        // Restore directory
        System.setProperty( "user.dir", originalUserDir );
    }

    @Test
    void exitIfConfigFileIsMissing() throws IOException
    {
        Files.delete( configFile );
        String[] args = {"--list"};
        try ( RealOutsideWorld outsideWorld = new RealOutsideWorld() )
        {
            DiagnosticsReportCommand diagnosticsReportCommand = new DiagnosticsReportCommand( homeDir, configDir, outsideWorld );

            CommandFailed commandFailed = assertThrows( CommandFailed.class, () -> diagnosticsReportCommand.execute( args ) );
            assertThat( commandFailed.getMessage(), containsString( "Unable to find config file, tried: " ) );
        }
    }

    @Test
    void allHasToBeOnlyClassifier() throws Exception
    {
        String[] args = {"all", "logs", "tx"};
        try ( RealOutsideWorld outsideWorld = new RealOutsideWorld() )
        {
            DiagnosticsReportCommand diagnosticsReportCommand = new DiagnosticsReportCommand( homeDir, configDir, outsideWorld );

            IncorrectUsage incorrectUsage = assertThrows( IncorrectUsage.class, () -> diagnosticsReportCommand.execute( args ) );
            assertEquals( "If you specify 'all' this has to be the only classifier. Found ['logs','tx'] as well.", incorrectUsage.getMessage() );
        }
    }

    @Test
    void printUnrecognizedClassifiers() throws Exception
    {
        String[] args = {"logs", "tx", "invalid"};
        try ( RealOutsideWorld outsideWorld = new RealOutsideWorld() )
        {
            DiagnosticsReportCommand
                    diagnosticsReportCommand = new DiagnosticsReportCommand( homeDir, configDir, outsideWorld );

            IncorrectUsage incorrectUsage = assertThrows( IncorrectUsage.class, () -> diagnosticsReportCommand.execute( args ) );
            assertEquals( "Unknown classifier: invalid", incorrectUsage.getMessage() );
        }
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
            OutsideWorld outsideWorld = mock( OutsideWorld.class );
            when( outsideWorld.fileSystem() ).thenReturn( fs );
            when( outsideWorld.outStream() ).thenReturn( ps );

            DiagnosticsReportCommand
                    diagnosticsReportCommand = new DiagnosticsReportCommand( homeDir, configDir, outsideWorld );
            diagnosticsReportCommand.execute( args );

            assertThat( baos.toString(), is(String.format(
                    "Finding running instance of neo4j%n" +
                            "No running instance of neo4j was found. Online reports will be omitted.%n" +
                            "If neo4j is running but not detected, you can supply the process id of the running instance with --pid%n" +
                            "All available classifiers:%n" +
                            "  config     include configuration file%n" +
                            "  logs       include log files%n" +
                            "  plugins    include a view of the plugin directory%n" +
                            "  ps         include a list of running processes%n" +
                            "  tree       include a view of the tree structure of the data directory%n" +
                            "  tx         include transaction logs%n" ) ) );
        }
    }

    @Test
    void overrideDestination() throws Exception
    {
        // because of https://bugs.openjdk.java.net/browse/JDK-8202127 and current surefire behaviour we need to have custom value for JRE >= 11
        String toArgument = JRE.JAVA_11.isCurrentVersion() ? "--to=" + System.getProperty( "user.dir" ) + "/other/" : "--to=other/";
        String[] args = {toArgument, "all"};
        try ( RealOutsideWorld outsideWorld = new RealOutsideWorld() )
        {
            DiagnosticsReportCommand
                    diagnosticsReportCommand = new DiagnosticsReportCommand( homeDir, configDir, outsideWorld );
            diagnosticsReportCommand.execute( args );

            File other = testDirectory.directory( "other" );
            FileSystemAbstraction fs = outsideWorld.fileSystem();
            assertThat( fs.fileExists( other ), is( true ) );
            assertThat( fs.listFiles( other ).length, is( 1 ) );

            // Default should be empty
            File reports = new File( testDirectory.directory(), "reports" );
            assertThat( fs.fileExists( reports ), is( false ) );
        }
    }

    @Test
    void errorOnInvalidPid() throws Exception
    {
        String[] args = {"--pid=a", "all"};
        try ( RealOutsideWorld outsideWorld = new RealOutsideWorld() )
        {
            DiagnosticsReportCommand
                    diagnosticsReportCommand = new DiagnosticsReportCommand( homeDir, configDir, outsideWorld );
            CommandFailed commandFailed = assertThrows( CommandFailed.class, () -> diagnosticsReportCommand.execute( args ) );
            assertEquals( "Unable to parse --pid", commandFailed.getMessage() );
        }
    }
}
