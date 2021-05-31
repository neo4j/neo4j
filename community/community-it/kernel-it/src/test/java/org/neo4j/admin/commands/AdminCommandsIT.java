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
package org.neo4j.admin.commands;

import org.apache.commons.lang3.mutable.MutableObject;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.DisabledOnOs;
import org.junit.jupiter.api.condition.OS;
import picocli.CommandLine;

import java.io.PrintStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.attribute.PosixFilePermissions;
import java.util.Set;

import org.neo4j.cli.AbstractCommand;
import org.neo4j.cli.ExecutionContext;
import org.neo4j.commandline.admin.security.SetDefaultAdminCommand;
import org.neo4j.commandline.admin.security.SetInitialPasswordCommand;
import org.neo4j.commandline.dbms.DiagnosticsReportCommand;
import org.neo4j.commandline.dbms.DumpCommand;
import org.neo4j.commandline.dbms.LoadCommand;
import org.neo4j.commandline.dbms.MemoryRecommendationsCommand;
import org.neo4j.commandline.dbms.StoreInfoCommand;
import org.neo4j.configuration.BootloaderSettings;
import org.neo4j.configuration.GraphDatabaseSettings;
import org.neo4j.consistency.CheckConsistencyCommand;
import org.neo4j.dbms.archive.Dumper;
import org.neo4j.dbms.archive.Loader;
import org.neo4j.importer.ImportCommand;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.testdirectory.TestDirectoryExtension;
import org.neo4j.test.rule.TestDirectory;

import static java.nio.file.attribute.PosixFilePermission.OWNER_READ;
import static java.nio.file.attribute.PosixFilePermission.OWNER_WRITE;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;

@TestDirectoryExtension
// Config files created in our build server doesn't get the right permissions,
// However this test is only interested in the parsing of config entries with --expand-command option, so not necessary to run it on Windows.
@DisabledOnOs( OS.WINDOWS )
class AdminCommandsIT
{
    @Inject
    private TestDirectory testDirectory;
    private Path confDir;
    private PrintStream out;
    private PrintStream err;
    private ExecutionContext context;

    @BeforeEach
    void setup() throws Exception
    {
        out = mock( PrintStream.class );
        err = mock( PrintStream.class );
        confDir = testDirectory.directory( "test.conf" );
        context = new ExecutionContext( testDirectory.homePath(), confDir, out, err, testDirectory.getFileSystem() );
        Path configFile = confDir.resolve( "neo4j.conf" );
        Files.createFile( configFile, PosixFilePermissions.asFileAttribute( Set.of( OWNER_READ, OWNER_WRITE ) ) );
        GraphDatabaseSettings.strict_config_validation.name();
        Files.write( configFile, (BootloaderSettings.initial_heap_size.name() + "=$(expr 500)").getBytes() );
    }

    @Test
    void shouldExpandCommands() throws Exception
    {
        assertSuccess( new SetInitialPasswordCommand( context ), "--expand-commands", "pass" );
        assertSuccess( new SetDefaultAdminCommand( context ), "--expand-commands", "admin" );
        assertSuccess( new StoreInfoCommand( context ), "--expand-commands", "path" );
        assertSuccess( new CheckConsistencyCommand( context ), "--expand-commands", "--database", "neo4j" );
        assertSuccess( new DiagnosticsReportCommand( context ), "--expand-commands" );
        assertSuccess( new LoadCommand( context, new Loader() ), "--expand-commands", "--from", "test" );
        assertSuccess( new MemoryRecommendationsCommand( context ), "--expand-commands" );
        assertSuccess( new ImportCommand( context ), "--expand-commands", "--nodes=" + testDirectory.createFile( "foo.csv" ).toAbsolutePath() );
        assertSuccess( new DumpCommand( context, new Dumper( context.err() ) ), "--expand-commands", "--to", "test" );
    }

    @Test
    void shouldNotExpandCommands()
    {
        assertExpansionError( new SetInitialPasswordCommand( context ), "pass" );
        assertExpansionError( new SetDefaultAdminCommand( context ), "user" );
        assertExpansionError( new StoreInfoCommand( context ), "path" );
        assertExpansionError( new CheckConsistencyCommand( context ), "--database", "neo4j" );
        assertExpansionError( new DiagnosticsReportCommand( context ) );
        assertExpansionError( new LoadCommand( context, new Loader() ), "--from", "test" );
        assertExpansionError( new MemoryRecommendationsCommand( context ) );
        assertExpansionError( new ImportCommand( context ), "--nodes=" + testDirectory.createFile( "foo.csv" ).toAbsolutePath() );
        assertExpansionError( new DumpCommand( context, new Dumper( context.err() ) ), "--to", "test" );
    }

    private static void assertSuccess( AbstractCommand command, String... args ) throws Exception
    {
        CommandLine.populateCommand( command, args ).call();
    }

    private static void assertExpansionError( AbstractCommand command, String... args )
    {
        var exception = new MutableObject<Exception>();
        new CommandLine( command ).setExecutionExceptionHandler( ( ex, commandLine, parseResult ) ->
                                                                 {
                                                                     exception.setValue( ex );
                                                                     return 1;
                                                                 } ).execute( args );
        assertThat( exception.getValue() ).hasMessageContaining( "is a command, but config is not explicitly told to expand it." );
    }
}
