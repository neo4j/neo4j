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
package org.neo4j.server.startup;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.DisabledOnOs;
import org.junit.jupiter.api.condition.OS;
import picocli.CommandLine;
import picocli.CommandLine.ExitCode;

import java.io.PrintStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.function.Function;

import org.neo4j.annotations.service.ServiceProvider;
import org.neo4j.cli.AbstractCommand;
import org.neo4j.cli.AdminTool;
import org.neo4j.cli.Command;
import org.neo4j.cli.CommandProvider;
import org.neo4j.cli.ExecutionContext;
import org.neo4j.configuration.BootloaderSettings;
import org.neo4j.configuration.GraphDatabaseSettings;

import static org.apache.commons.lang3.SystemUtils.IS_OS_WINDOWS;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assumptions.assumeThat;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.spy;
import static org.neo4j.server.startup.Bootloader.EXIT_CODE_OK;
import static org.neo4j.server.startup.Bootloader.PROP_VM_NAME;
import static org.neo4j.server.startup.Bootloader.PROP_VM_VENDOR;
import static org.neo4j.server.startup.Neo4jCommandTestBase.isCurrentlyRunningAsWindowsAdmin;

/**
 * This test class just verifies that the Neo4jAdminCommand is correctly invoking AdminTool
 */
class Neo4jAdminCommandTest
{
    /**
     * These tests are starting the real AdminTool, so consider them to be integration tests
     */
    @Nested
    class UsingRealProcess extends BootloaderCommandTestBase
    {
        private TestInFork fork;

        @Override
        @BeforeEach
        void setUp() throws Exception
        {
            super.setUp();
            fork = new TestInFork( out, err );
            addConf( GraphDatabaseSettings.default_database, GraphDatabaseSettings.DEFAULT_DATABASE_NAME ); //just make sure the file exists
        }

        @Test
        void shouldExecuteCommand() throws Exception
        {
            if ( fork.run( () -> execute( "test-command" ) ) )
            {
                assertThat( out.toString() ).contains( TestCommand.MSG );
            }
        }

        @Test
        void shouldNotPrintUnexpectedErrorStackTraceOnCommandNonZeroExit() throws Exception
        {
            if ( !fork.run( () -> assertThat( execute( "load" ) ).isEqualTo( ExitCode.USAGE ) ) )
            {
                assertThat( err.toString() ).isEmpty();
            }
        }

        @Test
        void shouldWarnWhenHomeIsInvalid() throws Exception
        {
            if ( fork.run( () -> execute( "test-command" ), Map.of( Bootloader.ENV_NEO4J_HOME, "foo" ) ) )
            {
                assertThat( err.toString() ).contains( "NEO4J_HOME path doesn't exist" );
            }
        }

        @Test
        void shouldNotExpandAtFilesInBootloader() throws Exception
        {
            Path commandFile = home.resolve( "fileWithArgs" );
            Files.write( commandFile, "foo bar baz".getBytes() );
            if ( fork.run( () -> execute( "test-command", "@" + commandFile, "--verbose" ) ) )
            {
                //In the command we expect the @file to be expanded
                assertThat( out.toString() ).containsSubsequence( "Test command executed", "foo", "bar", "baz" );
            }
            else
            {
                //But the command we execute should just forward the argument
                assertThat( out.toString() )
                        .containsSubsequence( "Executing command line:", "AdminTool test-command @" + commandFile + " --verbose" )
                        .doesNotContain( "foo", "bar", "baz" );
            }
        }

        @Test
        @DisabledOnOs( OS.WINDOWS )
        void shouldPassThroughAndAcceptVerboseAndExpandCommands() throws Exception
        {
            // The command will fail if the databases directory doesn't exist.
            // Exception would be thrown if expand commands didn't work.
            Path customDbDir = home.resolve("customDbDir");
            Path databasesDir = customDbDir.resolve("databases");
            Files.createDirectories(databasesDir.resolve("customDbName"));

            addConf(GraphDatabaseSettings.data_directory, "$(echo " + customDbDir.toAbsolutePath() + ")");
            if ( fork.run( () -> assertThat( execute( "report", "--to", home.toString(), "--verbose", "--expand-commands" ) ).isEqualTo( EXIT_CODE_OK ) ) )
            {
                assertThat(out.toString()).containsSubsequence("Writing report to", "customDbName", "100%");
                assertThat( err.toString() ).isEmpty();
            }
        }

        @Test
        void shouldUseEnvironmentJavaOptionsWhenGiven() throws Exception
        {
            if ( fork.run( () -> execute( "test-command" ), Map.of( Bootloader.ENV_JAVA_OPTS, "-XX:+UseG1GC -Xlog:gc" ) ) )
            {
                // The JVM needs to accept '-Xlog:gc' in order to print which GC it is using
                // and it needs to accept '-XX:+UseG1GC' in order to print 'Using G1',
                // so testing presence of 'Using G1' really verifies that both options are in use.
                assertThat( out.toString() ).containsSubsequence( "Using G1" );
            }
        }

        @Override
        protected CommandLine createCommand( PrintStream out, PrintStream err, Function<String,String> envLookup, Function<String,String> propLookup,
                Runtime.Version version )
        {
            return Neo4jAdminCommand.asCommandLine( new Neo4jAdminCommand.Neo4jAdminBootloaderContext( out, err, envLookup, propLookup, version ) );
        }
    }

    @Nested
    class UsingFakeProcess extends BootloaderCommandTestBase
    {
        @Test
        void shouldPrintUsageWhenNoArgument()
        {
            assertThat( execute() ).isEqualTo( ExitCode.USAGE );
            assertThat( out.toString() ).containsSubsequence( "Usage: neo4j-admin",  "--verbose", "--expand-commands" , "Commands:" );
        }

        @Test
        void shouldPassThroughAllCommandsAndWarnOnUnknownCommand()
        {
            assertThat( execute( "foo", "bar", "baz" ) ).isEqualTo( ExitCode.USAGE );
            assertThat( err.toString() ).contains( "Unmatched argument", "'foo'", "'bar'", "'baz'" );
        }

        @Test
        void shouldNotFailToPrintHelpWithConfigIssues()
        {
            addConf( BootloaderSettings.max_heap_size, "$(echo foo)" );
            assertThat( execute() ).isEqualTo( ExitCode.USAGE );
            assertThat( out.toString() ).contains( "Usage: neo4j-admin", "Commands:" );
        }

        @Test
        void shouldPassParallelGcByDefault()
        {
            assertThat( execute( "test-command" ) ).isEqualTo( EXIT_CODE_OK );
            assertThat( out.toString() ).contains( "-XX:+UseParallelGC" );
        }

        @Test
        void shouldSpecifyHeapSizeWhenGiven()
        {
            assertThat( execute( List.of( "test-command" ), Map.of( Bootloader.ENV_HEAP_SIZE, "666m") ) ).isEqualTo( EXIT_CODE_OK );
            assertThat( out.toString() )
                    .contains( "-Xmx666m" )
                    .contains( "-Xms666m" );
        }

        @Test
        void shouldReadMaxHeapSizeFromConfig()
        {
            addConf( BootloaderSettings.max_heap_size, "222m" );
            assertThat( execute( "test-command" ) ).isEqualTo( EXIT_CODE_OK );
            assertThat( out.toString() ).contains( "-Xmx227328k" );
        }

        @Test
        void shouldPrioritizeHeapSizeWhenConfigProvidedGiven()
        {
            addConf( BootloaderSettings.max_heap_size, "222m" );
            assertThat( execute( List.of( "test-command" ), Map.of( Bootloader.ENV_HEAP_SIZE, "666m") ) ).isEqualTo( EXIT_CODE_OK );
            assertThat( out.toString() )
                    .contains( "-Xmx666m" )
                    .contains( "-Xms666m" );
        }

        @Test
        void shouldIgnoreMinHeapSizeInConfig()
        {
            addConf( BootloaderSettings.initial_heap_size, "222m" );
            assertThat( execute( "test-command" ) ).isEqualTo( EXIT_CODE_OK );
            assertThat( out.toString() ).doesNotContain( "-Xms" );
        }

        @Test
        void shouldUseEnvironmentJavaOptionsWhenGiven()
        {
            assertThat( execute( List.of( "test-command" ), Map.of( Bootloader.ENV_JAVA_OPTS, "-XX:+UseZGC -Xlog:gc" ) ) ).isEqualTo( EXIT_CODE_OK );
            assertThat( out.toString() )
                    .contains( "-XX:+UseZGC" )
                    .contains( "-Xlog:gc" )
                    // parallel GC is used by default by admin commands,
                    // this JVM option should be overridden by the passed JAVA_OPTS
                    .doesNotContain( "-XX:+UseParallelGC" );
        }

        @Test
        void shouldIgnoreJvmOptionsFromConfigWhenJavaOptionsVariablePresent()
        {
            addConf( BootloaderSettings.additional_jvm, "-agentlib:jdwp=transport=dt_socket,server=y,suspend=n,address=*:5005" );
            assertThat( execute( List.of( "test-command" ), Map.of( Bootloader.ENV_JAVA_OPTS, "-XX:+UseZGC -Xlog:gc" ) ) ).isEqualTo( EXIT_CODE_OK );
            assertThat( out.toString() )
                    .contains( "-XX:+UseZGC" )
                    .contains( "-Xlog:gc" )
                    // parallel GC is used by default by admin commands,
                    // this JVM option should be overridden by the passed JAVA_OPTS
                    .doesNotContain( "-XX:+UseParallelGC" )
                    .doesNotContain( "-agentlib:jdwp=transport=dt_socket,server=y,suspend=n,address=*:5005" );
        }

        @Test
        void shouldIgnoreHeapSizeWhenJavaOptionsVariablePresent()
        {
            assertThat( execute( List.of( "test-command" ),
                    Map.of( Bootloader.ENV_JAVA_OPTS, "-XX:+UseZGC -Xlog:gc", Bootloader.ENV_HEAP_SIZE, "666m" ) ) )
                    .isEqualTo( EXIT_CODE_OK );
            assertThat( out.toString() )
                    .contains( "-XX:+UseZGC" )
                    .contains( "-Xlog:gc" )
                    .doesNotContain( "-Xmx666m" )
                    .doesNotContain( "-Xms666m" );
            assertThat( err.toString() )
                    .contains( "WARNING! HEAP_SIZE is ignored, because JAVA_OPTS is set" );
        }

        @Test
        void shouldPrintJVMInfo()
        {
            Runtime.Version version = Runtime.Version.parse( "11.0.11+9-LTS" );
            Map<String,String> vm = Map.of( PROP_VM_NAME, "Java HotSpot(TM) 64-Bit Server VM", PROP_VM_VENDOR, "Oracle" );
            assertThat( execute( List.of( "test-command"), vm, version ) ).isEqualTo( EXIT_CODE_OK );
            assertThat( err.toString() ).containsSubsequence( String.format( "Selecting JVM - Version:%s, Name:%s, Vendor:%s%n",
                    version, vm.get( PROP_VM_NAME ), vm.get( PROP_VM_VENDOR ) ) );
        }

        @Test
        void shouldHandleExpandCommandsAndPassItThrough()
        {
            if ( IS_OS_WINDOWS )
            {
                // This cannot run on Windows if the user is running as elevated to admin rights since this creates a scenario
                // where it's essentially impossible to create correct ACL/owner of the config file that passes the validation in the config reading.
                assumeThat( isCurrentlyRunningAsWindowsAdmin() ).isFalse();
            }
            String cmd = String.format( "$(%secho foo)", IS_OS_WINDOWS ? "cmd.exe /c " : "" );
            addConf( GraphDatabaseSettings.default_database, cmd );
            assertThat( execute( "test-command", "--expand-commands" ) ).isEqualTo( EXIT_CODE_OK );
            assertThat( out.toString() ).containsSubsequence( "test-command", "--expand-commands" );
        }

        @Test
        void shouldPassThroughVerbose()
        {
            assertThat( execute( "test-command", "--verbose" ) ).isEqualTo( EXIT_CODE_OK );
            assertThat( out.toString() ).containsSubsequence( "test-command", "--verbose" );
        }

        @Test
        void shouldFailOnMissingExpandCommands()
        {
            addConf( BootloaderSettings.max_heap_size, "$(echo foo)" );
            assertThat( execute( "test-command" ) ).isEqualTo( ExitCode.SOFTWARE );
            assertThat( err.toString() ).containsSubsequence( "Failed to read config",
                    "is a command, but config is not explicitly told to expand it. (Missing --expand-commands argument?)",
                    "Run with '--verbose' for a more detailed error message" );

            clearOutAndErr();
            assertThat( execute( "--verbose", "test-command" ) ).isEqualTo( ExitCode.SOFTWARE );
            assertThat( err.toString() ).containsSubsequence( "Failed to read config", "is a command, but config is not explicitly told to expand it" )
                    .doesNotContain( "Run with '--verbose' for a more detailed error message" );
        }

        @Override
        protected CommandLine createCommand( PrintStream out, PrintStream err, Function<String,String> envLookup, Function<String,String> propLookup,
                Runtime.Version version )
        {
            Neo4jAdminCommand.Neo4jAdminBootloaderContext ctx =
                    spy( new Neo4jAdminCommand.Neo4jAdminBootloaderContext( out, err, envLookup, propLookup, version ) );
            ProcessManager pm = new FakeProcessManager( config, ctx, new ProcessHandler(), AdminTool.class );
            doAnswer( inv -> pm ).when( ctx ).processManager();
            return Neo4jAdminCommand.asCommandLine( ctx );
        }
    }

    @CommandLine.Command( name = "test-command", description = "Command for testing purposes only" )
    static class TestCommand extends AbstractCommand
    {
        static final String MSG = "Test command executed";

        @CommandLine.Parameters( hidden = true )
        private List<String> allParameters = List.of();

        TestCommand( ExecutionContext ctx )
        {
            super( ctx );
        }

        @Override
        protected void execute()
        {
            ctx.out().println( MSG );
            for ( String param : allParameters )
            {
                ctx.out().println( param );
            }
        }
    }

    @ServiceProvider
    public static class TestCommandProvider implements CommandProvider<TestCommand>
    {
        @Override
        public TestCommand createCommand( ExecutionContext ctx )
        {
            return new TestCommand( ctx );
        }

        @Override
        public Command.CommandType commandType()
        {
            return Command.CommandType.TEST;
        }
    }
}
