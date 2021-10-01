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
import picocli.CommandLine;

import java.io.PrintStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.function.Function;

import org.neo4j.cli.AdminTool;
import org.neo4j.configuration.BootloaderSettings;
import org.neo4j.configuration.GraphDatabaseSettings;

import static org.apache.commons.lang3.SystemUtils.IS_OS_WINDOWS;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assumptions.assumeThat;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.spy;
import static org.neo4j.server.startup.Bootloader.PROP_JAVA_VERSION;
import static org.neo4j.server.startup.Bootloader.PROP_VM_NAME;
import static org.neo4j.server.startup.Bootloader.PROP_VM_VENDOR;
import static org.neo4j.server.startup.Neo4jCommandTestBase.isCurrentlyRunningAsWindowsAdmin;

/**
 * This test class just verifies that the Neo4jAdminCommand is correctly invoking AdminTool
 */
class Neo4jAdminCommandTest
{
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
        void shouldPrintUsageWhenNoArgument() throws Exception
        {
            if ( fork.run( () -> execute( null ) ) )
            {
                assertThat( out.toString() ).contains( "Usage: neo4j-admin", "Commands:" );
            }
        }

        @Test
        void shouldNotPrintUnexpectedErrorStackTraceOnCommandNonZeroExit() throws Exception
        {
            if ( !fork.run( () -> assertThat( execute( "load" ) ).isEqualTo( 2 ) ) )
            {
                assertThat( err.toString() ).isEmpty();
            }
        }

        @Test
        void shouldPassThroughAllCommandsAndWarnOnUnknownCommand() throws Exception
        {
            if ( fork.run( () -> execute( List.of( "foo", "bar", "baz" ), Map.of() ) ) )
            {
                assertThat( err.toString() ).contains( "Unmatched argument", "'foo'", "'bar'", "'baz'" );
            }
        }

        @Test
        void shouldWarnWhenHomeIsInvalid() throws Exception
        {
            if ( fork.run( () -> execute( "asd" ), Map.of( Bootloader.ENV_NEO4J_HOME, "foo" ) ) )
            {
                assertThat( err.toString() ).contains( "NEO4J_HOME path doesn't exist" );
            }
        }

        @Test
        void shouldExpandAtFilesWhenItReachesAdminCommand() throws Exception
        {
            Path commandFile = home.resolve( "fileWithArgs" );
            Files.write( commandFile, "foo bar baz".getBytes() );
            if ( fork.run( () -> execute( "@" + commandFile ) ) )
            {
                //In the command we expect the @file to be expanded
                assertThat( err.toString() ).contains( "Unmatched argument", "'foo'", "'bar'", "'baz'" );
            }
            //else The bootloader should just forward the argument (tested below with FakeProcess)
        }

        @Override
        protected CommandLine createCommand( PrintStream out, PrintStream err, Function<String,String> envLookup, Function<String,String> propLookup )
        {
            return Neo4jAdminCommand.asCommandLine( new Neo4jAdminCommand.Neo4jAdminBootloaderContext( out, err, envLookup, propLookup ) );
        }
    }

    @Nested
    class UsingFakeProcess extends BootloaderCommandTestBase
    {

        @Test
        void shouldNotExpandAtFilesInBootloader() throws Exception
        {
            Path commandFile = home.resolve( "fileWithArgs" );
            Files.write( commandFile, "foo bar baz".getBytes() );
            execute( "@" + commandFile );
            assertThat( out.toString() ).contains( "@" + commandFile ).doesNotContain( "foo", "bar", "baz" );
        }

        @Test
        void shouldPassParallelGcByDefault()
        {
            assertThat( execute( null ) ).isEqualTo( 0 );
            assertThat( out.toString() ).contains( "-XX:+UseParallelGC" );
        }

        @Test
        void shouldSpecifyHeapSizeWhenGiven()
        {
            assertThat( execute( List.of(), Map.of( Bootloader.ENV_HEAP_SIZE, "666m") ) ).isEqualTo( 0 );
            assertThat( out.toString() )
                    .contains( "-Xmx666m" )
                    .contains( "-Xms666m" );
        }

        @Test
        void shouldReadMaxHeapSizeFromConfig()
        {
            addConf( BootloaderSettings.max_heap_size, "222m" );
            assertThat( execute( null ) ).isEqualTo( 0 );
            assertThat( out.toString() ).contains( "-Xmx227328k" );
        }

        @Test
        void shouldPrioritizeHeapSizeWhenConfigProvidedGiven()
        {
            addConf( BootloaderSettings.max_heap_size, "222m" );
            assertThat( execute( List.of(), Map.of( Bootloader.ENV_HEAP_SIZE, "666m") ) ).isEqualTo( 0 );
            assertThat( out.toString() )
                    .contains( "-Xmx666m" )
                    .contains( "-Xms666m" );
        }

        @Test
        void shouldIgnoreMinHeapSizeInConfig()
        {
            addConf( BootloaderSettings.initial_heap_size, "222m" );
            assertThat( execute( null ) ).isEqualTo( 0 );
            assertThat( out.toString() ).doesNotContain( "-Xms" );
        }

        @Test
        void shouldPrintJVMInfo()
        {
            Map<String,String> vm = Map.of( PROP_JAVA_VERSION, "11.0", PROP_VM_NAME, "Java HotSpot(TM) 64-Bit Server VM", PROP_VM_VENDOR, "Oracle" );
            assertThat( execute( List.of(), vm ) ).isEqualTo( 0 );
            assertThat( out.toString() ).containsSubsequence( String.format( "Selecting JVM - Version:%s, Name:%s, Vendor:%s%n",
                    vm.get( PROP_JAVA_VERSION ), vm.get( PROP_VM_NAME ), vm.get( PROP_VM_VENDOR ) ) );
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
            addConf( GraphDatabaseSettings.default_database, "$(echo foo)" );
            assertThat( execute( List.of( "foo", "-b", "--expand-commands" ), Map.of() ) ).isEqualTo( 0 );
            assertThat( out.toString() ).containsSubsequence( "foo", "-b", "--expand-commands" );
        }

        @Test
        void shouldFailOnMissingExpandCommands()
        {
            addConf( GraphDatabaseSettings.default_database, "$(echo foo)" );
            assertThat( execute( "bar" ) ).isEqualTo( 1 );
            assertThat( err.toString() ).containsSubsequence( "Failed to read config", "is a command, but config is not explicitly told to expand it" );
        }

        @Override
        protected CommandLine createCommand( PrintStream out, PrintStream err, Function<String,String> envLookup, Function<String,String> propLookup )
        {
            Neo4jAdminCommand.Neo4jAdminBootloaderContext ctx = spy( new Neo4jAdminCommand.Neo4jAdminBootloaderContext( out, err, envLookup, propLookup ) );
            ProcessManager pm = new FakeProcessManager( config, ctx,  new ProcessHandler(), AdminTool.class );
            doAnswer( inv -> pm ).when( ctx ).processManager();
            return Neo4jAdminCommand.asCommandLine( ctx );
        }
    }
}
