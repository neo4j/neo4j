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

import org.apache.commons.lang3.ArrayUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.SystemUtils;
import org.eclipse.collections.api.factory.Lists;
import org.eclipse.collections.impl.factory.Sets;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.DisabledForJreRange;
import org.junit.jupiter.api.condition.DisabledOnOs;
import org.junit.jupiter.api.condition.EnabledOnOs;
import org.junit.jupiter.api.condition.JRE;
import org.junit.jupiter.api.condition.OS;
import picocli.CommandLine;
import picocli.CommandLine.ExitCode;
import sun.misc.Signal;

import java.io.File;
import java.io.IOException;
import java.io.PrintStream;
import java.lang.management.ManagementFactory;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.attribute.PosixFilePermission;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;

import org.neo4j.configuration.BootloaderSettings;
import org.neo4j.configuration.GraphDatabaseSettings;
import org.neo4j.configuration.connectors.HttpConnector;
import org.neo4j.io.fs.FileUtils;
import org.neo4j.kernel.internal.Version;
import org.neo4j.server.NeoBootstrapper;
import org.neo4j.test.OtherThreadExecutor;
import org.neo4j.test.conditions.Conditions;
import org.neo4j.test.extension.DisabledForRoot;
import org.neo4j.time.Stopwatch;

import static java.util.concurrent.TimeUnit.MINUTES;
import static org.apache.commons.lang3.SystemUtils.IS_OS_WINDOWS;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assumptions.assumeThat;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.spy;
import static org.neo4j.configuration.SettingImpl.newBuilder;
import static org.neo4j.configuration.SettingValueParsers.BOOL;
import static org.neo4j.server.startup.Bootloader.EXIT_CODE_NOT_RUNNING;
import static org.neo4j.server.startup.Bootloader.EXIT_CODE_OK;
import static org.neo4j.server.startup.Bootloader.EXIT_CODE_RUNNING;
import static org.neo4j.test.assertion.Assert.assertEventually;

class Neo4jCommandTest
{
    @Nested
    class UsingFakeProcess extends Neo4jCommandTestBase
    {
        private final ProcessHandler handler = new ProcessHandler();

        @Test
        void shouldPrintUsageWhenNoArgument()
        {
            assertThat( execute() ).isEqualTo( ExitCode.USAGE );
            assertThat( err.toString() ).contains( "Usage: Neo4j" );
        }

        @Test
        void shouldPrintPlatformSpecificUsage()
        {
            assertThat( execute( "help" ) ).isEqualTo( EXIT_CODE_OK );
            String output = out.toString();
            String[] availableCommands =  new String[] {"start", "restart", "console", "status", "stop"};
            if ( SystemUtils.IS_OS_WINDOWS )
            {
                availableCommands = ArrayUtils.addAll( availableCommands, "install-service", "uninstall-service", "update-service" );
            }
            availableCommands = ArrayUtils.addAll( availableCommands, "version", "help" );

            assertThat( output ).contains( availableCommands );
        }

        @Test
        void shouldPrintUsageWhenInvalidArgument()
        {
            assertThat( execute( "foo" ) ).isEqualTo( ExitCode.USAGE );
            assertThat( err.toString() ).contains( "Usage: Neo4j" );
        }

        @Test
        void shouldPrintUsageOnHelp()
        {
            assertThat( execute( "help" ) ).isEqualTo( EXIT_CODE_OK );
            assertThat( out.toString() ).contains( "Usage: Neo4j" );
        }

        @Test
        void shouldNotBeAbleToStartWhenAlreadyRunning()
        {
            execute( "start" );
            clearOutAndErr();
            executeWithoutInjection( "start" );
            assertThat( out.toString() ).contains( "Neo4j is already running" );
        }

        @Test
        void shouldDetectNeo4jNotRunningOnStatus()
        {
            assertThat( execute( "status" ) ).isEqualTo( EXIT_CODE_NOT_RUNNING );
            assertThat( out.toString() ).contains( "Neo4j is not running" );
        }

        @Test
        void shouldDetectNeo4jRunningOnStatus()
        {
            execute( "start" );
            clearOutAndErr();
            assertThat( execute( "status" ) ).isEqualTo( EXIT_CODE_OK );
            assertThat( out.toString() ).contains( "Neo4j is running" );
        }

        @Test
        void shouldDoNothingWhenStoppingNonRunningNeo4j()
        {
            assertThat( execute( "stop" ) ).isEqualTo( EXIT_CODE_OK );
            assertThat( out.toString() ).contains( "Neo4j is not running" );
        }

        @Test
        void shouldBeAbleToStopStartedNeo4j()
        {
            execute( "start" );
            assertThat( execute( "stop" ) ).isEqualTo( EXIT_CODE_OK );
            assertThat( out.toString() ).contains( "Stopping Neo4j", "stopped" );
        }

        @Test
        void shouldBeAbleToRestartNeo4j()
        {
            execute( "start" );
            Optional<ProcessHandle> firstProcess = getProcess();

            assertThat( firstProcess ).isPresent();

            execute( "restart" );
            Optional<ProcessHandle> secondProcess = getProcess();

            assertThat( secondProcess ).isPresent();
            assertThat( firstProcess.get().pid() ).isNotEqualTo( secondProcess.get().pid() );
            assertThat( out.toString() ).containsSubsequence( "Starting Neo4j.",  "Stopping Neo4j", "stopped" , "Starting Neo4j." );
        }

        @Test
        void shouldBeAbleToProvideHeapSettings()
        {
            addConf( BootloaderSettings.max_heap_size, "100m" );
            addConf( BootloaderSettings.initial_heap_size, "10m" );
            assertThat( execute( "start" ) ).isEqualTo( EXIT_CODE_OK );
            assertThat( out.toString() )
                    .contains( "-Xmx102400k" )
                    .contains( "-Xms10240k" );
        }

        @Test
        void shouldSeeErrorMessageOnInvalidHeap()
        {
            addConf( BootloaderSettings.max_heap_size, "foo" );
            assertThat( execute( "start" ) ).isEqualTo( ExitCode.SOFTWARE );
            assertThat( err.toString() ).contains( "'foo' is not a valid size" );
        }

        @Test
        void shouldOnlyPrintStacktraceOnVerbose()
        {
            addConf( HttpConnector.enabled, "foo" );
            assertThat( execute( "start" ) ).isEqualTo( ExitCode.SOFTWARE );
            assertThat( err.toString() ).contains( "Run with '--verbose' for a more detailed error message." );
            assertThat( err.toString() ).doesNotContain( "Exception" );

            clearOutAndErr();
            assertThat( execute( "start", "--verbose" ) ).isEqualTo( ExitCode.SOFTWARE );
            assertThat( err.toString() ).doesNotContain( "Run with '--verbose' for a more detailed error message." );
            assertThat( err.toString() ).contains( "BootFailureException" );
        }

        @Test
        @DisabledForJreRange( min = JRE.JAVA_17 )
        void shouldNotValidateSettingsNotUsedByBootloaderOnStopAndStatus()
        {
            assertThat( execute( "start" ) ).isEqualTo( EXIT_CODE_OK );
            assertThat( err.toString() ).isEmpty();

            addConf( GraphDatabaseSettings.allow_upgrade, "foo" ); //This setting is not used by the bootloader, so ignored.
            assertThat( execute( "status" ) ).isEqualTo( EXIT_CODE_OK );
            assertThat( execute( "stop" ) ).isEqualTo( EXIT_CODE_OK );
            assertThat( err.toString() ).isEmpty();
        }

        @Test
        @DisabledForJreRange( min = JRE.JAVA_17 )
        void shouldValidateSettingsUsedByBootloaderOnStopAndStatus()
        {
            assertThat( execute( "start" ) ).isEqualTo( EXIT_CODE_OK );
            assertThat( err.toString() ).isEmpty();

            addConf( GraphDatabaseSettings.strict_config_validation, "foo" );
            assertThat( execute( "status" ) ).isEqualTo( ExitCode.SOFTWARE );
            assertThat( err.toString() ).contains( "'foo' is not a valid boolean value" );
            clearOutAndErr();
            assertThat( execute( "stop" ) ).isEqualTo( ExitCode.SOFTWARE );
            assertThat( err.toString() ).contains( "'foo' is not a valid boolean value" );
        }

        @Test
        void shouldValidateSettingsOnStartAndConsole()
        {
            addConf( HttpConnector.enabled, "foo" );
            assertThat( execute( "start" ) ).isEqualTo( ExitCode.SOFTWARE );
            assertThat( err.toString() ).contains( "'foo' is not a valid boolean value" );
            clearOutAndErr();
            assertThat( execute( "console" ) ).isEqualTo( ExitCode.SOFTWARE );
            assertThat( err.toString() ).contains( "'foo' is not a valid boolean value" );
        }

        @Test
        void shouldBeAbleToPassCommandExpansion()
        {
            if ( IS_OS_WINDOWS )
            {
                // This cannot run on Windows if the user is running as elevated to admin rights since this creates a scenario
                // where it's essentially impossible to create correct ACL/owner of the config file that passes the validation in the config reading.
                assumeThat( isCurrentlyRunningAsWindowsAdmin() ).isFalse();
            }
            assertThat( execute( "start", "--expand-commands" ) ).isEqualTo( EXIT_CODE_OK );
            assertThat( out.toString() ).contains( "--expand-commands" );
            assertThat( execute( "stop", "--expand-commands" ) ).isEqualTo( EXIT_CODE_OK );
        }

        @Test
        void shouldNotComplainOnJavaWhenCorrectVersion()
        {
            Map<String,String> java = Map.of( Bootloader.PROP_VM_NAME, "Java HotSpot(TM) 64-Bit Server VM" );
            assertThat( execute( List.of( "start" ), java ) ).isEqualTo( EXIT_CODE_OK );
            assertThat( err.toString() ).doesNotContain( "WARNING! You are using an unsupported Java runtime" );
        }

        @Test
        void shouldComplainWhenJavaVersionIsTooNew()
        {
            Runtime.Version version = Runtime.Version.parse( "15.0.1+2" );
            Map<String,String> java = Map.of( Bootloader.PROP_VM_NAME, "Java HotSpot(TM) 64-Bit Server VM" );
            assertThat( execute( List.of( "start" ), java, version ) ).isEqualTo( EXIT_CODE_OK );
            assertThat( err.toString() ).contains( "WARNING! You are using an unsupported Java runtime." );
        }

        @Test
        void shouldComplainWhenRunningUnsupportedJvm()
        {
            Map<String,String> java = Map.of( Bootloader.PROP_VM_NAME, "Eclipse OpenJ9 VM" );
            assertThat( execute( List.of( "start" ), java ) ).isEqualTo( EXIT_CODE_OK );
            assertThat( err.toString() ).contains( "WARNING! You are using an unsupported Java runtime." );
        }

        @Test
        void shouldBeAbleToPrintCorrectVersion()
        {
            assertThat( execute( "version" ) ).isEqualTo( EXIT_CODE_OK );
            assertThat( out.toString() ).contains( Version.getNeo4jVersion() );

            clearOutAndErr();
            assertThat( execute( "--version" ) ).isEqualTo( EXIT_CODE_OK );
            assertThat( out.toString() ).contains( Version.getNeo4jVersion() );
        }

        @Test
        void shouldBeAbleToPrintCorrectVersionWhenRunning()
        {
            assertThat( execute( "start" ) ).isEqualTo( EXIT_CODE_OK );
            ProcessHandle firstHandle = getProcess().get();
            assertThat( execute( "version" ) ).isEqualTo( EXIT_CODE_OK );
            assertThat( out.toString() ).contains( Version.getNeo4jVersion() );
            ProcessHandle secondHandle = getProcess().get();
            assertThat( firstHandle.pid() ).isEqualTo( secondHandle.pid() );
            assertThat( execute( "stop" ) ).isEqualTo( EXIT_CODE_OK );
            assertFalse( firstHandle.isAlive() );
        }

        @Test
        @EnabledOnOs( OS.LINUX ) //stop involves services on windows
        void shouldBeAbleToStopRunningServerWithConfigErrors()
        {
            assertThat( execute( "start" ) ).isEqualTo( EXIT_CODE_OK );
            addConf( BootloaderSettings.gc_logging_enabled, "yes" );
            assertThat( execute( "stop" ) ).isEqualTo( EXIT_CODE_OK );
            clearOutAndErr();
            assertThat( execute( "start" ) ).isEqualTo( ExitCode.SOFTWARE );
            assertThat( err.toString() ).contains( "'yes' is not a valid boolean" );
        }

        @Test
        void shouldBeAbleToStartInFakeConsoleMode()
        {
            assertThat( execute( "console" ) ).isEqualTo( EXIT_CODE_OK ); //Since we're using a fake process it does not block on anything
            assertThat( out.toString() ).contains( TestEntryPoint.class.getName() );
        }

        @Test
        void shouldUseUtf8ByDefault()
        {
            assertThat( execute( "start" ) ).isEqualTo( EXIT_CODE_OK );
            assertThat( out.toString() ).contains( "-Dfile.encoding=UTF-8" );
        }

        @Test
        void shouldUseIncludeLibAndPluginAndConfInClassPath() throws IOException
        {
            //Empty dirs are ignored, create fake jars
            FileUtils.writeToFile( config.get( BootloaderSettings.lib_directory ).resolve( "fake.jar" ), "foo", true );
            FileUtils.writeToFile( config.get( GraphDatabaseSettings.plugin_dir ).resolve( "fake.jar" ), "foo", true );
            FileUtils.writeToFile( confFile.getParent().resolve( "fake.jar" ), "foo", true );

            assertThat( execute( "start" ) ).isEqualTo( EXIT_CODE_OK );
            assertThat( out.toString() ).containsSubsequence(
                    IS_OS_WINDOWS ? "--Classpath" : "-cp",
                    config.get( GraphDatabaseSettings.plugin_dir ).toString() + File.separator + "*",
                    confFile.getParent() + File.separator + "*",
                    config.get( BootloaderSettings.lib_directory ).toString() + File.separator + "*"
                    );
        }

        @Test
        void shouldPassHomeAndConfArgs()
        {
            assertThat( execute( "start" ) ).isEqualTo( EXIT_CODE_OK );
            assertThat( out.toString() ).contains( "--home-dir", "--config-dir" );
        }

        @Test
        void shouldHideDryRunArgument()
        {
            assertThat( execute( "help", "console" ) ).isEqualTo( EXIT_CODE_OK );
            assertThat( out.toString() ).doesNotContain( "--dry-run" );
        }

        @Test
        void shouldOnlyGetCommandLineFromDryRun()
        {
            assertThat( execute( "console", "--dry-run" ) ).isEqualTo( EXIT_CODE_OK );
            assertThat( out.toString() ).hasLineCount( 1 );
            assertThat( out.toString() ).containsSubsequence( "java", "-cp", TestEntryPoint.class.getName(), "--home-dir", "--config-dir" );
        }

        @Test
        void shouldQuoteArgsCorrectlyOnDryRun()
        {
            addConf( BootloaderSettings.additional_jvm, "\"-Dbaz=/path/with spaces/and double qoutes\"" );
            addConf( BootloaderSettings.additional_jvm, "\"-Dqux=/path/with spaces/and unmatched \"\" qoute\"" );
            addConf( BootloaderSettings.additional_jvm, "-Dcorge=/path/with/no/spaces" );
            addConf( BootloaderSettings.additional_jvm, "-Dgrault=/path/with/part/'quoted'" );
            addConf( BootloaderSettings.additional_jvm, "-Dgarply=\"/path/with/part/quoted\"" );
            assertThat( execute( "console", "--dry-run" ) ).isEqualTo( EXIT_CODE_OK );
            assertThat( out.toString() ).contains(
                    "\"-Dbaz=/path/with spaces/and double qoutes\"",
                    "'-Dqux=/path/with spaces/and unmatched \" qoute'",
                    "-Dcorge=/path/with/no/spaces",
                    "-Dgrault=/path/with/part/'quoted'",
                    "'-Dgarply=\"/path/with/part/quoted\"'"
            );

            assertThat( out.toString() ).doesNotContain( "\"-Dcorge=/path/with/no/spaces\"" );
        }

        @Test
        void shouldComplainOnIncorrectQuotingOnDryRun()
        {
            addConf( BootloaderSettings.additional_jvm, "-Dfoo=some\"partly'quoted'\"data" );
            assertThat( execute( "console", "--dry-run" ) ).isEqualTo( ExitCode.SOFTWARE );
            assertThat( err.toString() ).contains( "contains both single and double quotes" );
        }

        @Test
        void shouldNotComplainOnUnknownSettingWithStrictValidation()
        {
            addConf( newBuilder( "apoc.export.file.enabled", BOOL, false ).build(), "true" );
            addConf( GraphDatabaseSettings.strict_config_validation, "true" );
            assertThat( execute( "start" ) ).isEqualTo( EXIT_CODE_OK );
        }

        @Test
        void consoleShouldDetectAnotherConsoleRunning()
        {
            assertThat( execute( "console" ) ).isEqualTo( EXIT_CODE_OK );
            clearOutAndErr();
            assertThat( execute( "console" ) ).isEqualTo( EXIT_CODE_RUNNING );
            assertThat( out.toString() ).contains( "Neo4j is already running" );
        }

        @Test
        void dryRunShouldPrintWarningAndCommandIfAlreadyRunning()
        {
            assertThat( execute( "start" ) ).isEqualTo( EXIT_CODE_OK );
            clearOutAndErr();
            assertThat( execute( "console", "--dry-run" ) ).isEqualTo( EXIT_CODE_RUNNING );
            assertThat( out.toString() )
                    .containsSubsequence( "Neo4j is already running", "java", "-cp", TestEntryPoint.class.getName() );
        }

        @Nested
        @EnabledOnOs( OS.WINDOWS )
        class OnWindows
        {
            @Test
            void shouldNotStartIfServiceNotInstalled()
            {
                assertThat( executeWithoutInjection( "start" ) ).isEqualTo( EXIT_CODE_NOT_RUNNING );
                assertThat( err.toString() ).contains( "Neo4j service is not installed" );
            }

            @Test
            void shouldNotStopIfServiceNotInstalled()
            {
                assertThat( executeWithoutInjection( "stop" ) ).isEqualTo( EXIT_CODE_OK );
                assertThat( out.toString() ).contains( "Neo4j is not running" );
            }

            @Test
            void shouldComplainIfAlreadyInstalled()
            {
                assertThat( executeWithoutInjection( "install-service" ) ).isEqualTo( EXIT_CODE_OK );
                clearOutAndErr();
                assertThat( executeWithoutInjection( "install-service" ) ).isEqualTo( ExitCode.SOFTWARE );
                assertThat( out.toString() ).contains( "Neo4j service is already installed" );
            }

            @Test
            void shouldComplainIfUninstallingWhenNotInstalled()
            {
                assertThat( executeWithoutInjection( "uninstall-service" ) ).isEqualTo( EXIT_CODE_OK );
                assertThat( out.toString() ).contains( "Neo4j service is not installed" );
            }
        }

        @Override
        protected CommandLine createCommand( PrintStream out, PrintStream err, Function<String,String> envLookup, Function<String,String> propLookup,
                Runtime.Version version )
        {
            Neo4jCommand.Neo4jBootloaderContext ctx = spy( new Neo4jCommand.Neo4jBootloaderContext( out, err, envLookup, propLookup, entrypoint(), version ) );
            ProcessManager pm = new FakeProcessManager( config, ctx, handler, TestEntryPoint.class );
            doAnswer( inv -> pm ).when( ctx ).processManager();
            return Neo4jCommand.asCommandLine( ctx );
        }

        @Override
        protected int execute( List<String> args, Map<String,String> env, Runtime.Version version )
        {
            if ( IS_OS_WINDOWS )
            {
                if ( !args.isEmpty() && args.get( 0 ).equals( "start" ) )
                {
                    List<String> installArgs = new ArrayList<>( args );
                    installArgs.remove( 0 );
                    installArgs.add( 0, "install-service" );
                    int installExitCode = super.execute( installArgs, env, version );
                    if ( installExitCode != EXIT_CODE_OK )
                    {
                        return installExitCode;
                    }
                }
            }
            int exitCode = super.execute( args, env, version );
            if ( IS_OS_WINDOWS )
            {
                if ( !args.isEmpty() && args.get( 0 ).equals( "stop" ) && exitCode == EXIT_CODE_OK )
                {
                    return super.execute( List.of( "uninstall-service" ), env );
                }
            }
            return exitCode;
        }

        int executeWithoutInjection( String arg )
        {
            return super.execute( List.of( arg ), Map.of(), Runtime.version() );
        }

        @Override
        protected Optional<ProcessHandle> getProcess()
        {
            return handler.handle();
        }

        @Override
        protected Class<? extends EntryPoint> entrypoint()
        {
            return TestEntryPoint.class;
        }
    }

    @Nested
    class UsingRealProcess extends Neo4jCommandTestBase
    {
        private TestInFork fork;

        @Override
        @BeforeEach
        void setUp() throws Exception
        {
            super.setUp();
            fork = new TestInFork( out, err );
        }

        @Test
        void shouldBeAbleToStartInRealConsoleMode() throws Exception
        {
            if ( fork.run( () -> assertThat( execute( "console" ) ).isEqualTo( EXIT_CODE_OK ), Map.of( TestEntryPoint.ENV_TIMEOUT, "0" ) ) )
            {

                assertThat( out.toString() ).contains( TestEntryPoint.STARTUP_MSG );
            }
        }

        // Process.destroy()/destroyForcibly() on windows are the same and kills process instantly without invoking exit handler. Can't mimic CTRL+C in test.
        @DisabledOnOs( OS.WINDOWS )
        @Test
        void shouldWaitForNeo4jToDieBeforeExitInConsole() throws Exception
        {
            if ( fork.run( () -> assertThat( execute( "console" ) ).isEqualTo( EXIT_CODE_OK ), Map.of( TestEntryPoint.ENV_TIMEOUT, "1000" ), p ->
            {
                StringBuilder sb = new StringBuilder();
                assertEventually( () -> sb.append( new String( p.getInputStream().readNBytes( 1 ) ) ).toString(), s -> s.contains( TestEntryPoint.STARTUP_MSG ),
                        5, MINUTES );
                p.toHandle().destroy();
                return 0;
            } ) )
            {
                assertThat( out.toString() ).contains( TestEntryPoint.EXIT_MSG );
                assertThat( out.toString() ).doesNotContain( TestEntryPoint.END_MSG );
            }
        }

        @Test
        void shouldSeeErrorMessageOnTooSmallHeap() throws Exception
        {
            if ( fork.run( () ->
            {
                addConf( BootloaderSettings.max_heap_size, "1k" );
                assertThat( execute( "console" ) ).isEqualTo( ExitCode.SOFTWARE );
            } ) )
            {
                assertThat( out.toString() ).contains( "Too small maximum heap" );
            }
        }

        @DisabledOnOs( OS.WINDOWS )
        @Test
        void shouldWritePidFileOnStart()
        {
            assertThat( execute( "start" ) ).isEqualTo( EXIT_CODE_OK );
            assertThat( out.toString() ).contains( "Starting Neo4j." );
            assertThat( pidFile ).exists();
            assertThat( execute( "stop" ) ).isEqualTo( EXIT_CODE_OK );
            assertThat( pidFile ).doesNotExist();
        }

        @DisabledOnOs( OS.WINDOWS )
        @Test
        void shouldWritePidFileOnConsole() throws Exception
        {
            if ( fork.run( () ->
            {
                try ( OtherThreadExecutor executor = new OtherThreadExecutor( "TestExecutor" ) )
                {
                    Future<Integer> console = executor.executeDontWait( () -> execute( "console" ) );
                    assertEventually( () -> Files.exists( pidFile ), Conditions.TRUE, 2, MINUTES );
                    Optional<ProcessHandle> process = getProcess();
                    assertThat( process ).isPresent();

                    process.get().destroy();
                    console.get();
                }
            }, Map.of( TestEntryPoint.ENV_TIMEOUT, "1000" ) ) )
            {
                assertThat( pidFile ).doesNotExist();
            }
        }

        @DisabledOnOs( OS.WINDOWS )
        @Test
        void shouldDetectRunningNeo4jOnConsole() throws Exception
        {
            if ( !fork.run( () ->
            {
                assertThat( execute( "start" ) ).isEqualTo( EXIT_CODE_OK );
                assertThat( execute( "console" ) ).isEqualTo( ExitCode.SOFTWARE );
            } ) )
            {
                assertThat( out.toString() ).contains( "Neo4j is already running" );
            }
        }

        @DisabledOnOs( OS.WINDOWS )
        @DisabledForRoot
        @Test
        void shouldPrintErrorOnFailedPidWriteOnConsole() throws Exception
        {
            if ( !fork.run( () ->
            {
                Path runDir = pidFile.getParent();
                Files.createDirectories( runDir );
                Set<PosixFilePermission> origPermissions = Files.getPosixFilePermissions( runDir );
                try
                {
                    Files.setPosixFilePermissions( runDir, Set.of() );
                    assertThat( execute( "console" ) ).isEqualTo( EXIT_CODE_OK );
                }
                finally
                {
                    Files.setPosixFilePermissions( runDir, origPermissions );
                }
            }, Map.of( TestEntryPoint.ENV_TIMEOUT, "0" ) ) )
            {
                assertThat( err.toString() ).contains( "Failed to write PID file: Access denied" );
            }
        }

        @DisabledOnOs( OS.WINDOWS )
        @DisabledForRoot //Turns out root can always read the file anyway, causing test issues on TC
        @Test
        void shouldGetReasonableErrorWhenUnableToReadPidFile() throws IOException
        {
            assertThat( execute( "start" ) ).isEqualTo( EXIT_CODE_OK );
            assertThat( pidFile ).exists();
            Set<PosixFilePermission> origPermissions = Files.getPosixFilePermissions( pidFile );
            try
            {
                Files.setPosixFilePermissions( pidFile, Sets.mutable.withAll( origPermissions ).without( PosixFilePermission.OWNER_READ ) );
                assertThat( execute( "status" ) ).isEqualTo( ExitCode.SOFTWARE );
                assertThat( err.toString() ).contains( "Access denied" );
            }
            finally
            {
                Files.setPosixFilePermissions( pidFile, origPermissions );
            }
        }

        @Test
        void shouldBeAbleToGetGcLogging() throws Exception
        {
            if ( fork.run( () ->
            {
                Files.createDirectories( config.get( GraphDatabaseSettings.logs_directory ) );
                addConf( BootloaderSettings.gc_logging_enabled, "true" );
                assertThat( execute( "console" ) ).isEqualTo( EXIT_CODE_OK );
            }, Map.of( TestEntryPoint.ENV_TIMEOUT, "0" ) ) )
            {
                assertThat( out.toString() ).containsSubsequence( "-Xlog:gc*,safepoint,age*=trace:file=", "gc.log", "::filecount=5,filesize=20480k" );
            }
        }

        @Override
        protected Class<? extends EntryPoint> entrypoint()
        {
            return TestEntryPoint.class;
        }
    }

    private static class TestEntryPoint implements EntryPoint
    {
        static final String ENV_TIMEOUT = "TestEntryPointTimeout";
        static final String STARTUP_MSG = "TestEntryPoint started";
        static final String EXIT_MSG = "TestEntryPoint exited";
        static final String END_MSG = "TestEntryPoint ended";

        public static void main( String[] args ) throws InterruptedException
        {
            Runtime.getRuntime().addShutdownHook( new Thread( () -> System.out.println( EXIT_MSG ) ) );
            Signal.handle( new Signal( NeoBootstrapper.SIGINT ), s -> System.exit( 0 ) ); //mimic neo4j (NeoBootstrapper.installSignalHandlers)
            Signal.handle( new Signal( NeoBootstrapper.SIGTERM ), s -> System.exit( 0 ) );

            System.out.println( STARTUP_MSG );
            Lists.mutable
                    .with( args )
                    .withAll( ManagementFactory.getRuntimeMXBean().getInputArguments() )
                    .forEach( System.out::println );
            Stopwatch stopwatch = Stopwatch.start();

            int timeoutSeconds = StringUtils.isNotEmpty( System.getenv( ENV_TIMEOUT ) ) ? Integer.parseInt( System.getenv( ENV_TIMEOUT ) ) : 60;
            while ( !stopwatch.hasTimedOut( timeoutSeconds, TimeUnit.SECONDS ) )
            {
                Thread.sleep( 1000 ); //A minute should be enough for all tests and any erroneously leaked processes will eventually die.
            }
            System.out.println( END_MSG );
        }

        @Override
        public Priority getPriority()
        {
            return Priority.HIGH;
        }
    }
}
