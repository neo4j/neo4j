/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [https://neo4j.com]
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
 * along with this program.  If not, see <https://www.gnu.org/licenses/>.
 */
package org.neo4j.server.startup;

import static java.util.Collections.emptyMap;
import static org.apache.commons.lang3.SystemUtils.IS_OS_WINDOWS;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assumptions.assumeThat;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.spy;
import static org.neo4j.cli.AbstractAdminCommand.CRASH_INFO_TIMEOUT;
import static org.neo4j.configuration.GraphDatabaseSettings.logs_directory;
import static org.neo4j.server.startup.Bootloader.EXIT_CODE_OK;
import static org.neo4j.server.startup.ServerCommandIT.isCurrentlyRunningAsWindowsAdmin;

import java.io.PrintStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.DisabledIf;
import org.junit.jupiter.api.condition.DisabledOnOs;
import org.junit.jupiter.api.condition.OS;
import org.neo4j.annotations.service.ServiceProvider;
import org.neo4j.cli.AbstractAdminCommand;
import org.neo4j.cli.AdminTool;
import org.neo4j.cli.CommandFailedException;
import org.neo4j.cli.CommandProvider;
import org.neo4j.cli.CommandType;
import org.neo4j.cli.ExecutionContext;
import org.neo4j.configuration.BootloaderSettings;
import org.neo4j.configuration.GraphDatabaseSettings;
import org.neo4j.internal.helpers.ProcessUtils;
import org.neo4j.io.fs.FileSystemUtils;
import org.neo4j.memory.EmptyMemoryTracker;
import picocli.CommandLine;
import picocli.CommandLine.ExitCode;

/**
 * This test class just verifies that the Neo4jAdminCommand is correctly invoking AdminTool
 */
class Neo4jAdminCommandTest {
    /**
     * These tests are starting the real AdminTool, so consider them to be integration tests
     */
    @Nested
    class UsingRealProcess extends BootloaderCommandTestBase {
        private TestInFork fork;

        @Override
        @BeforeEach
        protected void setUp() throws Exception {
            super.setUp();
            fork = new TestInFork(out, err);
            addConf(
                    GraphDatabaseSettings.initial_default_database,
                    GraphDatabaseSettings.DEFAULT_DATABASE_NAME); // just make sure the file exists
        }

        @Test
        void shouldExecuteCommand() throws Exception {
            if (fork.run(() -> execute("dbms", "test-command"))) {
                assertThat(out.toString()).contains(TestCommand.MSG);
            }
        }

        @Test
        void shouldNotPrintUnexpectedErrorStackTraceOnCommandNonZeroExit() throws Exception {
            if (!fork.run(() -> execute("database", "load"), Map.of(), p -> ExitCode.SOFTWARE)) {
                assertThat(err.toString()).isEmpty();
            }
        }

        @Test
        void shouldWarnWhenHomeIsInvalid() throws Exception {
            if (fork.run(() -> execute("dbms", "test-command"), Map.of(Bootloader.ENV_NEO4J_HOME, "foo"))) {
                assertThat(err.toString()).contains("NEO4J_HOME path doesn't exist");
            }
        }

        @Test
        void shouldNotExpandAtFilesInBootloader() throws Exception {
            Path commandFile = home.resolve("fileWithArgs");
            Files.write(commandFile, "foo bar baz".getBytes());
            if (fork.run(() -> execute("dbms", "test-command", "@" + commandFile, "--verbose"))) {
                // In the command we expect the @file to be expanded
                assertThat(out.toString()).containsSubsequence("Test command executed", "foo", "bar", "baz");
            } else {
                // But the command we execute should just forward the argument
                assertThat(out.toString())
                        .containsSubsequence(
                                "Executing command line:", "AdminTool dbms test-command @" + commandFile + " --verbose")
                        .doesNotContain("foo", "bar", "baz");
            }
        }

        @Test
        @DisabledOnOs(OS.WINDOWS)
        @DisabledIf("isMissingTestRequiredTools")
        void shouldPassThroughAndAcceptVerboseAndExpandCommands() throws Exception {
            // The command will fail if the databases directory doesn't exist.
            // Exception would be thrown if expand commands didn't work.
            Path customDbDir = home.resolve("customDbDir");
            Path databasesDir = customDbDir.resolve("databases");
            Files.createDirectories(databasesDir.resolve("customDbName"));

            addConf(GraphDatabaseSettings.data_directory, "$(echo " + customDbDir.toAbsolutePath() + ")");
            if (fork.run(() -> assertThat(
                            execute("server", "report", "--to-path", home.toString(), "--verbose", "--expand-commands"))
                    .isEqualTo(EXIT_CODE_OK))) {
                assertThat(out.toString()).containsSubsequence("Writing report to", "customDbName", "100%");
                assertThat(err.toString()).contains("WARNING: Using incubator modules: jdk.incubator.vector");
            }
        }

        boolean isMissingTestRequiredTools() {
            try {
                ProcessUtils.executeCommandWithOutput("ps", Duration.ofMinutes(1));
                return false;
            } catch (IllegalArgumentException iae) {
                return true;
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }

        @Test
        void shouldUseEnvironmentJavaOptionsWhenGiven() throws Exception {
            if (fork.run(
                    () -> execute("dbms", "test-command"), Map.of(Bootloader.ENV_JAVA_OPTS, "-XX:+UseG1GC -Xlog:gc"))) {
                // The JVM needs to accept '-Xlog:gc' in order to print which GC it is using
                // and it needs to accept '-XX:+UseG1GC' in order to print 'Using G1',
                // so testing presence of 'Using G1' really verifies that both options are in use.
                assertThat(out.toString()).containsSubsequence("Using G1");
            }
        }

        @Test
        @DisabledOnOs(OS.WINDOWS)
        void shouldWriteExceptionToFile() throws Exception {
            if (fork.run(
                    () -> {
                        execute("dbms", "test-command", "--throw");
                        Path trace = fs.listFiles(
                                        config.get(logs_directory),
                                        p -> p.toString().contains("neo4j-admin-exception-trace"))[0];
                        assertThat(FileSystemUtils.readString(fs, trace, EmptyMemoryTracker.INSTANCE))
                                .contains(CommandFailedException.class.getName());
                    },
                    Map.of(CRASH_INFO_TIMEOUT, "0"))) {
                assertThat(err.toString()).containsSubsequence("Full exception details written to:");
                // No exception in console
                assertThat(err.toString()).doesNotContain(CommandFailedException.class.getName());
                // but we do get the error message
                assertThat(err.toString()).containsSubsequence(TestCommand.THROW_MSG);
            }
        }

        @Test
        @DisabledOnOs(OS.WINDOWS)
        void shouldNotWriteExceptionToFileOnFastFailure() throws Exception {
            if (fork.run(() -> execute("dbms", "test-command", "--throw"), Map.of(CRASH_INFO_TIMEOUT, "60"))) {
                assertThat(err.toString()).contains(TestCommand.THROW_MSG);
                assertThat(err.toString()).doesNotContain("Full exception details written to:");
                assertThat(err.toString()).doesNotContain(CommandFailedException.class.getName());
            }
        }

        @Test
        @DisabledOnOs(OS.WINDOWS)
        void writeExceptionToFileAndConsoleOnFastFailureInVerboseMode() throws Exception {
            if (fork.run(
                    () -> execute("dbms", "test-command", "--throw", "--verbose"), Map.of(CRASH_INFO_TIMEOUT, "60"))) {
                assertThat(err.toString()).containsSubsequence("Full exception details written to:");
                // but we do get the error message
                assertThat(err.toString()).containsSubsequence(TestCommand.THROW_MSG);
            }
        }

        @Test
        @DisabledOnOs(OS.WINDOWS)
        void writeMessageToConsoleOnFastFailure() throws Exception {
            if (fork.run(() -> execute("dbms", "test-command", "--throw"), Map.of(CRASH_INFO_TIMEOUT, "60"))) {
                assertThat(err.toString()).containsSubsequence(TestCommand.THROW_MSG);
            }
        }

        @Test
        @DisabledOnOs(OS.WINDOWS)
        void shouldNotAffectVerboseWhenWritingExceptionToFile() throws Exception {
            if (fork.run(
                    () -> {
                        execute("dbms", "test-command", "--verbose", "--throw");
                        Path trace = fs.listFiles(
                                        config.get(logs_directory),
                                        p -> p.toString().contains("neo4j-admin-exception-trace"))[0];
                        assertThat(FileSystemUtils.readString(fs, trace, EmptyMemoryTracker.INSTANCE))
                                .contains(CommandFailedException.class.getName());
                    },
                    Map.of(CRASH_INFO_TIMEOUT, "0"))) {
                assertThat(err.toString()).containsSubsequence("Full exception details written to:");
                // Get the expected error
                assertThat(err.toString()).containsSubsequence(TestCommand.THROW_MSG);
            }
        }

        @Test
        @DisabledOnOs(OS.WINDOWS)
        void shouldWriteNoTraceFileOnNormalRun() throws Exception {
            if (fork.run(() -> {
                fs.mkdirs(config.get(logs_directory)); // pre-create logs folder so we can enumerate files
                execute("dbms", "test-command", "nothing to see");
                Path[] traces = fs.listFiles(
                        config.get(logs_directory), p -> p.toString().contains("neo4j-admin-exception-trace"));
                assertThat(traces.length).isZero();
            })) {
                assertThat(out.toString()).contains(TestCommand.MSG);
                assertThat(err.toString()).doesNotContain("Full exception details written to:");
                assertThat(err.toString()).doesNotContain(CommandFailedException.class.getName());
                assertThat(err.toString()).doesNotContain(TestCommand.THROW_MSG);
            }
        }

        @Test
        @DisabledOnOs(OS.WINDOWS)
        void shouldNotAffectVerboseEvenIfTraceCannotBeWritten() throws Exception {
            Path badLogsDir = home.resolve("not_a_folder");
            Files.writeString(badLogsDir, "Dummy");
            addConf(logs_directory, badLogsDir.toString());
            if (fork.run(
                    () -> execute("dbms", "test-command", "--verbose", "--throw"), Map.of(CRASH_INFO_TIMEOUT, "0"))) {
                assertThat(out.toString()).doesNotContain("Full exception details written to:");
                assertThat(err.toString()).containsSubsequence(TestCommand.THROW_MSG);
                assertThat(err.toString())
                        .containsSubsequence("Suppressed: java.io.IOException: Unable to write directory path");
            }
        }

        @Test
        @DisabledOnOs(OS.WINDOWS)
        void shouldBeNoConsoleExceptionEvenIfTraceCannotBeWritten() throws Exception {
            Path badLogsDir = home.resolve("not_a_folder");
            Files.writeString(badLogsDir, "Dummy");
            addConf(logs_directory, badLogsDir.toString());
            if (fork.run(() -> execute("dbms", "test-command", "--throw"))) {
                assertThat(out.toString()).doesNotContain("Full exception details written to:");
                // No exception in console
                assertThat(err.toString()).doesNotContain(CommandFailedException.class.getName());
                // but we do get the error message
                assertThat(err.toString()).containsSubsequence(TestCommand.THROW_MSG);
            }
        }

        @Override
        protected CommandLine createCommand(
                PrintStream out,
                PrintStream err,
                Function<String, String> envLookup,
                Function<String, String> propLookup,
                Runtime.Version version) {
            var environment = new Environment(out, err, envLookup, propLookup, version);
            return Neo4jAdminCommand.asCommandLine(new Neo4jAdminCommand(environment), environment);
        }
    }

    @Nested
    class UsingFakeProcess extends BootloaderCommandTestBase {
        @Test
        void shouldPrintUsageWhenNoArgument() {
            assertThat(execute()).isEqualTo(ExitCode.USAGE);
            assertThat(err.toString())
                    .containsSubsequence("Usage: neo4j-admin", "--verbose", "--expand-commands", "Commands:");
        }

        @Test
        void shouldPassThroughAllCommandsAndWarnOnUnknownCommand() {
            assertThat(execute("foo", "bar", "baz")).isEqualTo(ExitCode.USAGE);
            assertThat(err.toString()).contains("Unmatched argument", "'foo'", "'bar'", "'baz'");
        }

        @Test
        void shouldNotFailToPrintHelpWithConfigIssues() {
            addConf(BootloaderSettings.max_heap_size, "$(echo foo)");
            assertThat(execute()).isEqualTo(ExitCode.USAGE);
            assertThat(err.toString()).contains("Usage: neo4j-admin", "Commands:");
        }

        @Test
        void shouldSpecifyHeapSizeWhenGiven() {
            assertThat(execute(List.of("dbms", "test-command"), Map.of(Bootloader.ENV_HEAP_SIZE, "666m")))
                    .isEqualTo(EXIT_CODE_OK);
            assertThat(out.toString()).contains("-Xmx666m").contains("-Xms666m");
        }

        @Test
        void shouldReadMaxHeapSizeFromConfig() {
            addConf(BootloaderSettings.max_heap_size, "222m");
            assertThat(execute("dbms", "test-command")).isEqualTo(EXIT_CODE_OK);
            assertThat(out.toString()).contains("-Xmx227328k");
        }

        @Test
        void shouldPrioritizeHeapSizeWhenConfigProvidedGiven() {
            addConf(BootloaderSettings.max_heap_size, "222m");
            assertThat(execute(List.of("dbms", "test-command"), Map.of(Bootloader.ENV_HEAP_SIZE, "666m")))
                    .isEqualTo(EXIT_CODE_OK);
            assertThat(out.toString()).contains("-Xmx666m").contains("-Xms666m");
        }

        @Test
        void shouldIgnoreMinHeapSizeInConfig() {
            addConf(BootloaderSettings.initial_heap_size, "222m");
            assertThat(execute("dbms", "test-command")).isEqualTo(EXIT_CODE_OK);
            assertThat(out.toString()).doesNotContain("-Xms");
        }

        @Test
        void includeUnsafeOptionOnJDK25ByDefault() {
            Runtime.Version version = Runtime.Version.parse("25.0.1+2");
            assertThat(execute(List.of("dbms", "test-command"), emptyMap(), version))
                    .isEqualTo(EXIT_CODE_OK);
            assertThat(out.toString()).contains("--sun-misc-unsafe-memory-access=allow");
        }

        @Test
        void doNotOverrideUnsafeOptionOnJDK25WhenProvided() {
            Runtime.Version version = Runtime.Version.parse("25.0.1+2");
            addConf(BootloaderSettings.additional_jvm, "--sun-misc-unsafe-memory-access=debug");
            assertThat(execute(List.of("dbms", "test-command"), emptyMap(), version))
                    .isEqualTo(EXIT_CODE_OK);
            assertThat(out.toString()).contains("--sun-misc-unsafe-memory-access=debug");
        }

        @Test
        void shouldUseEnvironmentJavaOptionsWhenGiven() {
            assertThat(execute(
                            List.of("dbms", "test-command"), Map.of(Bootloader.ENV_JAVA_OPTS, "-XX:+UseZGC -Xlog:gc")))
                    .isEqualTo(EXIT_CODE_OK);
            assertThat(out.toString())
                    .contains("-XX:+UseZGC")
                    .contains("-Xlog:gc")
                    // parallel GC is used by default by admin commands,
                    // this JVM option should be overridden by the passed JAVA_OPTS
                    .doesNotContain("-XX:+UseParallelGC");
        }

        @Test
        void shouldIgnoreJvmOptionsFromConfigWhenJavaOptionsVariablePresent() {
            addConf(
                    BootloaderSettings.additional_jvm,
                    "-agentlib:jdwp=transport=dt_socket,server=y,suspend=n,address=*:5005");
            assertThat(execute(
                            List.of("dbms", "test-command"), Map.of(Bootloader.ENV_JAVA_OPTS, "-XX:+UseZGC -Xlog:gc")))
                    .isEqualTo(EXIT_CODE_OK);
            assertThat(out.toString())
                    .contains("-XX:+UseZGC")
                    .contains("-Xlog:gc")
                    // parallel GC is used by default by admin commands,
                    // this JVM option should be overridden by the passed JAVA_OPTS
                    .doesNotContain("-XX:+UseParallelGC")
                    .doesNotContain("-agentlib:jdwp=transport=dt_socket,server=y,suspend=n,address=*:5005");
        }

        @Test
        void shouldIgnoreHeapSizeWhenJavaOptionsVariablePresent() {
            assertThat(execute(
                            List.of("dbms", "test-command"),
                            Map.of(Bootloader.ENV_JAVA_OPTS, "-XX:+UseZGC -Xlog:gc", Bootloader.ENV_HEAP_SIZE, "666m")))
                    .isEqualTo(EXIT_CODE_OK);
            assertThat(out.toString())
                    .contains("-XX:+UseZGC")
                    .contains("-Xlog:gc")
                    .doesNotContain("-Xmx666m")
                    .doesNotContain("-Xms666m");
            assertThat(err.toString()).contains("WARNING! HEAP_SIZE is ignored, because JAVA_OPTS is set");
        }

        @Test
        void shouldHandleExpandCommandsAndPassItThrough() {
            if (IS_OS_WINDOWS) {
                // This cannot run on Windows if the user is running as elevated to admin rights since this creates a
                // scenario
                // where it's essentially impossible to create correct ACL/owner of the config file that passes the
                // validation in the config reading.
                assumeThat(isCurrentlyRunningAsWindowsAdmin()).isFalse();
            }
            String cmd = String.format("$(%secho foo)", IS_OS_WINDOWS ? "cmd.exe /c " : "");
            addConf(GraphDatabaseSettings.initial_default_database, cmd);
            assertThat(execute("dbms", "test-command", "--expand-commands")).isEqualTo(EXIT_CODE_OK);
            assertThat(out.toString()).containsSubsequence("test-command", "--expand-commands");
        }

        @Test
        void shouldPassThroughVerbose() {
            assertThat(execute("dbms", "test-command", "--verbose")).isEqualTo(EXIT_CODE_OK);
            assertThat(out.toString()).containsSubsequence("test-command", "--verbose");
        }

        @Test
        void shouldPassEndOfOptionsDelimiterAndRetainOrder() {
            assertThat(execute("dbms", "test-command", "--pre --verbose --before -- after"))
                    .isNotZero();
            assertThat(err.toString()).containsSubsequence("--pre", "--verbose", "--before", "--", "after");
        }

        @Test
        void shouldFailOnMissingExpandCommands() {
            addConf(BootloaderSettings.max_heap_size, "$(echo foo)");
            assertThat(execute("dbms", "test-command")).isEqualTo(ExitCode.SOFTWARE);
            assertThat(err.toString())
                    .containsSubsequence(
                            "Failed to read config",
                            "is a command, but config is not explicitly told to expand it. (Missing --expand-commands argument?)",
                            "Run with '--verbose' for a more detailed error message");

            clearOutAndErr();
            assertThat(execute("--verbose", "dbms", "test-command")).isEqualTo(ExitCode.SOFTWARE);
            assertThat(err.toString())
                    .containsSubsequence(
                            "Failed to read config", "is a command, but config is not explicitly told to expand it")
                    .doesNotContain("Run with '--verbose' for a more detailed error message");
        }

        @Override
        protected CommandLine createCommand(
                PrintStream out,
                PrintStream err,
                Function<String, String> envLookup,
                Function<String, String> propLookup,
                Runtime.Version version) {
            var environment = new Environment(out, err, envLookup, propLookup, version);
            var command = new Neo4jAdminCommand(environment) {
                @Override
                protected Bootloader.Admin createAdminBootloader(String[] args) {
                    var bootloader = spy(super.createAdminBootloader(args));
                    ProcessManager pm =
                            new FakeProcessManager(config, bootloader, new ProcessHandler(), AdminTool.class);
                    doAnswer(inv -> pm).when(bootloader).processManager();
                    return bootloader;
                }
            };

            return Neo4jAdminCommand.asCommandLine(command, environment);
        }
    }

    @CommandLine.Command(name = "test-command", description = "Command for testing purposes only")
    static class TestCommand extends AbstractAdminCommand {
        static final String MSG = "Test command executed";
        static final String THROW_MSG = "Test command expected failure message";

        @CommandLine.Parameters(hidden = true)
        private List<String> allParameters = List.of();

        @CommandLine.Option(names = "--throw")
        private boolean shouldThrow;

        TestCommand(ExecutionContext ctx) {
            super(ctx);
        }

        @Override
        protected void execute() {
            if (shouldThrow) {
                throw new CommandFailedException(THROW_MSG, org.neo4j.cli.ExitCode.FAIL, false);
            }
            ctx.out().println(MSG);
            for (String param : allParameters) {
                ctx.out().println(param);
            }
        }
    }

    @ServiceProvider
    public static class TestCommandProvider implements CommandProvider {
        @Override
        public TestCommand createCommand(ExecutionContext ctx) {
            return new TestCommand(ctx);
        }

        @Override
        public CommandType commandType() {
            return CommandType.TEST;
        }
    }
}
