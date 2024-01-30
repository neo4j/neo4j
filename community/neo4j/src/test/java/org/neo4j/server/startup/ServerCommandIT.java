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

import static java.lang.System.currentTimeMillis;
import static java.util.concurrent.TimeUnit.MINUTES;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assumptions.assumeThat;
import static org.neo4j.internal.helpers.Exceptions.stringify;
import static org.neo4j.server.startup.Bootloader.EXIT_CODE_OK;
import static org.neo4j.test.assertion.Assert.assertEventually;

import java.io.IOException;
import java.io.PrintStream;
import java.nio.file.Path;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.DisabledOnOs;
import org.junit.jupiter.api.condition.EnabledOnOs;
import org.junit.jupiter.api.condition.OS;
import org.neo4j.cli.ExitCode;
import org.neo4j.configuration.BootloaderSettings;
import org.neo4j.configuration.GraphDatabaseSettings;
import org.neo4j.io.fs.FileSystemUtils;
import org.neo4j.memory.EmptyMemoryTracker;

/**
 * A base test for some commands in 'neo4j-admin server' and 'neo4j' group.
 */
abstract class ServerCommandIT extends ServerProcessTestBase {

    // Test is locale sensitive, error messages will change in non-English locale
    private static final Locale defaultLocale = Locale.getDefault();

    @BeforeAll
    public static void beforeAll() {
        Locale.setDefault(Locale.ROOT);
    }

    @AfterAll
    public static void afterAll() {
        Locale.setDefault(defaultLocale);
    }

    @Test
    void startShouldFailWithNiceOutputOnInvalidNeo4jConfig() {
        addConf(GraphDatabaseSettings.pagecache_memory, "hgb!C2/#C");
        var exitCode = execute("start");
        assertThat(exitCode).isEqualTo(ExitCode.FAIL);
        assertThat(err.toString())
                .contains(
                        "Error: Error evaluating value for setting 'server.memory.pagecache.size'.",
                        "Configuration contains errors.");
    }

    @Test
    void startShouldFailOnInvalidUserLog4jConfig() throws IOException {
        Path log4jConfig = config.get(GraphDatabaseSettings.user_logging_config_path);
        FileSystemUtils.writeString(fs, log4jConfig, "<Configuration></Cunfigoratzion>", EmptyMemoryTracker.INSTANCE);
        int exitCode = execute("start");
        assertThat(exitCode).isEqualTo(ExitCode.FAIL);
        assertThat(err.toString())
                .contains(
                        "Error at 1:18: The element type \"Configuration\" must be terminated by the matching end-tag \"</Configuration>\".",
                        "Configuration contains errors.");
    }

    @Test
    @DisabledOnOs(OS.WINDOWS)
    void startShouldBeAllowedWithWarningsOnInvalidServerLog4jConfig() throws IOException {
        Path log4jConfig = config.get(GraphDatabaseSettings.server_logging_config_path);
        FileSystemUtils.writeString(fs, log4jConfig, "<Configuration></Cunfigoratzion>", EmptyMemoryTracker.INSTANCE);
        int exitCode = execute("start");
        assertThat(exitCode).isEqualTo(ExitCode.OK);
        assertThat(err.toString())
                .contains(
                        "Warning at 1:18: The element type \"Configuration\" must be terminated by the matching end-tag \"</Configuration>\".");
    }

    @Test
    @EnabledOnOs(OS.WINDOWS)
    void startShouldBeAllowedWithWarningsOnInvalidServerLog4jConfigOnWindows() throws IOException {
        assumeThat(isCurrentlyRunningAsWindowsAdmin()).isTrue();
        addConf(BootloaderSettings.windows_service_name, "neo4j-" + currentTimeMillis());
        try {
            assertThat(execute("windows-service", "install")).isEqualTo(EXIT_CODE_OK);
            startShouldBeAllowedWithWarningsOnInvalidServerLog4jConfig();
        } finally {
            assertThat(execute("windows-service", "uninstall")).isEqualTo(EXIT_CODE_OK);
        }
    }

    @DisabledOnOs(OS.WINDOWS)
    @Test
    void shouldBeAbleToStartAndStopRealServerOnNonWindows() {
        shouldBeAbleToStartAndStopRealServer();
        assertThat(err.toString()).contains("WARNING: Using incubator modules: jdk.incubator.vector");
    }

    @EnabledOnOs(OS.WINDOWS)
    @Test
    void shouldBeAbleToStartAndStopRealServerOnWindows() {
        assumeThat(isCurrentlyRunningAsWindowsAdmin()).isTrue();
        addConf(BootloaderSettings.windows_service_name, "neo4j-" + currentTimeMillis());
        try {
            assertThat(execute("windows-service", "install")).isEqualTo(EXIT_CODE_OK);
            shouldBeAbleToStartAndStopRealServer();
        } finally {
            assertThat(execute("windows-service", "uninstall")).isEqualTo(EXIT_CODE_OK);
        }
        assertThat(err.toString()).isEmpty();
    }

    @EnabledOnOs(OS.WINDOWS)
    @Test
    void shouldBeAbleToUpdateRealServerOnWindows() throws InterruptedException, IOException {
        assumeThat(isCurrentlyRunningAsWindowsAdmin()).isTrue();
        addConf(BootloaderSettings.windows_service_name, "neo4j-" + currentTimeMillis());
        try {
            assertThat(execute("windows-service", "install")).isEqualTo(0);

            int updatedSetting = 2 * INITIAL_HEAP_MB;
            addConf(BootloaderSettings.initial_heap_size, String.format("%dm", updatedSetting));

            // Try a couple of times to issue the windows-service update call. There's an idea that on
            // WindowsServer2019 there may be a delay between installing the service and it being
            // available for being updated... so consider this temporary.
            int updateServiceResult = 0;
            for (int i = 0; i < 3; i++) {
                updateServiceResult = execute("windows-service", "update");
                if (updateServiceResult != 0) {
                    System.out.println("failed, print");
                    printVerboseWindowsDebugInformation();
                    Thread.sleep(2_000);
                } else {
                    break;
                }
            }

            assertThat(updateServiceResult)
                    .withFailMessage(() -> "Out:" + out.toString() + ", err: " + err.toString())
                    .isEqualTo(0);

            shouldBeAbleToStartAndStopRealServer(updatedSetting);
        } finally {
            assertThat(execute("windows-service", "uninstall")).isEqualTo(0);
        }
        assertThat(err.toString()).isEmpty();
    }

    private void printVerboseWindowsDebugInformation() throws IOException {
        PrintStream err = new PrintStream(this.err);
        var environment =
                new Environment(new PrintStream(out), err, System::getenv, System::getProperty, Runtime.version());
        try (var bootloader = new Bootloader.Dbms(environment, false, false)) {
            WindowsBootloaderOs windows = (WindowsBootloaderOs) BootloaderOsAbstraction.getOsAbstraction(bootloader);
            try {
                err.println("Printing results from Get-Service call:");
                for (String resultRow : windows.getServiceStatusResult()) {
                    err.println(resultRow);
                }
            } catch (BootProcessFailureException e) {
                err.println(stringify(e));
            }
        }
    }

    private void shouldBeAbleToStartAndStopRealServer() {
        shouldBeAbleToStartAndStopRealServer(INITIAL_HEAP_MB);
    }

    private void shouldBeAbleToStartAndStopRealServer(int initialHeapSize) {
        int startSig = execute(List.of("start"), Map.of());
        assertThat(startSig).isEqualTo(EXIT_CODE_OK);
        assertEventually(
                this::getDebugLogLines,
                s -> s.contains(String.format("-Xms%dk, -Xmx%dk", initialHeapSize * 1024, MAX_HEAP_MB * 1024)),
                5,
                MINUTES);
        assertEventually(this::getDebugLogLines, s -> s.contains("NeoWebServer] ========"), 5, MINUTES);
        assertEventually(this::getUserLogLines, s -> s.contains("Remote interface available at"), 5, MINUTES);
        assertThat(execute("stop")).isEqualTo(EXIT_CODE_OK);
    }
}
