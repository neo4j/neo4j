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
package org.neo4j.dbms.diagnostics.profile;

import static java.lang.String.format;
import static java.util.concurrent.TimeUnit.MINUTES;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assumptions.assumeThat;
import static org.neo4j.test.assertion.Assert.assertEventually;
import static org.neo4j.test.conditions.Conditions.greaterThanOrEqualTo;

import java.io.IOException;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import org.apache.commons.io.output.NullPrintStream;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.DisabledOnOs;
import org.junit.jupiter.api.condition.OS;
import org.neo4j.configuration.BootloaderSettings;
import org.neo4j.configuration.Config;
import org.neo4j.configuration.GraphDatabaseSettings;
import org.neo4j.dbms.diagnostics.jmx.JMXDumper;
import org.neo4j.dbms.diagnostics.jmx.JmxDump;
import org.neo4j.internal.helpers.ProcessUtils;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.fs.FileSystemUtils;
import org.neo4j.memory.EmptyMemoryTracker;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.testdirectory.TestDirectoryExtension;
import org.neo4j.test.utils.TestDirectory;
import org.neo4j.time.Clocks;
import org.neo4j.time.Stopwatch;

@TestDirectoryExtension
@DisabledOnOs(OS.WINDOWS)
class NmtProfilerTest {
    @Inject
    TestDirectory dir;

    @Inject
    FileSystemAbstraction fs;

    NmtProfiler profiler;
    Process process;

    @AfterEach
    void after() throws InterruptedException {
        if (profiler != null && profiler.available()) {
            profiler.stop();
        }
        if (process != null) {
            process.destroyForcibly();
            process.waitFor(1, MINUTES);
        }
    }

    @Test
    void shouldNotBeAvailableWithoutNmt() throws IOException {
        startProcess(false, "");
        createProfiler();
        assertThat(profiler.available()).isFalse();
        assertThat(profiler.failure()).hasMessageContaining("NMT not enabled");
    }

    @Test
    void shouldProduceReportWithNmt() throws IOException {
        String report = runProfilerAndProduceReport("summary");
        assertThat(report).contains("Native Memory Tracking:", "reserved", "committed");
    }

    @Test
    void shouldProduceDetailReportWithNmt() throws IOException {
        String report = runProfilerAndProduceReport("detail");
        assertThat(report).contains("Native Memory Tracking:", "reserved", "committed", "Virtual memory map:");
    }

    private String runProfilerAndProduceReport(String mode) throws IOException {
        startProcess(true, mode);
        createProfiler();
        assertThat(profiler.available()).isTrue();
        profiler.start();
        assertEventually(() -> getReports("nmt-diff").length, greaterThanOrEqualTo(1), 1, MINUTES);

        Path[] reports = getReports("nmt-full");
        String report = FileSystemUtils.readString(fs, reports[0], EmptyMemoryTracker.INSTANCE);
        return report;
    }

    private Path[] getReports(String prefix) throws IOException {
        return fs.listFiles(
                dir.homePath(),
                path -> path.getFileName().toString().startsWith(prefix)
                        && !FileSystemUtils.readString(fs, path, EmptyMemoryTracker.INSTANCE)
                                .isEmpty());
    }

    private void startProcess(boolean withNmt, String mode) throws IOException {
        process = ProfileableProcess.startProcess(withNmt, mode);
    }

    private void createProfiler() throws IOException {
        assertThat(profiler).isNull();
        assertThat(process).isNotNull();
        Path pidFile = dir.file("test.pid");
        Config cfg = Config.newBuilder()
                .set(GraphDatabaseSettings.neo4j_home, dir.homePath())
                .set(BootloaderSettings.pid_file, pidFile)
                .build();
        FileSystemUtils.writeString(fs, pidFile, format("%s%n", process.pid()), EmptyMemoryTracker.INSTANCE);
        JMXDumper jmxDumper = new JMXDumper(cfg, fs, NullPrintStream.INSTANCE, NullPrintStream.INSTANCE, true);
        Optional<JmxDump> maybeDump = jmxDumper.getJMXDump();
        assumeThat(maybeDump).isPresent(); // IF not, then no point in running tests
        JmxDump jmxDump = maybeDump.get();
        profiler = new NmtProfiler(jmxDump, fs, dir.homePath(), Duration.ofSeconds(1), Clocks.nanoClock());
    }

    private static class ProfileableProcess {
        static Process startProcess(boolean withNmt, String mode) throws IOException {
            List<String> command = new ArrayList<>();
            command.add(ProcessUtils.getJavaExecutable().toString());
            if (withNmt) {
                command.add("-XX:NativeMemoryTracking=" + mode);
            }
            command.add("-cp");
            command.add(ProcessUtils.getClassPath());
            command.add(ProfileableProcess.class.getName());
            return new ProcessBuilder(command).start();
        }

        public static void main(String[] args) throws Exception {
            Stopwatch time = Stopwatch.start();
            while (!time.hasTimedOut(10, MINUTES)) {
                //noinspection BusyWait
                Thread.sleep(100);
            }
        }
    }
}
