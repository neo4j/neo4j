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
package org.neo4j.dbms.diagnostics.profile;

import static java.lang.String.format;
import static java.util.concurrent.TimeUnit.MINUTES;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.neo4j.test.assertion.Assert.assertEventually;
import static org.neo4j.test.conditions.Conditions.TRUE;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.PrintStream;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.neo4j.cli.ExecutionContext;
import org.neo4j.configuration.BootloaderSettings;
import org.neo4j.configuration.Config;
import org.neo4j.configuration.GraphDatabaseSettings;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.fs.FileSystemUtils;
import org.neo4j.memory.EmptyMemoryTracker;
import org.neo4j.test.OtherThreadExecutor;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.testdirectory.TestDirectoryExtension;
import org.neo4j.test.proc.ProcessUtil;
import org.neo4j.test.utils.TestDirectory;
import org.neo4j.time.Stopwatch;
import picocli.CommandLine;

@TestDirectoryExtension
class ProfileCommandTest {

    @Inject
    TestDirectory dir;

    @Inject
    FileSystemAbstraction fs;

    private Path output;
    private Path jfrDir;
    private Path threadDumpDir;
    private ExecutionContext context;
    private ByteArrayOutputStream ctxOut;

    @BeforeEach
    void setUp() throws IOException {
        output = dir.directory("output");
        jfrDir = output.resolve("jfr");
        threadDumpDir = output.resolve("threads");
        fs.mkdirs(jfrDir);
        fs.mkdirs(threadDumpDir);
        setPid(ProcessHandle.current().pid());
        ctxOut = new ByteArrayOutputStream();
        context = new ExecutionContext(
                dir.homePath(),
                dir.homePath(),
                new PrintStream(ctxOut),
                new PrintStream(new ByteArrayOutputStream()),
                fs);
    }

    @Test
    void shouldExecuteForSomeTimeAndGenerateProfiles() throws Exception {
        execute(Duration.ofSeconds(1));
        assertHasJfr();
        assertHasThreadDumps();
    }

    @Test
    void shouldThrowWhenNoProcessIsFound() throws Exception {
        setPid(Long.MAX_VALUE);
        assertThatThrownBy(() -> execute(Duration.ofSeconds(1))).hasMessageContaining("Can not connect");
    }

    @Test
    void shouldAbortIfProcessDies() throws Exception {
        Process process = SeparateProcess.startProcess();
        assertThat(process.isAlive()).isTrue();
        setPid(process.pid());

        Stopwatch start = Stopwatch.start();
        try (OtherThreadExecutor otherThread = new OtherThreadExecutor("test")) {
            Future<Void> future = otherThread.executeDontWait(() -> execute(Duration.ofMinutes(10)));
            assertEventually(this::hasThreadDumps, TRUE, 1, MINUTES);
            process.destroy();
            process.waitFor();
            future.get();
        }
        assertThat(start.elapsed()).isLessThan(Duration.ofMinutes(1));
        assertThat(ctxOut.toString()).contains("All profilers failed");
    }

    private void assertHasJfr() throws IOException {
        List<Path> files = Arrays.stream(fs.listFiles(
                        jfrDir, path -> path.getFileName().toString().endsWith(".jfr")))
                .toList();
        assertThat(files).hasSize(1);
    }

    private void assertHasThreadDumps() throws IOException {
        assertThat(hasThreadDumps()).isTrue();
    }

    private boolean hasThreadDumps() throws IOException {
        List<Path> files = Arrays.stream(fs.listFiles(
                        threadDumpDir, path -> path.getFileName().toString().startsWith("threads")))
                .toList();
        return files.size() > 0;
    }

    private void setPid(long pid) throws IOException {
        Path pidFile = Config.defaults(GraphDatabaseSettings.neo4j_home, dir.homePath())
                .get(BootloaderSettings.pid_file);
        fs.mkdirs(pidFile.getParent());
        FileSystemUtils.writeString(fs, pidFile, format("%s%n", pid), EmptyMemoryTracker.INSTANCE);
    }

    private Void execute(Duration duration) throws Exception {
        ProfileCommand cmd = new ProfileCommand(context);
        CommandLine.populateCommand(cmd, output.toString(), duration.getSeconds() + "s");
        cmd.execute();
        return null;
    }

    private static class SeparateProcess {
        static Process startProcess() throws IOException {
            List<String> command = new ArrayList<>();
            command.add(ProcessUtil.getJavaExecutable().toString());
            command.add("-cp");
            command.add(ProcessUtil.getClassPath());
            command.add(SeparateProcess.class.getName());
            return new ProcessBuilder(command).start();
        }

        public static void main(String[] args) throws InterruptedException {
            Thread.sleep(TimeUnit.MINUTES.toMillis(10));
        }
    }
}
