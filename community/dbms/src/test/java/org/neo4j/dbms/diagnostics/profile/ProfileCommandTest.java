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
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.Assumptions.assumeThat;
import static org.neo4j.dbms.archive.StandardCompressionFormat.GZIP;
import static org.neo4j.test.assertion.Assert.assertEventually;
import static org.neo4j.test.conditions.Conditions.TRUE;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.io.PrintStream;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.Future;
import org.apache.commons.compress.archivers.ArchiveEntry;
import org.apache.commons.compress.archivers.tar.TarArchiveInputStream;
import org.apache.commons.io.output.NullPrintStream;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.DisabledOnOs;
import org.junit.jupiter.api.condition.OS;
import org.neo4j.cli.ExecutionContext;
import org.neo4j.configuration.BootloaderSettings;
import org.neo4j.configuration.Config;
import org.neo4j.configuration.GraphDatabaseSettings;
import org.neo4j.dbms.diagnostics.jmx.JMXDumper;
import org.neo4j.dbms.diagnostics.jmx.JmxDump;
import org.neo4j.internal.helpers.Exceptions;
import org.neo4j.internal.helpers.ProcessUtils;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.fs.FileSystemUtils;
import org.neo4j.memory.EmptyMemoryTracker;
import org.neo4j.test.OtherThreadExecutor;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.testdirectory.TestDirectoryExtension;
import org.neo4j.test.utils.TestDirectory;
import org.neo4j.time.Stopwatch;
import picocli.CommandLine;

@TestDirectoryExtension
@DisabledOnOs(OS.WINDOWS)
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
    private Config config;

    @BeforeEach
    void setUp() throws IOException {
        output = dir.directory("output");
        jfrDir = output.resolve("jfr");
        threadDumpDir = output.resolve("threads");
        config = Config.defaults(GraphDatabaseSettings.neo4j_home, dir.homePath());
        setPid(ProcessHandle.current().pid());
        ctxOut = new ByteArrayOutputStream();
        context = new ExecutionContext(
                dir.homePath(),
                dir.homePath(),
                new PrintStream(ctxOut),
                new PrintStream(new ByteArrayOutputStream()),
                fs);

        JMXDumper jmxDumper = new JMXDumper(config, fs, NullPrintStream.INSTANCE, NullPrintStream.INSTANCE, true);
        Optional<JmxDump> maybeDump = jmxDumper.getJMXDump();
        assumeThat(maybeDump).isPresent(); // IF not, then no point in running tests
        maybeDump.orElseThrow().close();
    }

    @Test
    void shouldExecuteForSomeTimeAndGenerateProfiles() throws Exception {
        execute(Duration.ofSeconds(1));
        unpackResult();
        assertThat(hasJfr()).isTrue();
        assertThat(hasThreadDumps()).isTrue();
    }

    @Test
    void shouldThrowWhenNoProcessIsFound() throws Exception {
        setPid(Long.MAX_VALUE);
        assertThatThrownBy(() -> execute(Duration.ofSeconds(1))).hasMessageContaining("Can not connect");
    }

    @Test
    void shouldBeAbleToSelectProfiles() throws Exception {
        execute(Duration.ofSeconds(1), "JFR");
        unpackResult();
        assertThat(hasJfr()).isTrue();
        assertThat(hasThreadDumps()).isFalse();

        fs.deleteRecursively(output);

        execute(Duration.ofSeconds(1), "THREADS");
        unpackResult();
        assertThat(hasJfr()).isFalse();
        assertThat(hasThreadDumps()).isTrue();

        fs.deleteRecursively(output);

        execute(Duration.ofSeconds(1), "JFR", "THREADS");
        unpackResult();
        assertThat(hasJfr()).isTrue();
        assertThat(hasThreadDumps()).isTrue();
    }

    @Test
    void shouldCompressResultByDefault() throws Exception {
        execute(Duration.ofSeconds(1));
        Path[] paths = fs.listFiles(output);
        assertThat(paths).hasSize(1);
        assertThat(paths[0].getFileName().toString()).endsWith(".gzip");
    }

    @Test
    void shouldAbortIfProcessDies() throws Exception {
        final var startedPath = dir.homePath().resolve("SeparateProcess.started");
        Process process = SeparateProcess.startProcess(startedPath);
        assertThat(process.isAlive()).isTrue();
        assertEventually(
                "SeparateProcess should start and write marker file",
                () -> Files.exists(startedPath),
                exists -> exists,
                1,
                MINUTES);
        setPid(process.pid());

        Stopwatch start = Stopwatch.start();
        Future<Void> future = null;
        try (OtherThreadExecutor otherThread = new OtherThreadExecutor("test")) {
            future = otherThread.executeDontWait(() -> {
                try {
                    return execute(Duration.ofMinutes(10));
                } catch (Exception e) {
                    // add some extra debug to track down flakiness
                    System.out.println(ctxOut.toString());
                    if (!process.isAlive()) {
                        throw Exceptions.chain(
                                e, new IllegalStateException("Process died with " + process.exitValue()));
                    }
                    throw e;
                }
            });
            assertEventually(this::hasThreadDumps, TRUE, 1, MINUTES);
            process.destroy();
            process.waitFor();
        } finally {
            if (future != null) {
                future.get();
            }
        }
        assertThat(start.elapsed()).isLessThan(Duration.ofMinutes(1));
        assertThat(ctxOut.toString()).contains("All profilers failed");
    }

    private boolean hasJfr() throws IOException {
        return fs.isDirectory(jfrDir)
                && fs.listFiles(jfrDir, path -> path.getFileName().toString().endsWith(".jfr")).length == 1;
    }

    private boolean hasThreadDumps() throws IOException {
        DirectoryStream.Filter<Path> dumps =
                path -> path.getFileName().toString().startsWith("threads");
        return fs.isDirectory(threadDumpDir) && fs.listFiles(threadDumpDir, dumps).length > 0;
    }

    private void setPid(long pid) throws IOException {
        Path pidFile = config.get(BootloaderSettings.pid_file);
        fs.mkdirs(pidFile.getParent());
        FileSystemUtils.writeString(fs, pidFile, format("%s%n", pid), EmptyMemoryTracker.INSTANCE);
    }

    private Void execute(Duration duration, String... additionalArgs) throws Exception {
        ProfileCommand cmd = new ProfileCommand(context);
        List<String> args = new ArrayList<>();
        args.add(output.toString());
        args.add(duration.getSeconds() + "s");
        Collections.addAll(args, additionalArgs);

        CommandLine.populateCommand(cmd, args.toArray(new String[0]));
        cmd.execute();
        return null;
    }

    private void unpackResult() throws IOException {
        Path[] paths = fs.listFiles(output);
        assertThat(paths).hasSize(1);

        try (TarArchiveInputStream stream =
                new TarArchiveInputStream(GZIP.decompress(fs.openAsInputStream(paths[0])))) {
            ArchiveEntry entry;
            while ((entry = stream.getNextEntry()) != null) {
                Path file = output.resolve(entry.getName());
                if (entry.isDirectory()) {
                    fs.mkdirs(file);
                } else {
                    try (OutputStream output = fs.openAsOutputStream(file, false)) {
                        stream.transferTo(output);
                    }
                }
            }
        }
    }

    private static class SeparateProcess {
        static Process startProcess(Path startedPath) throws IOException {
            List<String> command = new ArrayList<>();
            command.add(ProcessUtils.getJavaExecutable().toString());
            command.add("-cp");
            command.add(ProcessUtils.getClassPath());
            command.add(SeparateProcess.class.getName());
            command.add(startedPath.toAbsolutePath().toString());
            return new ProcessBuilder(command).start();
        }

        public static void main(String[] args) throws Exception {
            assert args.length == 1;
            Files.writeString(
                    Paths.get(args[0]), "starting...", StandardOpenOption.CREATE_NEW, StandardOpenOption.WRITE);

            Stopwatch time = Stopwatch.start();
            while (!time.hasTimedOut(10, MINUTES)) {
                //noinspection BusyWait
                Thread.sleep(100);
            }
        }
    }
}
