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
package org.neo4j.shell.terminal;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assumptions.assumeTrue;
import static org.mockito.Mockito.mock;

import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.OutputStream;
import java.nio.file.FileSystems;
import java.nio.file.Path;
import java.time.Duration;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import org.jline.reader.LineReaderBuilder;
import org.jline.reader.impl.history.DefaultHistory;
import org.jline.terminal.Terminal;
import org.jline.terminal.impl.DumbTerminal;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.neo4j.shell.completions.DbInfo;
import org.neo4j.shell.exception.UserInterruptException;
import org.neo4j.shell.parser.ShellStatementParser;
import org.neo4j.shell.printer.Printer;

/**
 * {@code TerminalBuilder.streams(in, out)} always produces an {@code ExternalTerminal}, even when "dumb" is
 * requested (see {@code ExecTerminalProvider.newTerminal}) - a genuine {@code DumbTerminal} only exists via its
 * own constructor, used directly here.
 */
class JlineTerminalTest {
    @Test
    void maskedPromptOnGenuineDumbTerminalCanBeInterrupted(@TempDir Path tempDir) throws Exception {
        // A fifo blocks like a real pty fd; a PipedInputStream wouldn't exercise this (it wakes on interrupt).
        assumeTrue(FileSystems.getDefault().supportedFileAttributeViews().contains("posix"));
        var fifo = tempDir.resolve("fifo");
        var mkfifo = new ProcessBuilder("mkfifo", fifo.toString()).start();
        assertThat(mkfifo.waitFor()).isZero();

        // A fifo's read end blocks until a writer opens it, so open the write end on a background thread.
        var keepOpen = new AtomicReference<OutputStream>();
        var writerThread = new Thread(() -> {
            try {
                keepOpen.set(new FileOutputStream(fifo.toFile()));
            } catch (Exception ignored) {
            }
        });
        writerThread.setDaemon(true);
        writerThread.start();

        var in = new FileInputStream(fifo.toFile());
        try {
            var dumbTerminal = new DumbTerminal(in, OutputStream.nullOutputStream());

            var jLineReader = LineReaderBuilder.builder()
                    .terminal(dumbTerminal)
                    .parser(new StatementJlineParser(new ShellStatementParser()))
                    .expander(new JlineTerminal.EmptyExpander())
                    .history(new DefaultHistory())
                    .build();

            var terminal = new JlineTerminal(
                    jLineReader,
                    true,
                    mock(Printer.class),
                    SimplePrompt::defaultPrompt,
                    Duration.ZERO,
                    Duration.ZERO,
                    mock(DbInfo.class),
                    new ActivityListener());
            var reader = terminal.read();

            var result = new AtomicReference<Object>();
            Thread t = new Thread(() -> {
                try {
                    result.set(reader.simplePrompt("password: ", true));
                } catch (Exception e) {
                    result.set(e);
                }
            });
            t.setDaemon(true);
            t.start();

            // Poll until genuinely blocked before raising the signal, so it isn't delivered before the handler
            // is installed.
            var deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
            while (t.getState() != Thread.State.WAITING && t.getState() != Thread.State.TIMED_WAITING) {
                assertThat(System.nanoTime()).isLessThan(deadline);
                Thread.sleep(10);
            }

            dumbTerminal.raise(Terminal.Signal.INT);
            t.join(5000);

            assertThat(result.get()).isInstanceOf(UserInterruptException.class);
        } finally {
            // Join before reading keepOpen: open() returning doesn't mean writerThread has set it yet. Close
            // the writer before the reader - closing a fifo's read end with its write end still open can
            // deadlock close() on some platforms.
            writerThread.join();
            var out = keepOpen.get();
            if (out != null) {
                out.close();
            }
            in.close();
        }
    }
}
