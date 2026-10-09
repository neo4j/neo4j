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

import static java.util.stream.StreamSupport.stream;

import java.io.IOException;
import java.io.InterruptedIOException;
import java.nio.file.FileAlreadyExistsException;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.attribute.PosixFilePermissions;
import java.time.Duration;
import java.time.temporal.ChronoUnit;
import java.util.List;
import java.util.function.Supplier;
import org.jline.reader.Expander;
import org.jline.reader.History;
import org.jline.reader.LineReader;
import org.jline.reader.ParsedLine;
import org.jline.terminal.Attributes;
import org.jline.terminal.Terminal;
import org.jline.utils.NonBlockingReader;
import org.neo4j.shell.Historian;
import org.neo4j.shell.completions.DbInfo;
import org.neo4j.shell.exception.NoMoreInputException;
import org.neo4j.shell.exception.UserInterruptException;
import org.neo4j.shell.log.Logger;
import org.neo4j.shell.parser.StatementParser.ParsedStatements;
import org.neo4j.shell.printer.AnsiFormattedText;
import org.neo4j.shell.printer.Printer;
import org.neo4j.shell.timeout.IdleTimeoutService;
import org.neo4j.util.VisibleForTesting;

/**
 * CypherShellTerminal backed by jline.
 */
public class JlineTerminal implements CypherShellTerminal {
    private static final Logger log = Logger.create();
    static final String NO_CONTINUATION_PROMPT_PATTERN = "  ";
    private static final char MASK_CHAR = '*';

    private final LineReader jLineReader;
    private final Printer printer;
    private final Reader reader;
    private final Writer writer;
    private final boolean isInteractive;
    private final DbInfo dbInfo;
    private final Supplier<SimplePrompt> simplePromptSupplier;
    private final IdleTimeoutService exitOnIdleTimeoutService;
    private final IdleTimeoutService stopPollingWhenIdleService;

    public JlineTerminal(
            LineReader jLineReader,
            boolean isInteractive,
            Printer printer,
            Supplier<SimplePrompt> simplePromptSupplier,
            Duration idleTimeout,
            Duration idleDelay,
            DbInfo dbInfo,
            ActivityListener activityListener) {
        assert jLineReader.getParser() instanceof StatementJlineParser;
        this.jLineReader = jLineReader;
        this.printer = printer;
        this.isInteractive = isInteractive;
        this.simplePromptSupplier = simplePromptSupplier;
        this.reader = new JLineReader();
        this.writer = new JLineWriter();
        this.dbInfo = dbInfo;
        this.exitOnIdleTimeoutService = IdleTimeoutService.create(idleTimeout, idleDelay, () -> {
            System.err.println(
                    "Timeout after idling, avoid this by increasing --idle-timeout or omitting it completely.");
            System.exit(124);
        });
        this.stopPollingWhenIdleService = IdleTimeoutService.create(
                Duration.of(1, ChronoUnit.MINUTES),
                Duration.of(30, ChronoUnit.SECONDS),
                dbInfo::stopPolling,
                dbInfo::resumePolling);
        activityListener.set(() -> {
            exitOnIdleTimeoutService.imAwake();
            stopPollingWhenIdleService.imAwake();
        });
    }

    private StatementJlineParser getParser() {
        return (StatementJlineParser) jLineReader.getParser();
    }

    @Override
    public Reader read() {
        jLineReader.getTerminal().resume();
        return reader;
    }

    @VisibleForTesting
    public Terminal getTerminal() {
        return jLineReader.getTerminal();
    }

    @Override
    public SimplePrompt simplePrompt() {
        return simplePromptSupplier.get();
    }

    @Override
    public Writer write() {
        return writer;
    }

    @Override
    public boolean isInteractive() {
        return isInteractive;
    }

    @Override
    public Historian getHistory() {
        return new JlineHistorian();
    }

    @Override
    public void setHistoryBehaviour(HistoryBehaviour behaviour) throws IOException {
        if (behaviour instanceof FileHistory fileHistory) {
            setFileHistory(fileHistory.historyFile());
        } else if (behaviour instanceof DefaultHistory) {
            setFileHistory(Historian.defaultHistoryFile());
        } else if (behaviour instanceof InMemoryHistory) {
            jLineReader.setVariable(LineReader.HISTORY_FILE, null);
            loadHistory();
        } else if (behaviour instanceof DisableHistory) {
            // This is handled on construction of the reader.
        }
    }

    private static void safeCreateHistoryFile(Path path) throws IOException {
        if (Files.isDirectory(path)) {
            throw new IOException("History file cannot be a directory, please delete " + path);
        }
        if (!Files.exists(path.getParent())) {
            Files.createDirectories(path.getParent());
        }
        if (!Files.exists(path)) {
            try {
                Files.createFile(path);
            } catch (FileAlreadyExistsException e) {
                // Ignore
            }
        }
        if (isPosix()) {
            Files.setPosixFilePermissions(path, PosixFilePermissions.fromString("rw-------"));
        }
    }

    private static boolean isPosix() {
        return FileSystems.getDefault().supportedFileAttributeViews().contains("posix");
    }

    private void setFileHistory(Path file) throws IOException {
        safeCreateHistoryFile(file);
        if (!file.equals(jLineReader.getVariable(LineReader.HISTORY_FILE))) {
            jLineReader.setVariable(LineReader.HISTORY_FILE, file);
            // the load here makes sure that history will work right from the start
            loadHistory();
            Runtime.getRuntime().addShutdownHook(new Thread(this::flushHistory));
        }
    }

    @Override
    public void bindUserInterruptHandler(UserInterruptHandler handler) {
        jLineReader.getTerminal().handle(Terminal.Signal.INT, signal -> handler.handleUserInterrupt());
    }

    private void flushHistory() {
        try {
            getHistory().flushHistory();
        } catch (IOException e) {
            log.error("Failed to save history", e);
            printer.printError("Failed to save history: " + e.getMessage());
        }
    }

    private void loadHistory() {
        try {
            jLineReader.getHistory().load();
        } catch (IOException e) {
            log.error("Failed to load history", e);
            printer.printError("Failed to load history: " + e.getMessage());
        }
    }

    @Override
    public void close() throws Exception {
        exitOnIdleTimeoutService.close();
        dbInfo.close();
    }

    private class JlineHistorian implements Historian {
        @Override
        public List<String> getHistory() {
            loadHistory();
            return stream(jLineReader.getHistory().spliterator(), false)
                    .map(History.Entry::line)
                    .toList();
        }

        @Override
        public void flushHistory() throws IOException {
            jLineReader.getHistory().save();
        }

        @Override
        public void clear() throws IOException {
            jLineReader.getHistory().purge();
        }
    }

    private class JLineReader implements Reader {
        private interface ThrowingSupplier<T> {
            T get() throws NoMoreInputException, UserInterruptException;
        }

        private <T> T withIdleServicesActive(ThrowingSupplier<T> read)
                throws NoMoreInputException, UserInterruptException {
            try {
                stopPollingWhenIdleService.resume();
                exitOnIdleTimeoutService.resume();
                return read.get();
            } finally {
                stopPollingWhenIdleService.pause();
                exitOnIdleTimeoutService.pause();
            }
        }

        private String readLine(String prompt, boolean mask) throws NoMoreInputException, UserInterruptException {
            return withIdleServicesActive(() -> {
                try {
                    final Character mask0 = mask ? MASK_CHAR : null;
                    return jLineReader.readLine(prompt, mask0, null);
                } catch (org.jline.reader.EndOfFileException e) {
                    throw new NoMoreInputException();
                } catch (org.jline.reader.UserInterruptException e) {
                    throw new UserInterruptException(e.getPartialLine());
                }
            });
        }

        @Override
        public ParsedStatements readStatement(AnsiFormattedText prompt)
                throws NoMoreInputException, UserInterruptException {
            getParser().setEnableStatementParsing(true);
            jLineReader.setVariable(LineReader.SECONDARY_PROMPT_PATTERN, continuationPromptPattern(prompt));

            var line = readLine(prompt.resetAndRender(), false);
            var parsed = jLineReader.getParsedLine();

            if (parsed instanceof ParsedLineStatements statements) {
                if (!statements.line().equals(line)) {
                    throw new IllegalStateException("Unparsed lines do not match");
                }
                return statements.statements();
            } else {
                throw new IllegalStateException(
                        "Unexpected type of parsed line " + parsed.getClass().getSimpleName());
            }
        }

        private String continuationPromptPattern(AnsiFormattedText prompt) {
            if (prompt.textLength() > PROMPT_MAX_LENGTH) {
                return NO_CONTINUATION_PROMPT_PATTERN;
            } else {
                // Note, jline has built in support for this using '%P', but that causes a bug in certain environments
                // https://github.com/jline/jline3/issues/751
                return " ".repeat(prompt.textLength());
            }
        }

        @Override
        public String simplePrompt(String prompt, boolean mask) throws NoMoreInputException, UserInterruptException {
            // JLine's dumb-terminal mask thread corrupts the prompt.
            if (mask && isTerminalDumb()) {
                return withIdleServicesActive(() -> readMaskedLineFromTerminal(prompt));
            }

            try {
                // Temporarily disable history, completion and statement parsing for simple prompts
                jLineReader.getVariables().put(LineReader.DISABLE_HISTORY, Boolean.TRUE);
                jLineReader.getVariables().put(LineReader.DISABLE_COMPLETION, Boolean.TRUE);
                getParser().setEnableStatementParsing(false);

                return readLine(prompt, mask);
            } finally {
                jLineReader.getVariables().remove(LineReader.DISABLE_HISTORY);
                jLineReader.getVariables().remove(LineReader.DISABLE_COMPLETION);
                getParser().setEnableStatementParsing(true);
            }
        }

        private boolean isTerminalDumb() {
            var type = jLineReader.getTerminal().getType();
            return Terminal.TYPE_DUMB.equals(type) || Terminal.TYPE_DUMB_COLOR.equals(type);
        }

        private static final long READ_TIMEOUT_MILLIS = 100;

        // Finite timeout, not a single blocking read: on a real DumbTerminal, a zero-timeout read falls into a
        // plain blocking stream read that Thread.interrupt() can't wake (same reason jline's own
        // BindingReader.readCharacter polls instead of a single read).
        private int readWithTimeout(NonBlockingReader reader) throws IOException {
            while (true) {
                // A buffered byte returns immediately without checking Thread.interrupted(), so check
                // explicitly or a concurrent interrupt could go unnoticed while input keeps arriving.
                if (Thread.interrupted()) {
                    throw new InterruptedIOException();
                }
                int c = reader.read(READ_TIMEOUT_MILLIS);
                if (Thread.interrupted()) {
                    throw new InterruptedIOException();
                }
                if (c != NonBlockingReader.READ_EXPIRED) {
                    return c;
                }
            }
        }

        // peek() has the same buffered-byte fast path as readWithTimeout; not looped, since a timed-out peek
        // is a legitimate "no newline yet" answer rather than something to retry.
        private int peekWithTimeout(NonBlockingReader reader) throws IOException {
            if (Thread.interrupted()) {
                throw new InterruptedIOException();
            }
            int c = reader.peek(READ_TIMEOUT_MILLIS);
            if (Thread.interrupted()) {
                throw new InterruptedIOException();
            }
            return c;
        }

        // Reads via the terminal's own reader rather than a second stream consumer: the pump thread already
        // owns the raw input once resumed, so a separate reader on the same stream would race it for bytes.
        private String readMaskedLineFromTerminal(String prompt) throws NoMoreInputException, UserInterruptException {
            var terminal = jLineReader.getTerminal();
            var writer = terminal.writer();

            // Disable echo (and ECHONL, which echoes the newline even with ECHO off) before printing the
            // prompt, not after, so keystrokes aren't echoed in the window before attributes take effect.
            var originalAttributes = terminal.getAttributes();
            var maskedAttributes = new Attributes(originalAttributes);
            maskedAttributes.setLocalFlag(Attributes.LocalFlag.ECHO, false);
            maskedAttributes.setLocalFlag(Attributes.LocalFlag.ECHONL, false);
            terminal.setAttributes(maskedAttributes);

            // Install before the prompt is visible, not after: a Ctrl-C in that window would otherwise reach
            // the stale previous handler instead of interrupting this read.
            var readingThread = Thread.currentThread();
            var previousIntrHandler = terminal.handle(Terminal.Signal.INT, signal -> readingThread.interrupt());

            writer.print(prompt);
            writer.flush();

            var reader = terminal.reader();
            var line = new StringBuilder();
            try {
                int c;
                while ((c = readWithTimeout(reader)) != -1 && c != '\n' && c != '\r') {
                    // jline's own EOF and Ctrl-D handling is bypassed here too; a non-empty line's Ctrl-D is a
                    // no-op (DELETE_CHAR_OR_LIST falls through to completion, which is disabled), so consume it.
                    if (c == '\u0004') {
                        if (line.isEmpty()) {
                            throw new NoMoreInputException();
                        }
                        continue;
                    }
                    // Without ISIG configured (e.g. a real DumbTerminal), Ctrl-C arrives as a literal ETX byte
                    // rather than Terminal.Signal.INT.
                    if (c == '\u0003') {
                        writer.println();
                        writer.flush();
                        throw new UserInterruptException(line.toString());
                    }
                    // Backward-delete, reimplemented since this bypasses jline's line editor. By code point,
                    // not UTF-16 unit, so a supplementary character (e.g. an emoji) isn't split.
                    if (c == '\b' || c == '\u007f') {
                        if (!line.isEmpty()) {
                            var lastCodePointStart = line.offsetByCodePoints(line.length(), -1);
                            line.delete(lastCodePointStart, line.length());
                        }
                        continue;
                    }
                    line.append((char) c);
                }
                if (c == '\r' && peekWithTimeout(reader) == '\n') {
                    readWithTimeout(reader);
                }
                if (c == -1 && line.isEmpty()) {
                    throw new NoMoreInputException();
                }
            } catch (InterruptedIOException e) {
                writer.println();
                writer.flush();
                throw new UserInterruptException(line.toString());
            } catch (IOException e) {
                throw new NoMoreInputException();
            } finally {
                terminal.setAttributes(originalAttributes);
                terminal.handle(Terminal.Signal.INT, previousIntrHandler);
            }

            writer.println();
            writer.flush();
            return line.toString();
        }
    }

    private class JLineWriter implements Writer {
        @Override
        public void println(String line) {
            jLineReader.printAbove(line + System.lineSeparator());
        }
    }

    public static class EmptyExpander implements Expander {
        @Override
        public String expandHistory(History history, String line) {
            return line;
        }

        @Override
        public String expandVar(String word) {
            return word;
        }
    }

    interface ParsedLineStatements extends ParsedLine {
        ParsedStatements statements();
    }
}
