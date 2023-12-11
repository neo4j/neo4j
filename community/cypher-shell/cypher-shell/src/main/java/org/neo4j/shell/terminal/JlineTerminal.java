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
import java.nio.file.FileAlreadyExistsException;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.attribute.PosixFilePermissions;
import java.util.List;
import java.util.function.Supplier;
import org.jline.reader.Expander;
import org.jline.reader.History;
import org.jline.reader.LineReader;
import org.jline.reader.ParsedLine;
import org.jline.terminal.Terminal;
import org.neo4j.shell.Historian;
import org.neo4j.shell.exception.NoMoreInputException;
import org.neo4j.shell.exception.UserInterruptException;
import org.neo4j.shell.log.Logger;
import org.neo4j.shell.parser.StatementParser.ParsedStatements;
import org.neo4j.shell.printer.AnsiFormattedText;
import org.neo4j.shell.printer.Printer;

/**
 * CypherShellTerminal backed by jline.
 */
public class JlineTerminal implements CypherShellTerminal {
    private static final Logger log = Logger.create();
    static final String NO_CONTINUATION_PROMPT_PATTERN = "  ";

    private final LineReader jLineReader;
    private final Printer printer;
    private final Reader reader;
    private final Writer writer;
    private final boolean isInteractive;
    private final Supplier<SimplePrompt> simplePromptSupplier;

    public JlineTerminal(
            LineReader jLineReader,
            boolean isInteractive,
            Printer printer,
            Supplier<SimplePrompt> simplePromptSupplier) {
        assert jLineReader.getParser() instanceof StatementJlineParser;
        this.jLineReader = jLineReader;
        this.printer = printer;
        this.isInteractive = isInteractive;
        this.simplePromptSupplier = simplePromptSupplier;
        this.reader = new JLineReader();
        this.writer = new JLineWriter();
    }

    private StatementJlineParser getParser() {
        return (StatementJlineParser) jLineReader.getParser();
    }

    @Override
    public Reader read() {
        jLineReader.getTerminal().resume();
        return reader;
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
        private String readLine(String prompt, Character mask) throws NoMoreInputException, UserInterruptException {
            try {
                return jLineReader.readLine(prompt, mask);
            } catch (org.jline.reader.EndOfFileException e) {
                throw new NoMoreInputException();
            } catch (org.jline.reader.UserInterruptException e) {
                throw new UserInterruptException(e.getPartialLine());
            }
        }

        @Override
        public ParsedStatements readStatement(AnsiFormattedText prompt)
                throws NoMoreInputException, UserInterruptException {
            getParser().setEnableStatementParsing(true);
            jLineReader.setVariable(LineReader.SECONDARY_PROMPT_PATTERN, continuationPromptPattern(prompt));

            var line = readLine(prompt.renderedString(), null);
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
        public String simplePrompt(String prompt, Character mask) throws NoMoreInputException, UserInterruptException {
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
