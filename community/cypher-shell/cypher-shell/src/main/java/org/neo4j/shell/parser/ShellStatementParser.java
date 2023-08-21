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
package org.neo4j.shell.parser;

import static java.util.Arrays.asList;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.Reader;
import java.io.StringReader;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Locale;
import org.eclipse.collections.api.block.predicate.primitive.CharPredicate;
import org.neo4j.shell.commands.Param;

/**
 * A cypher aware parser which can detect shell commands (:prefixed) or cypher.
 */
public class ShellStatementParser implements StatementParser {
    private static final char BACKSLASH = '\\';
    private static final String LINE_COMMENT_START = "//";
    private static final String LINE_COMMENT_END = "\n";
    private static final String BLOCK_COMMENT_START = "/*";
    private static final String BLOCK_COMMENT_END = "*/";
    private static final char BACKTICK = '`';
    private static final char DOUBLE_QUOTE = '"';
    private static final char SINGLE_QUOTE = '\'';

    @Override
    public ParsedStatements parse(Reader reader) throws IOException {
        return parse(new PeekingReader(reader));
    }

    @Override
    public ParsedStatements parse(String line) throws IOException {
        return parse(new StringReader(line));
    }

    private ParsedStatements parse(PeekingReader reader) throws IOException {
        var result = new ArrayList<ParsedStatement>();
        int nextValue;

        while ((nextValue = reader.peek()) != -1) {
            if (isWhitespace(nextValue)) {
                reader.read(); // Skip white space at beginning of statement.
            } else if (isAtStartOfComment(nextValue, reader)) {
                skipComments(reader); // Skip comments at beginning of statement.
            } else if (nextValue == ':') {
                result.add(parseCommand(reader));
            } else {
                result.add(parseCypher(reader));
            }
        }

        return new ParsedStatements(result);
    }

    private static boolean isAtStartOfComment(int nextValue, PeekingReader reader) throws IOException {
        return nextValue == '/' && isStartOfComment(reader.peek(2));
    }

    private static boolean isStartOfComment(char[] chars) {
        return switch (String.valueOf(chars)) {
            case LINE_COMMENT_START, BLOCK_COMMENT_START -> true;
            default -> false;
        };
    }

    private static void skipComments(PeekingReader reader) throws IOException {
        String awaitedRightDelimiter = getRightCommentDelimiter(reader.peek(2));

        if (awaitedRightDelimiter == null) {
            return;
        }

        // Discard left delimiter
        reader.read();
        reader.read();
        // Discard comment and right delimiter
        reader.skipUntilAndIncluding(awaitedRightDelimiter.toCharArray());
    }

    /*
     * Hacky handwritten parser for commands.
     *
     * Commands come in these flavours:
     * - No argument commands like :help.
     * - White space separated argument commands like `:connect -u bob -p hej`.
     * - :param with cypher map syntax (`param {a:1}`), allows multi line argument.
     * - :param with arrow syntax (`:param a => 1`), single line, single argument.
     */
    private ParsedStatement parseCommand(PeekingReader reader) throws IOException {
        final int startOffset = reader.offset();

        final var name = reader.readWhile(c -> !isWhitespace(c) && c != ';').toLowerCase(Locale.US);
        assert name.startsWith(":");

        // Skip whitespace after name
        int read;
        do {
            read = reader.read();
        } while (Character.isWhitespace(read) && read != '\n' && read != '\r');

        if (read == '\n' || read == '\r' || read == -1) {
            // No command parameter
            // Note, there might be an extra \n that belongs to the statement,
            // but it does not matter if it's read or not.
            final var endOffset = read != -1 ? reader.offset() - 2 : reader.offset() - 1;
            return new CommandStatement(name, List.of(), true, startOffset, endOffset);
        } else if (Param.NAME.equals(name) || Param.ALIAS.equals(name)) {
            // :param needs some special handling
            final var arg = new StringBuilder(Character.toString(read));
            boolean complete = true;
            if (read == '{') {
                // Cypher map syntax, allows for multiline input
                // Read until the (assumed) end of the map is found
                complete = readCypher(reader, arg, new MapEndPredicate()); // Read until cypher map ends
            }
            reader.readLine(arg); // Read the remainder of the last line (the only line in arrow syntax)
            final var endOffset = reader.offset() - 1;
            return new CommandStatement(name, List.of(normaliseCommandArgs(arg)), complete, startOffset, endOffset);
        } else {
            // All other commands are single line.
            final var argString = normaliseCommandArgs(reader.readLine(new StringBuilder(Character.toString(read))));
            final int endOffset = reader.offset() - 1;
            final var args = argString.isEmpty() ? List.<String>of() : asList(argString.split("\\s+"));
            return new CommandStatement(name, args, true, startOffset, endOffset);
        }
    }

    private String normaliseCommandArgs(StringBuilder input) {
        // Here we remove trailing white space and semicolons.
        // The start of the string is already stripped.
        int i = input.length();
        while (i > 0 && isWhiteSpaceOrSemiColon(input.charAt(i - 1))) {
            --i;
        }
        if (i >= 0) input.setLength(i);
        return input.toString();
    }

    private static boolean isWhiteSpaceOrSemiColon(char c) {
        return c == ';' || Character.isWhitespace(c);
    }

    /*
     * Parses a single cypher statement from the specified reader.
     */
    private static ParsedStatement parseCypher(PeekingReader reader) throws IOException {
        int startOffset = reader.offset();
        StringBuilder statement = new StringBuilder();
        if (readCypher(reader, statement, c -> c == ';')) {
            // remove semicolon
            statement.setLength(statement.length() - 1);
            return new CypherStatement(statement.toString(), true, startOffset, reader.offset() - 2);
        } else {
            return new CypherStatement(statement.toString(), false, startOffset, reader.offset() - 1);
        }
    }

    private static boolean readCypher(PeekingReader reader, StringBuilder statement, CharPredicate endPredicate)
            throws IOException {
        String awaitedRightDelimiter = null;
        int read;
        boolean skipNext = false;
        char current = 0, previous;

        while ((read = reader.read()) != -1) {
            previous = current;
            current = (char) read;

            statement.append(current);

            if (skipNext) {
                // This char is escaped so gets no special treatment
                skipNext = false;
            } else if (inComment(awaitedRightDelimiter)) {
                if (isRightDelimiter(awaitedRightDelimiter, previous, current)) {
                    // No longer in comment
                    awaitedRightDelimiter = null;
                }
            } else if (current == BACKSLASH) {
                // backslash can escape stuff outside of comments (but inside quotes too!)
                skipNext = true;
            } else if (inQuote(awaitedRightDelimiter)) {
                if (isRightDelimiter(awaitedRightDelimiter, previous, current)) {
                    // No longer in quote
                    awaitedRightDelimiter = null;
                }
            }
            // Not escaped, not in a quote, not in a comment
            else if (endPredicate.accept(current)) {
                return true;
            } else {
                // If it's the start of a quote or comment
                awaitedRightDelimiter = getRightDelimiter(previous, current);
            }
        }

        // No more input
        return false;
    }

    private static boolean isWhitespace(int c) {
        return c == ' ' || c == '\n' || c == '\t' || c == '\r';
    }

    /*
     * Returns true if inside a quote, false otherwise
     */
    private static boolean inQuote(String awaitedRightDelimiter) {
        return awaitedRightDelimiter != null && !inComment(awaitedRightDelimiter);
    }

    /*
     * Returns true if the last two chars ends the current awaited delimiter, false otherwise
     */
    private static boolean isRightDelimiter(String awaitedRightDelimiter, char first, char last) {
        if (awaitedRightDelimiter == null) {
            return false;
        } else if (awaitedRightDelimiter.length() == 1) {
            return awaitedRightDelimiter.charAt(0) == last;
        } else if (awaitedRightDelimiter.length() == 2) {
            return awaitedRightDelimiter.charAt(0) == first && awaitedRightDelimiter.charAt(1) == last;
        } else {
            return false;
        }
    }

    private static boolean inComment(String awaitedRightDelimiter) {
        return LINE_COMMENT_END.equals(awaitedRightDelimiter) || BLOCK_COMMENT_END.equals(awaitedRightDelimiter);
    }

    /*
     * If the last characters start a quote or a comment, this returns the piece of text which will end said quote or comment.
     */
    private static String getRightDelimiter(char first, char last) {
        var commentRight = getRightCommentDelimiter(new char[] {first, last});
        return commentRight != null ? commentRight : getRightQuoteDelimiter(last);
    }

    private static String getRightCommentDelimiter(char[] chars) {
        return switch (String.valueOf(chars)) {
            case LINE_COMMENT_START -> LINE_COMMENT_END;
            case BLOCK_COMMENT_START -> BLOCK_COMMENT_END;
            default -> null;
        };
    }

    private static String getRightQuoteDelimiter(char last) {
        return switch (last) {
            case BACKTICK, DOUBLE_QUOTE, SINGLE_QUOTE -> String.valueOf(last);
            default -> null;
        };
    }

    private static class MapEndPredicate implements CharPredicate {
        private int bracketDepth = 1; // Init to 1 because the first { is already read

        @Override
        public boolean accept(char value) {
            if (value == '{') {
                bracketDepth += 1;
            } else if (value == '}') {
                bracketDepth -= 1;
            }
            return bracketDepth <= 0;
        }
    }

    /*
     * Wraps a buffered reader and provides some convenience methods.
     */
    private static class PeekingReader {
        private final BufferedReader reader;
        private int offset;

        PeekingReader(Reader reader) {
            this.reader = reader instanceof BufferedReader buffered ? buffered : new BufferedReader(reader);
        }

        public int read() throws IOException {
            int read = reader.read();
            if (read != -1) {
                ++offset;
            }

            return read;
        }

        public int peek() throws IOException {
            reader.mark(1);
            int value = reader.read();
            reader.reset();
            return value;
        }

        public char[] peek(int size) throws IOException {
            reader.mark(size);
            char[] values = new char[size];
            for (int i = 0; i < size; ++i) {
                int read = reader.read();

                if (read == -1) {
                    reader.reset();
                    return Arrays.copyOf(values, i);
                }

                values[i] = (char) read;
            }
            reader.reset();
            return values;
        }

        public void skipUntilAndIncluding(char[] chars) throws IOException {
            int matches = 0;
            int read;
            while ((read = read()) != -1) {
                if (read == chars[matches]) {
                    ++matches;
                    if (matches == chars.length) {
                        return;
                    }
                } else if (matches != 0) {
                    matches = 0;
                }
            }
        }

        /**
         * Reads until, but not including, the next line break.
         */
        public StringBuilder readLine(StringBuilder builder) throws IOException {
            int peek;
            while ((peek = peek()) != -1 && peek != '\n' && peek != '\r') {
                builder.append((char) read());
            }
            return builder;
        }

        public String readWhile(CharPredicate predicate) throws IOException {
            final var line = new StringBuilder();
            int peek;
            while ((peek = peek()) != -1) {
                if (predicate.accept((char) peek)) {
                    line.append((char) read());
                } else {
                    return line.toString();
                }
            }
            return line.toString();
        }

        public int offset() {
            return offset;
        }
    }
}
