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
package org.neo4j.shell.parser;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.Reader;
import java.io.StringReader;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Set;
import org.eclipse.collections.api.block.predicate.primitive.CharPredicate;

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
    private static final Set<String> SINGLE_ARGUMENT_COMMANDS = Set.of(":param", ":params");

    @Override
    public ParsedStatements parse(Reader reader) throws IOException {
        return parse(new PeekingReader(reader));
    }

    @Override
    public ParsedStatements parse(String line) throws IOException {
        return parse(new StringReader(line));
    }

    private static ParsedStatements parse(PeekingReader reader) throws IOException {
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
     * Parses a single cypher shell command statement from the specified reader.
     */
    private static ParsedStatement parseCommand(PeekingReader reader) throws IOException {
        int startOffset = reader.offset();
        var line = reader.readWhile(c -> c != '\n' && c != '\r');
        int endOffset = reader.offset() - 1;
        assert line.startsWith(":");

        var parts = stripTrailingSemicolons(line).split("\\s+");
        var name = parts[0];

        var nextChar = reader.peek();
        var isComplete = nextChar == '\n' || nextChar == '\r';

        if (SINGLE_ARGUMENT_COMMANDS.contains(name)) {
            // We don't fully parse the commands (yet), but need some special handling for these cases
            var arg = line.substring(name.length()).trim();
            return new CommandStatement(
                    name, arg.isEmpty() ? List.of() : List.of(arg), isComplete, startOffset, endOffset);
        }

        return new CommandStatement(name, Arrays.stream(parts).skip(1).toList(), isComplete, startOffset, endOffset);
    }

    private static String stripTrailingSemicolons(String input) {
        int i = input.length() - 1;
        while (i >= 0 && input.charAt(i) == ';') {
            --i;
        }
        return input.substring(0, i + 1);
    }

    /*
     * Parses a single cypher statement from the specified reader.
     */
    private static ParsedStatement parseCypher(PeekingReader reader) throws IOException {
        int startOffset = reader.offset();
        String awaitedRightDelimiter = null;
        StringBuilder statement = new StringBuilder();
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
            else if (current == ';') {
                return new CypherStatement(statement.toString(), true, startOffset, reader.offset() - 1);
            } else {
                // If it's the start of a quote or comment
                awaitedRightDelimiter = getRightDelimiter(previous, current);
            }
        }

        // No more input
        return new CypherStatement(statement.toString(), false, startOffset, reader.offset() - 1);
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

        public String readWhile(CharPredicate predicate) throws IOException {
            var line = new StringBuilder();
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
