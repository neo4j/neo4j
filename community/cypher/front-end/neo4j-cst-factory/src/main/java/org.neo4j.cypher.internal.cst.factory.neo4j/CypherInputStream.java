/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [https://neo4j.com]
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.neo4j.cypher.internal.cst.factory.neo4j;

import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.nio.CharBuffer;
import java.nio.charset.CharsetEncoder;
import java.nio.charset.StandardCharsets;
import org.neo4j.cypher.internal.parser.javacc.InvalidUnicodeLiteral;

/**
 * CharStream operating over an input String.
 * <p>
 * This class is a stream that unescapes escaped unicode characters
 * <p>
 * <pre>
 * Example
 *      query: "WITH 1 AS x
 *              RETURN '\\u01FF' AS y"
 *     result: W, I, T, H,  , 1,  , A, S,  , x,\n, R, E, T, U, R, N,  , ', ǿ, ',  , A, S,  , y
 *                                                                         ^
 *                                                                         un-escaped unicode
 * </pre>
 * <p>
 * As parsing progresses, the {@link CypherInputStream} will convert more and more
 * of `query` into `result`, while updating `lines`, `columns` and `offset`.
 */
public class CypherInputStream extends InputStream {
    private static final char BACKSLASH = '\\';
    private static final IOException END_OF_INPUT = new IOException("End of input");

    private final String query;
    private int queryCursor = -1;
    private int queryCursorColumn;
    private int queryCursorLine = 1;
    private boolean queryCursorIsCR;
    private boolean queryCursorIsLF;

    private int tabSize = 1;

    CharsetEncoder utf8 = StandardCharsets.UTF_8.newEncoder();
    private CharBuffer charBuffer = CharBuffer.allocate(1);
    private ByteBuffer currentBytes = ByteBuffer.allocate(4);
    private boolean escaped = false;

    public CypherInputStream(String query) {
        this.query = query;
        this.currentBytes.position(4);
    }

    private boolean endOfInput() {
        return queryCursor + 1 >= query.length();
    }

    @Override
    public int read() {
        if (currentBytes.hasRemaining()) {
            return currentBytes.get();
        }
        if (endOfInput()) {
            return -1;
        }
        currentBytes.clear();
        charBuffer.clear();
        charBuffer.put(0, nextChar());
        utf8.encode(charBuffer, currentBytes, false);
        currentBytes.limit(currentBytes.position());
        currentBytes.position(0);
        return currentBytes.get();
    }

    private char nextChar() {
        char c = nextQueryChar();

        if (c == BACKSLASH && !endOfInput() && !escaped) {
            char nextChar = query.charAt(queryCursor + 1);
            if (nextChar == 'u') {
                return convertUnicode(nextChar);
            }

            escaped = true;
            return c;
        }

        escaped = false;
        return c;
    }

    private char nextQueryChar() {
        queryCursor++;
        char c = query.charAt(queryCursor);
        updateLineColumn(c);
        return c;
    }

    private void updateLineColumn(char c) {
        queryCursorColumn++;

        if (queryCursorIsLF) {
            queryCursorIsLF = false;
            queryCursorColumn = 1;
            queryCursorLine++;
        } else if (queryCursorIsCR) {
            queryCursorIsCR = false;
            if (c == '\n') {
                queryCursorIsLF = true;
            } else {
                queryCursorColumn = 1;
                queryCursorLine++;
            }
        }

        switch (c) {
            case '\r' -> queryCursorIsCR = true;
            case '\n' -> queryCursorIsLF = true;
            case '\t' -> {
                queryCursorColumn--;
                queryCursorColumn += tabSize - (queryCursorColumn % tabSize);
            }
            default -> {}
        }
    }

    private char convertUnicode(char c) {
        try {
            while (c == 'u') {
                c = nextQueryChar();
            }

            return (char) (hexval(c) << 12
                    | hexval(nextQueryChar()) << 8
                    | hexval(nextQueryChar()) << 4
                    | hexval(nextQueryChar()));
        } catch (final IOException e) {
            throw new InvalidUnicodeLiteral(e.getMessage(), queryCursor, queryCursorLine, queryCursorColumn);
        }
    }

    private static int hexval(final char c) throws IOException {
        return switch (c) {
            case '0' -> 0;
            case '1' -> 1;
            case '2' -> 2;
            case '3' -> 3;
            case '4' -> 4;
            case '5' -> 5;
            case '6' -> 6;
            case '7' -> 7;
            case '8' -> 8;
            case '9' -> 9;
            case 'a', 'A' -> 10;
            case 'b', 'B' -> 11;
            case 'c', 'C' -> 12;
            case 'd', 'D' -> 13;
            case 'e', 'E' -> 14;
            case 'f', 'F' -> 15;
            default -> throw new IOException(
                    "Invalid input '" + c + "': expected four hexadecimal digits specifying a unicode character");
        };
    }
}
