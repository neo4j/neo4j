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
package org.neo4j.cypher.internal.cst.factory.neo4j;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.nio.charset.StandardCharsets;
import org.junit.jupiter.api.Test;

public class CypherInputStreamTest {
    private void checkBytes(String c, CypherInputStream x) {
        for (byte b : c.getBytes(StandardCharsets.UTF_8)) {
            assertEquals(b, x.read());
        }
    }

    @Test
    void basicHappyPath() {
        CypherInputStream x = new CypherInputStream("abc d  \nö\r\n\t");

        checkBytes("a", x);
        checkBytes("b", x);
        checkBytes("c", x);
        checkBytes(" ", x);
        checkBytes("d", x);
        checkBytes(" ", x);
        checkBytes(" ", x);
        checkBytes("\n", x);
        checkBytes("ö", x);
        checkBytes("\r", x);
        checkBytes("\n", x);
        checkBytes("\t", x);
    }

    @Test
    void returnMinusOneOnEOF() {
        CypherInputStream x = new CypherInputStream("");
        assertEquals(-1, x.read());
    }

    @Test
    void convertEscapedUnicode() {
        // java allows multiple u's in encoded unicode characters
        // see: The Java Language Specification
        //      James Gosling, Bill Joy, Guy Steele, Gilad Bracha
        CypherInputStream x = new CypherInputStream("\\u0024 \u0024 \uuuuu0024 $");

        checkBytes("$", x);
        checkBytes(" ", x);
        checkBytes("$", x);
        checkBytes(" ", x);
        checkBytes("$", x);
        checkBytes(" ", x);
        checkBytes("$", x);
    }

    @Test
    void escapeBackslash() {
        CypherInputStream x = new CypherInputStream("\\\\ \\a \\\\");

        checkBytes("\\", x);
        checkBytes("\\", x);
        checkBytes(" ", x);
        checkBytes("\\", x);
        checkBytes("a", x);
        checkBytes(" ", x);
        checkBytes("\\", x);
        checkBytes("\\", x);
    }

    @Test
    void convertEscapedBackslashUnicodeMix() {
        /* Cypher admits both escaped unicodes and unescaped unicodes:

        ```
        MATCH \u0020  (n)  -> valid unicode
        MATCH u0020   (n)  -> valid unicode
        MATCH \\u0020 (n)  -> not unicode, it interprets escaped \ in Cypher
        ```

        To understand this test it's easier if you mentally separate
        the chars and group double `\` (escaped backslash in Java):

          * `\\\\uaa3a` ~ `\\ \\ uaa3a` would be `\\ uaa3a`,
             it wouldn't be unicode for cypher.

          * `\\\\\\uaa3a` ~ `\\ \\ \\ uaa3a` would be `\\ \uaa3a`,
             so the first `\` has been escaped, but the second one makes the unicode work.

          * `\\\\\\\\uaa3a` ~ `\\ \\ \\ \\ uaa3a` would be `\\ \\ uaa3a`,
             it wouldn't be unicode again

          * etc
        */
        CypherInputStream x = new CypherInputStream("\\\\uaa3a \\\\\\uaa3a \\\\\\\\uaa3a \\\\\\\\\uaa3a");
        checkBytes("\\\\uaa3a \\\\\uaa3a \\\\\\\\uaa3a \\\\\\\\\uaa3a", x);
    }

    @Test
    void acceptConsecutiveUnicode() {
        CypherInputStream x = new CypherInputStream("\uaa3a\uaa3a");
        checkBytes("\uaa3a\uaa3a", x);
    }
}
