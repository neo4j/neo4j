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
