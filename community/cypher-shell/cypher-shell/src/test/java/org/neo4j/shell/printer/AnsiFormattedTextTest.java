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
package org.neo4j.shell.printer;

import static org.junit.jupiter.api.Assertions.assertEquals;

import org.fusesource.jansi.Ansi;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class AnsiFormattedTextTest {

    @BeforeEach
    void setup() {
        Ansi.setEnabled(true);
    }

    @AfterEach
    void cleanup() {
        Ansi.setEnabled(Ansi.isDetected());
    }

    @Test
    void simpleString() {
        AnsiFormattedText st = AnsiFormattedText.from("hello");
        assertEquals("hello", st.plainString());
        assertEquals("hello", st.resetAndRender());
    }

    @Test
    void noStyleShouldBePlain() {
        AnsiFormattedText st = AnsiFormattedText.s().colorDefault().boldOff().append("yo");

        assertEquals("yo", st.plainString());
        assertEquals("\u001B[39;22myo\u001B[m", st.resetAndRender());
    }

    @Test
    void withFormatting() {
        AnsiFormattedText st = AnsiFormattedText.s()
                .colorRed()
                .bold("hello")
                .colorDefault()
                .brightRed()
                .append(" hello")
                .colorDefault()
                .append(" world");

        assertEquals("hello hello world", st.plainString());
        assertEquals("\u001B[31;1mhello\u001B[22;39;91m hello\u001B[39m world\u001B[m", st.resetAndRender());
    }

    @Test
    void nestedFormattingWorks() {
        AnsiFormattedText st = AnsiFormattedText.s()
                .colorDefault()
                .bold()
                .append("hello")
                .boldOff()
                .append(" world");
        st = AnsiFormattedText.s().colorRed().append(st);

        assertEquals("hello world", st.plainString());
        assertEquals("\u001B[31m\u001B[39;1mhello\u001B[22m world\u001B[m", st.resetAndRender());
    }

    @Test
    void outerAttributeTakesColorPrecedence() {
        AnsiFormattedText st = AnsiFormattedText.s().brightRed().append("inner");

        assertEquals("\u001B[91minner\u001B[m", st.resetAndRender());

        st = AnsiFormattedText.s().colorDefault().append(st);

        assertEquals("inner", st.plainString());
        assertEquals("\u001B[39m\u001B[91minner\u001B[m", st.resetAndRender());
    }

    @Test
    void outerAttributeTakesBoldPrecedence() {
        AnsiFormattedText st = AnsiFormattedText.s().brightRed().bold().append("inner");

        assertEquals("\u001B[91;1minner\u001B[m", st.resetAndRender());

        st = AnsiFormattedText.s().boldOff().append(st);

        assertEquals("\u001B[22m\u001B[91;1minner\u001B[m", st.resetAndRender());
    }

    @Test
    void shouldAppend() {
        AnsiFormattedText st = AnsiFormattedText.from("hello");

        st = st.append(" world");

        assertEquals("hello world", st.plainString());
    }
}
