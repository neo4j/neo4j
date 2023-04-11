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
package org.neo4j.shell.printer;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;

import org.fusesource.jansi.Ansi;
import org.junit.jupiter.api.Test;

class AnsiFormattedTextTest {

    @Test
    void simpleString() {
        assertAnsi(AnsiFormattedText.from("hello"), "hello", "hello");
    }

    @Test
    void noStyleShouldBePlain() {
        assertAnsi(AnsiFormattedText.s().colorDefault().boldOff().append("yo"), "yo", "yo");
    }

    @Test
    void withFormatting() {
        assertAnsi(
                AnsiFormattedText.s().colorRed().bold("hello").colorDefault().append(" world"),
                "@|RED,BOLD hello|@ world",
                "\u001B[31;1mhello\u001B[m world");
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
        assertAnsi(st, "@|RED,BOLD hello|@@|RED  world|@", "\u001B[31;1mhello\u001B[m\u001B[31m world\u001B[m");
    }

    @Test
    void outerAttributeTakesColorPrecedence() {
        AnsiFormattedText st = AnsiFormattedText.s().colorRed().append("inner");

        assertAnsi(st, "@|RED inner|@", "\u001B[31minner\u001B[m");

        st = AnsiFormattedText.s().colorDefault().append(st);

        assertAnsi(st, "inner", "inner");
    }

    @Test
    void outerAttributeTakesBoldPrecedence() {
        AnsiFormattedText st = AnsiFormattedText.s().colorRed().bold().append("inner");

        assertAnsi(st, "@|RED,BOLD inner|@", "\u001B[31;1minner\u001B[m");

        st = AnsiFormattedText.s().boldOff().append(st);

        assertAnsi(st, "@|RED inner|@", "\u001B[31minner\u001B[m");
    }

    @Test
    void shouldAppend() {
        AnsiFormattedText st = AnsiFormattedText.from("hello");

        st = st.append(" world");

        assertAnsi(st, "hello world", "hello world");
    }

    /**
     * @param in AnsiFormattedText to test
     * @param jansiString expected jansi string
     * @param ansiString expected string with ansi escape codes
     */
    private void assertAnsi(AnsiFormattedText in, String jansiString, String ansiString) {
        assertEquals(jansiString, in.jansiFormattedString());
        assertEquals(ansiString, in.renderedString());
        assertFalse(in.plainString().contains("\u001B"));
        Ansi.setEnabled(false);
        assertEquals(Ansi.ansi().render(jansiString).toString(), in.plainString());
        Ansi.setEnabled(true);
    }
}
