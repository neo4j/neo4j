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

import static java.nio.charset.StandardCharsets.ISO_8859_1;
import static java.nio.charset.StandardCharsets.UTF_8;
import static org.assertj.core.api.Assertions.assertThat;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.io.PrintWriter;
import java.nio.charset.Charset;
import java.util.List;
import org.junit.jupiter.api.Test;

class WeakPromptTest {
    @Test
    void testReadLine() throws IOException {
        for (var charset : List.of(UTF_8, ISO_8859_1)) {
            assertRead("one line\n", charset, "one line");
            assertRead("one line\r", charset, "one line");
            assertRead("one line\r\n", charset, "one line");
            assertRead("one line\nand more", charset, "one line");
            assertRead("one line with tab \t\n", charset, "one line with tab \t");
            assertRead("\n", charset, "");
            assertRead("åäö\n", charset, "åäö");
            assertRead("incomplete line", charset, null);
            assertRead("", charset, null);
        }
    }

    @Test
    void consumesTheRightAmountOfInput() throws IOException {
        var input = new ByteArrayInputStream("Does not compute\n:(\r...\r\n".getBytes(UTF_8));
        var out = new ByteArrayOutputStream();
        var prompt = new WeakPrompt(input, new PrintWriter(out), UTF_8);

        var result = prompt.readLine("Tell me your inner feelings: ");
        assertThat(result).isEqualTo("Does not compute");
        input.mark(1);
        assertThat(input.read()).isEqualTo((int) ':');
        input.reset();

        result = prompt.readLine("Really?");
        assertThat(result).isEqualTo(":(");
        input.mark(1);
        assertThat(input.read()).isEqualTo((int) '.');
        input.reset();

        result = prompt.readLine("Relax!");
        assertThat(result).isEqualTo("...");
        input.mark(1);
        assertThat(input.read()).isEqualTo((int) '\n');
        input.reset();
    }

    @Test
    void testReadPassword() throws IOException {
        for (var charset : List.of(UTF_8, ISO_8859_1)) {
            assertReadPassword("one line\n", charset, "one line");
            assertReadPassword("one line\r", charset, "one line");
            assertReadPassword("one line\r\n", charset, "one line");
            assertReadPassword("one line\nand more", charset, "one line");
            assertReadPassword("one line with tab \t\n", charset, "one line with tab \t");
            assertReadPassword("\n", charset, "");
            assertReadPassword("åäö\n", charset, "åäö");
            assertReadPassword("incomplete line", charset, null);
            assertReadPassword("", charset, null);
        }
    }

    private void assertRead(String input, Charset charset, String expected) throws IOException {
        final var out = new ByteArrayOutputStream();
        assertThat(newWeakPrompt(input, out, charset).readLine("Read me: ")).isEqualTo(expected);
        assertThat(out.toString(charset)).isEqualTo("Read me: ");
    }

    private void assertReadPassword(String input, Charset charset, String expected) throws IOException {
        final var out = new ByteArrayOutputStream();
        assertThat(newWeakPrompt(input, out, charset).readPassword("Read me: ")).isEqualTo(expected);

        if (input.contains("\n") || input.contains("\r")) {
            assertThat(out.toString(charset)).isEqualTo("Read me: " + System.lineSeparator());
        } else {
            assertThat(out.toString(charset)).isEqualTo("Read me: ");
        }
    }

    private WeakPrompt newWeakPrompt(String input, OutputStream out, Charset charset) {
        return new WeakPrompt(new ByteArrayInputStream(input.getBytes(charset)), new PrintWriter(out), charset);
    }
}
