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
package org.neo4j.shell.terminal;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import org.jline.terminal.Attributes;
import org.jline.terminal.impl.ExecPty;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

class TtyPromptTest {

    @Test
    void readLine() throws IOException {
        assertRead("one line\n", "one line");
        assertRead("one line\r", "one line");
        assertRead("one line\r\n", "one line");
        assertRead("one line\nand more", "one line");
        assertRead("one line with tab \t\n", "one line with tab \t");
        assertRead("\n", "");
        assertRead("åäö\n", "åäö");
        assertRead("incomplete line", null);
        assertRead("", null);
    }

    @Test
    void readPassword() throws IOException {
        assertReadPassword("one line\n", "one line");
        assertReadPassword("one line\r", "one line");
        assertReadPassword("one line\r\n", "one line");
        assertReadPassword("one line\nand more", "one line");
        assertReadPassword("one line with tab \t\n", "one line with tab \t");
        assertReadPassword("\n", "");
        assertReadPassword("åäö\n", "åäö");
        assertReadPassword("incomplete line", null);
        assertReadPassword("", null);
    }

    @Test
    void readLineShouldNotSetAttributes() throws Exception {
        final var out = new ByteArrayOutputStream();
        final var mockPty = mock(ExecPty.class);
        final var attributes = new Attributes();
        attributes.setLocalFlag(Attributes.LocalFlag.ECHONL, true);
        when(mockPty.getAttr()).thenReturn(attributes);
        final var prompt = new TtyPrompt(mockPty, new ByteArrayInputStream("\n".getBytes(UTF_8)), out);
        prompt.readLine(">");

        verify(mockPty, times(0)).setAttr(any());
    }

    @Test
    void readPasswordShouldRestoreAttributes() throws Exception {
        final var out = new ByteArrayOutputStream();
        final var mockPty = mock(ExecPty.class);
        final var attributes = new Attributes();
        attributes.setLocalFlag(Attributes.LocalFlag.ECHONL, true);
        when(mockPty.getAttr()).thenReturn(attributes);
        final var prompt = new TtyPrompt(mockPty, new ByteArrayInputStream("\n".getBytes(UTF_8)), out);
        prompt.readPassword(">");

        ArgumentCaptor<Attributes> attrCaptor = ArgumentCaptor.forClass(Attributes.class);
        verify(mockPty, times(2)).setAttr(attrCaptor.capture());
        final var attrs = attrCaptor.getAllValues();

        final var attrsWithEchoOff = new Attributes(attributes);
        attrsWithEchoOff.setLocalFlag(Attributes.LocalFlag.ECHO, false);
        assertEqualAttributes(attrs.get(0), attrsWithEchoOff);

        assertEqualAttributes(attrs.get(1), attributes);
    }

    private void assertRead(String input, String expected) throws IOException {
        final var out = new ByteArrayOutputStream();
        assertThat(newTtyPrompt(input, out).readLine("Read me: "), is(expected));
        assertThat(out.toString(UTF_8), is("Read me: "));
    }

    private void assertReadPassword(String input, String expected) throws IOException {
        final var out = new ByteArrayOutputStream();
        assertThat(newTtyPrompt(input, out).readPassword("Read me: "), is(expected));

        if (input.contains("\n") || input.contains("\r")) {
            assertThat(out.toString(), is("Read me: " + System.lineSeparator()));
        } else {
            assertThat(out.toString(), is("Read me: "));
        }
    }

    private TtyPrompt newTtyPrompt(String input, OutputStream out) throws IOException {
        final var mockPty = mock(ExecPty.class);
        final var attributes = new Attributes();
        when(mockPty.getAttr()).thenReturn(attributes);
        return new TtyPrompt(mockPty, new ByteArrayInputStream(input.getBytes(UTF_8)), out);
    }

    private void assertEqualAttributes(Attributes attributes, Attributes expected) {
        assertThat(attributes.getControlChars(), is(expected.getControlChars()));
        assertThat(attributes.getControlFlags(), is(expected.getControlFlags()));
        assertThat(attributes.getInputFlags(), is(expected.getInputFlags()));
        assertThat(attributes.getLocalFlags(), is(expected.getLocalFlags()));
        assertThat(attributes.getOutputFlags(), is(expected.getOutputFlags()));
    }
}
