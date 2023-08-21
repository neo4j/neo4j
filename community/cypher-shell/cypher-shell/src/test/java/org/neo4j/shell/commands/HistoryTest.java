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
package org.neo4j.shell.commands;

import static java.lang.String.format;
import static java.util.stream.Collectors.toList;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.stream.IntStream;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.neo4j.shell.Historian;
import org.neo4j.shell.exception.CommandException;
import org.neo4j.shell.printer.Printer;

class HistoryTest {
    private Printer printer = mock(Printer.class);
    private Historian historian = mock(Historian.class);
    private Command cmd = new History(printer, historian);

    @BeforeEach
    void setup() {
        printer = mock(Printer.class);
        historian = mock(Historian.class);
        cmd = new History(printer, historian);
    }

    @Test
    void shouldNotAcceptSomeArgs() {
        assertThatThrownBy(() -> cmd.execute(List.of("bob")))
                .isInstanceOf(CommandException.class)
                .hasMessageContaining("Unrecognised argument bob");
    }

    @Test
    void shouldPrintHistoryCorrectlyNumberedFrom1() throws CommandException, IOException {
        when(historian.getHistory()).thenReturn(List.of(":help", ":exit"));

        cmd.execute(List.of());

        verify(printer).printOut(eq(format(" 1  :help%n 2  :exit%n")));
        verify(historian, times(0)).clear();
    }

    @Test
    void shouldHandleMultiLine() throws CommandException {
        when(historian.getHistory()).thenReturn(Arrays.asList("match\n(n)\nreturn\nn\n;", ":exit"));

        cmd.execute(List.of());

        var expected = format(" 1  match%n" + "    (n)%n" + "    return%n" + "    n%n" + "    ;%n" + " 2  :exit%n");

        verify(printer).printOut(eq(expected));
    }

    @Test
    void shouldHandleMultiLineCRNL() throws CommandException {
        when(historian.getHistory()).thenReturn(Arrays.asList("match\r\n(n)\r\nreturn\r\nn\r\n;", ":exit"));

        cmd.execute(List.of());

        var expected = format(" 1  match%n" + "    (n)%n" + "    return%n" + "    n%n" + "    ;%n" + " 2  :exit%n");

        verify(printer).printOut(eq(expected));
    }

    @Test
    void shouldClearHistory() throws CommandException, IOException {
        cmd.execute(List.of("clear"));

        verify(historian, times(1)).clear();
        verify(printer).printIfVerbose(eq("Removing history..."));
    }

    @Test
    void shouldLimitHistory() throws CommandException {
        when(historian.getHistory())
                .thenReturn(IntStream.range(1, 21).mapToObj(Integer::toString).collect(toList()));

        cmd.execute(List.of());

        var expected = format(" 5   5%n" + " 6   6%n"
                + " 7   7%n"
                + " 8   8%n"
                + " 9   9%n"
                + " 10  10%n"
                + " 11  11%n"
                + " 12  12%n"
                + " 13  13%n"
                + " 14  14%n"
                + " 15  15%n"
                + " 16  16%n"
                + " 17  17%n"
                + " 18  18%n"
                + " 19  19%n"
                + " 20  20%n");
        verify(printer).printOut(eq(expected));
    }
}
