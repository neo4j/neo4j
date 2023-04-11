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

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.junit.jupiter.api.Assertions.assertEquals;

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.neo4j.driver.exceptions.ClientException;
import org.neo4j.shell.cli.Format;
import org.neo4j.shell.exception.CommandException;

class AnsiPrinterTest {
    private final ByteArrayOutputStream out = new ByteArrayOutputStream();
    private final ByteArrayOutputStream err = new ByteArrayOutputStream();
    private AnsiPrinter printer;

    @BeforeEach
    void setup() {
        out.reset();
        err.reset();
        this.printer = new AnsiPrinter(Format.VERBOSE, new PrintStream(out), new PrintStream(err), true);
    }

    @Test
    void printError() {
        printer.printError("bob");
        assertOutput("", """
                        [31mbob[m
                        """);
    }

    @Test
    void printException() {
        printer.printError(new Throwable("bam"));
        assertOutput("", """
                        [31mbam[m
                        """);
    }

    @Test
    void printOut() {
        printer.printOut("sob");
        assertOutput("""
                        sob
                        """);
    }

    @Test
    void printOutManyShouldNotBuildState() {
        printer.printOut("bob");
        printer.printOut("nob");
        printer.printOut("cod");

        assertOutput(
                """
                        bob
                        nob
                        cod
                        """);
    }

    @Test
    void printErrManyShouldNotBuildState() {
        printer.printError("bob");
        printer.printError("nob");
        printer.printError("cod");

        assertOutput(
                "",
                """
                        [31mbob[m
                        [31mnob[m
                        [31mcod[m
                        """);
    }

    @Test
    void printIfVerbose() {
        printer.printIfVerbose("foo");
        printer.printIfPlain("bar");

        assertOutput("""
                        foo
                        """);
    }

    @Test
    void printIfPlain() {
        printer = new AnsiPrinter(Format.PLAIN, new PrintStream(out), new PrintStream(err));

        printer.printIfVerbose("foo");
        printer.printIfPlain("bar");

        assertOutput("""
                        bar
                        """);
    }

    @Test
    void testSimple() {
        printer.printError(new NullPointerException("yahoo"));
        assertOutput("", """
                        [31myahoo[m
                        """);
    }

    @Test
    void testNested() {
        printer.printError(new ClientException("outer", new CommandException("nested")));
        assertOutput("", """
                        [31mouter[m
                        """);
    }

    @Test
    void testNestedDeep() {
        var e = new ClientException("outer", new ClientException("nested", new ClientException("nested deep")));
        printer.printError(e);
        assertOutput("", """
                        [31mouter[m
                        """);
    }

    @Test
    void testNullMessage() {
        printer.printError(new ClientException(null));
        printer.printError(new ClientException("outer", new NullPointerException(null)));
        assertOutput(
                "",
                """
                        [31mClientException[m
                        [31mouter[m
                        """);
    }

    private void assertOutput(String expected) {
        assertOutput(expected, "");
    }

    private void assertOutput(String expectedOut, String expectedErr) {
        assertEquals(expectedOut, out.toString(UTF_8).replace("\r", ""));
        assertEquals(expectedErr, err.toString());
    }
}
