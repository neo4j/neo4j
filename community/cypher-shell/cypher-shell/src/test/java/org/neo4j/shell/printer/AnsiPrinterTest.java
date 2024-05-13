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
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;

import java.io.PrintStream;
import org.fusesource.jansi.Ansi;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.neo4j.driver.exceptions.ClientException;
import org.neo4j.shell.cli.Format;
import org.neo4j.shell.exception.CommandException;

class AnsiPrinterTest {
    private final PrintStream out = mock(PrintStream.class);
    private final PrintStream err = mock(PrintStream.class);
    private AnsiPrinter logger = new AnsiPrinter(Format.VERBOSE, out, err);

    @BeforeEach
    void setup() {
        Ansi.setEnabled(true);
    }

    @AfterEach
    void cleanup() {
        Ansi.setEnabled(Ansi.isDetected());
    }

    @Test
    void printError() {
        logger.printError("bob");
        verify(err).println("bob");
    }

    @Test
    void printException() {
        logger.printError(new Throwable("bam"));
        verify(err).println("\u001B[91mbam\u001B[m");
    }

    @Test
    void printOut() {
        logger.printOut("sob");
        verify(out).println("sob");
    }

    @Test
    void printOutManyShouldNotBuildState() {
        logger.printOut("bob");
        logger.printOut("nob");
        logger.printOut("cod");

        verify(out).println("bob");
        verify(out).println("nob");
        verify(out).println("cod");
    }

    @Test
    void printErrManyShouldNotBuildState() {
        logger.printError("bob");
        logger.printError("nob");
        logger.printError("cod");

        verify(err).println("bob");
        verify(err).println("nob");
        verify(err).println("cod");
    }

    @Test
    void printIfVerbose() {
        logger = new AnsiPrinter(Format.VERBOSE, out, err);

        logger.printIfVerbose("foo");
        logger.printIfPlain("bar");

        verify(out).println("foo");
        verifyNoMoreInteractions(out);
    }

    @Test
    void printIfPlain() {
        logger = new AnsiPrinter(Format.PLAIN, out, err);

        logger.printIfVerbose("foo");
        logger.printIfPlain("bar");

        verify(out).println("bar");
        verifyNoMoreInteractions(out);
    }

    @Test
    void testSimple() {
        assertEquals("\u001B[91myahoo\u001B[m", logger.getFormattedMessage(new NullPointerException("yahoo")));
    }

    @Test
    void testNested() {
        assertEquals(
                "\u001B[91mouter\u001B[m",
                logger.getFormattedMessage(new ClientException("outer", new CommandException("nested"))));
    }

    @Test
    void testNestedDeep() {
        assertEquals(
                "\u001B[91mouter\u001B[m",
                logger.getFormattedMessage(new ClientException(
                        "outer", new ClientException("nested", new ClientException("nested deep")))));
    }

    @Test
    void testNullMessage() {
        assertEquals("\u001B[91mClientException\u001B[m", logger.getFormattedMessage(new ClientException(null)));
        assertEquals(
                "\u001B[91mouter\u001B[m",
                logger.getFormattedMessage(new ClientException("outer", new NullPointerException(null))));
    }

    @Test
    void testExceptionGetsFormattedMessage() {
        AnsiPrinter logger = spy(this.logger);
        logger.printError(new NullPointerException("yahoo"));
        verify(logger).printError("\u001B[91myahoo\u001B[m");
    }
}
