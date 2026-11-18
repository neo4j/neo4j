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

import java.io.PrintStream;
import org.neo4j.driver.exceptions.ClientException;
import org.neo4j.driver.exceptions.DiscoveryException;
import org.neo4j.driver.exceptions.ServiceUnavailableException;
import org.neo4j.shell.cli.Format;
import org.neo4j.shell.exception.AnsiFormattedException;
import org.neo4j.shell.system.Tty;

/**
 * A basic logger which prints Ansi formatted text to STDOUT and STDERR
 */
public class AnsiPrinter implements Printer {
    private final PrintStream out;
    private final PrintStream err;
    private Format format;

    public AnsiPrinter() {
        this(Format.VERBOSE, System.out, System.err);
    }

    public AnsiPrinter(Format format, PrintStream out, PrintStream err) {
        this.format = format;
        this.out = out;
        this.err = err;

        AnsiFormattedText.setEnabled(isOutputInteractive());
    }

    private static Throwable getRootCause(final Throwable th) {
        Throwable cause = th;
        while (cause.getCause() != null) {
            cause = cause.getCause();
        }
        return cause;
    }

    /**
     * @return true if the shell is outputting to a TTY, false otherwise (e.g., we are writing to a file)
     */
    private static boolean isOutputInteractive() {
        return Tty.isStdoutAndStderrTty();
    }

    @Override
    public Format getFormat() {
        return format;
    }

    @Override
    public void setFormat(Format format) {
        this.format = format;
    }

    @Override
    public void printError(Throwable throwable) {
        printError(getFormattedMessage(throwable));
    }

    @Override
    public void printError(String s) {
        err.println(s);
    }

    @Override
    public void printOut(final String msg) {
        out.println(msg);
    }

    /**
     * Formatting for Bolt exceptions.
     */
    public String getFormattedMessage(final Throwable e) {
        AnsiFormattedText msg = AnsiFormattedText.s().brightRed();

        if (e instanceof AnsiFormattedException ae) {
            msg.append(ae.getFormattedMessage());
        } else if (e instanceof ClientException
                && e.getMessage() != null
                && e.getMessage().contains("Missing username")) {
            // Username and password was not specified
            msg.append(e.getMessage())
                    .append("\nPlease specify --username, and optionally --password, as argument(s)")
                    .append("\nor as environment variable(s), NEO4J_USERNAME, and NEO4J_PASSWORD respectively.")
                    .append("\nSee --help for more info.");
        } else {
            Throwable cause = e;

            // Get the suppressed root cause of ServiceUnavailableExceptions
            if (e instanceof ServiceUnavailableException) {
                Throwable[] suppressed = e.getSuppressed();
                for (Throwable s : suppressed) {
                    if (s instanceof DiscoveryException) {
                        cause = getRootCause(s);
                        break;
                    }
                }
            }

            if (cause.getMessage() != null) {
                msg.append(cause.getMessage());
            } else {
                msg.append(cause.getClass().getSimpleName());
            }
        }

        return msg.resetAndRender();
    }
}
