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
package org.neo4j.dbms.archive.printer;

import static java.util.Objects.requireNonNull;

import java.io.PrintStream;
import org.neo4j.logging.InternalLog;
import org.neo4j.logging.NullLog;

public class ProgressPrinters {
    private ProgressPrinters() {}

    public static OutputProgressPrinter printStreamPrinter(PrintStream printStream) {
        requireNonNull(printStream);
        return new PrintStreamOutputProgressPrinter(printStream);
    }

    public static OutputProgressPrinter emptyPrinter() {
        return EmptyOutputProgressPrinter.EMPTY_PROGRESS_PRINTER;
    }

    public static OutputProgressPrinter logProviderPrinter(InternalLog log) {
        requireNonNull(log);
        if (log instanceof NullLog) {
            return emptyPrinter();
        }
        return new LogOutputProgressPrinter(log);
    }

    private static class PrintStreamOutputProgressPrinter implements OutputProgressPrinter {

        private final PrintStream printStream;
        private final boolean interactive;

        PrintStreamOutputProgressPrinter(PrintStream printStream) {
            this.printStream = printStream;
            this.interactive = System.console() != null;
        }

        @Override
        public void print(String message) {
            printStream.print(lineSeparator() + message);
        }

        @Override
        public void complete() {
            printStream.print(System.lineSeparator());
        }

        private char lineSeparator() {
            return interactive ? '\r' : '\n';
        }
    }

    private static class LogOutputProgressPrinter implements OutputProgressPrinter {

        private final InternalLog log;

        LogOutputProgressPrinter(InternalLog log) {
            this.log = log;
        }

        @Override
        public void print(String message) {
            log.info(message);
        }
    }

    public static final class EmptyOutputProgressPrinter implements OutputProgressPrinter {

        static EmptyOutputProgressPrinter EMPTY_PROGRESS_PRINTER = new EmptyOutputProgressPrinter();

        private EmptyOutputProgressPrinter() {}

        @Override
        public void print(String message) {}
    }
}
