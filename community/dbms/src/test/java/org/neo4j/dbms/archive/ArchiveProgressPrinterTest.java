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
package org.neo4j.dbms.archive;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.neo4j.dbms.archive.printer.ProgressPrinters.printStreamPrinter;

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import org.junit.jupiter.api.Test;
import org.neo4j.dbms.archive.printer.OutputProgressPrinter;
import org.neo4j.dbms.archive.printer.ProgressPrinters;
import org.neo4j.logging.AssertableLogProvider;
import org.neo4j.logging.InternalLog;

class ArchiveProgressPrinterTest {
    @Test
    void printProgressStreamOutput() {
        ByteArrayOutputStream bout = new ByteArrayOutputStream();
        PrintStream printStream = new PrintStream(bout);
        OutputProgressPrinter outputPrinter = printStreamPrinter(printStream);

        executeSomeWork(outputPrinter);

        printStream.flush();
        String output = bout.toString();
        assertEquals(
                output,
                "\nFiles: 1/10, data: " + String.format("%4.1f%%", 0.5) + "\nFiles: 2/10, data: "
                        + String.format("%4.1f%%", 20.5) + "\nFiles: 2/10, data: "
                        + String.format("%4.1f%%", 20.5) + "\nFiles: 3/10, data: "
                        + String.format("%4.1f%%", 30.5) + "\nFiles: 3/10, data: "
                        + String.format("%4.1f%%", 30.5) + "\nDone: 3 files, 305B processed."
                        + System.lineSeparator());
    }

    @Test
    void printProgressEmptyReporter() {
        OutputProgressPrinter outputPrinter = ProgressPrinters.emptyPrinter();
        assertDoesNotThrow(() -> executeSomeWork(outputPrinter));
    }

    @Test
    void printProgressLogger() {
        AssertableLogProvider logProvider = new AssertableLogProvider();
        InternalLog providerLog = logProvider.getLog(ArchiveProgressPrinterTest.class);
        OutputProgressPrinter outputPrinter = ProgressPrinters.logProviderPrinter(providerLog);

        executeSomeWork(outputPrinter);

        assertEquals(
                logProvider.serialize(),
                "INFO @ org.neo4j.dbms.archive.ArchiveProgressPrinterTest: Files: 1/10, data: "
                        + String.format("%4.1f%%", 0.5) + "\n"
                        + "INFO @ org.neo4j.dbms.archive.ArchiveProgressPrinterTest: Files: 2/10, data: "
                        + String.format("%4.1f%%", 20.5) + "\n"
                        + "INFO @ org.neo4j.dbms.archive.ArchiveProgressPrinterTest: Files: 2/10, data: "
                        + String.format("%4.1f%%", 20.5) + "\n"
                        + "INFO @ org.neo4j.dbms.archive.ArchiveProgressPrinterTest: Files: 3/10, data: "
                        + String.format("%4.1f%%", 30.5) + "\n"
                        + "INFO @ org.neo4j.dbms.archive.ArchiveProgressPrinterTest: Files: 3/10, data: "
                        + String.format("%4.1f%%", 30.5) + "\n"
                        + "INFO @ org.neo4j.dbms.archive.ArchiveProgressPrinterTest: Done: 3 files, 305B processed.\n");
    }

    private static void executeSomeWork(OutputProgressPrinter outputPrinter) {
        ArchiveProgressPrinter progressPrinter = new ArchiveProgressPrinter(outputPrinter);
        progressPrinter.maxBytes = 1000;
        progressPrinter.maxFiles = 10;

        progressPrinter.beginFile();
        progressPrinter.addBytes(5);
        progressPrinter.endFile();
        progressPrinter.beginFile();
        progressPrinter.addBytes(50);
        progressPrinter.addBytes(50);
        progressPrinter.printOnNextUpdate();
        progressPrinter.addBytes(100);
        progressPrinter.endFile();
        progressPrinter.beginFile();
        progressPrinter.printOnNextUpdate();
        progressPrinter.addBytes(100);
        progressPrinter.endFile();
        progressPrinter.done();
        progressPrinter.printProgress();
    }
}
