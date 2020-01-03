/*
 * Copyright (c) 2002-2020 "Neo4j,"
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

import org.junit.jupiter.api.Test;

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;

import static org.junit.jupiter.api.Assertions.assertEquals;

class ArchiveProgressPrinterTest
{
    @Test
    void progressOutput()
    {
        ByteArrayOutputStream bout = new ByteArrayOutputStream();
        PrintStream printStream = new PrintStream( bout );
        ArchiveProgressPrinter progressPrinter = new ArchiveProgressPrinter( printStream );
        progressPrinter.maxBytes = 1000;
        progressPrinter.maxFiles = 10;

        progressPrinter.beginFile();
        progressPrinter.addBytes( 5 );
        progressPrinter.endFile();
        progressPrinter.beginFile();
        progressPrinter.addBytes( 50 );
        progressPrinter.addBytes( 50 );
        progressPrinter.printOnNextUpdate();
        progressPrinter.addBytes( 100 );
        progressPrinter.endFile();
        progressPrinter.beginFile();
        progressPrinter.printOnNextUpdate();
        progressPrinter.addBytes( 100 );
        progressPrinter.endFile();
        progressPrinter.done();
        progressPrinter.printProgress();

        printStream.flush();
        String output = bout.toString();
        assertEquals( output,
                "\nFiles: 1/10, data:  0.5%" +
                "\nFiles: 2/10, data: 20.5%" +
                "\nFiles: 2/10, data: 20.5%" +
                "\nFiles: 3/10, data: 30.5%" +
                "\nFiles: 3/10, data: 30.5%" +
                "\nDone: 3 files, 305B processed." +
                        System.lineSeparator()
        );
    }
}
