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
package org.neo4j.shell;

import org.neo4j.shell.cli.Format;
import org.neo4j.shell.prettyprint.OutputFormatter;
import org.neo4j.shell.printer.Printer;

public class StringLinePrinter implements Printer {
    private final StringBuilder sb = new StringBuilder();

    @Override
    public void printOut(String line) {
        sb.append(line).append(OutputFormatter.NEWLINE);
    }

    public void clear() {
        sb.setLength(0);
    }

    public String output() {
        return sb.toString();
    }

    @Override
    public void printError(Throwable throwable) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void printError(String text) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Format getFormat() {
        throw new UnsupportedOperationException();
    }

    @Override
    public void setFormat(Format format) {
        throw new UnsupportedOperationException();
    }
}
