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

import org.neo4j.shell.parameter.ParameterService;
import org.neo4j.shell.prettyprint.PrettyPrinter;
import org.neo4j.shell.printer.Printer;
import org.neo4j.shell.state.BoltStateHandler;

/**
 * This class initializes a {@link CypherShell} with a fake {@link org.neo4j.shell.state.BoltStateHandler} which allows for faked sessions and faked results to
 * test some basic shell functionality without requiring a full integration test.
 */
public class OfflineTestShell extends CypherShell {

    public OfflineTestShell(Printer printer, BoltStateHandler boltStateHandler, PrettyPrinter prettyPrinter) {
        super(printer, boltStateHandler, prettyPrinter, ParameterService.create(boltStateHandler));
    }

    @Override
    protected void addRuntimeHookToResetShell() {
        // Do Nothing
    }
}
