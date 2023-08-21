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
package org.neo4j.server.startup;

import org.neo4j.cli.AbstractCommand;
import org.neo4j.cli.ExecutionContext;
import picocli.CommandLine.Command;
import picocli.CommandLine.Option;

@Command(name = "console", description = "Start server in console.")
public class ConsoleCommand extends AbstractCommand {

    @Option(names = "--dry-run", hidden = true, description = "Print (only) the command line instead of executing it")
    boolean dryRun; // Note that this is a hidden "unsupported" argument, not intended for usage outside official

    public ConsoleCommand(ExecutionContext ctx) {
        super(ctx);
    }

    @Override
    protected void execute() throws Exception {
        var enhancedCtx = EnhancedExecutionContext.unwrapFromExecutionContext(ctx);
        try (var bootloader = enhancedCtx.createDbmsBootloader()) {
            bootloader.console(dryRun);
        }
    }
}
