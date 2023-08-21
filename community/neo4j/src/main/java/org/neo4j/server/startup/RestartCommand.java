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
import picocli.CommandLine;

@CommandLine.Command(name = "restart", description = "Restart the server daemon.")
public class RestartCommand extends AbstractCommand {

    @CommandLine.Option(
            names = "--shutdown-timeout",
            description =
                    "A time interval in seconds for how long the command will wait for the DBMS process to stop. The default is "
                            + Bootloader.DEFAULT_NEO4J_SHUTDOWN_TIMEOUT
                            + " seconds. The interval can also be configured using "
                            + Bootloader.ENV_NEO4J_SHUTDOWN_TIMEOUT
                            + " environment variable. If both are present, this option has higher priority than the environment variable.")
    private Integer timeout;

    public RestartCommand(ExecutionContext ctx) {
        super(ctx);
    }

    @Override
    protected void execute() throws Exception {
        var enhancedCtx = EnhancedExecutionContext.unwrapFromExecutionContext(ctx);
        try (var bootloader = enhancedCtx.createDbmsBootloader()) {
            bootloader.restart(timeout);
        }
    }
}
