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
package org.neo4j.server.startup.provider;

import org.neo4j.server.startup.BootloaderCommand;
import org.neo4j.server.startup.Neo4jCommand;
import picocli.CommandLine;

@CommandLine.Command(
        name = "windows-service",
        description = "Neo4j windows service commands.",
        subcommands = {
            Neo4jCommand.InstallService.class,
            Neo4jCommand.UpdateService.class,
            Neo4jCommand.UninstallService.class
        })
class Neo4jServiceCommand extends BootloaderCommand {
    Neo4jServiceCommand(Neo4jCommand.Neo4jBootloaderContext ctx) {
        super(ctx);
    }
}
