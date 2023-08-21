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

import org.neo4j.cli.AdminTool;
import org.neo4j.cli.ExecutionContext;
import picocli.CommandLine;

@CommandLine.Command(name = "Neo4j", description = "Neo4j database server CLI.")
public class Neo4jCommand extends Neo4jAdminCommand {

    public Neo4jCommand(Environment environment) {
        super(AdminTool.Neo4jAlias.class, environment);
    }

    @Override
    protected CommandLine getActualAdminCommand(ExecutionContext executionContext) {
        return AdminTool.Neo4jAlias.getCommandLine(executionContext);
    }

    public static void main(String[] args) {
        var environment = Environment.SYSTEM;
        int exitCode = Neo4jCommand.asCommandLine(new Neo4jCommand(environment), environment)
                .execute(args);
        System.exit(exitCode);
    }
}
