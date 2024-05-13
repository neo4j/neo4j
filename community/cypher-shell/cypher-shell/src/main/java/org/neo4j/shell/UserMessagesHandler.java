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

import static org.neo4j.shell.util.Versions.version;

import org.neo4j.shell.printer.AnsiFormattedText;

public record UserMessagesHandler(Connector connector) {
    public String getWelcomeMessage() {
        final var message = AnsiFormattedText.from("Connected to Neo4j");

        String protocolVersion = connector.getProtocolVersion();
        if (!protocolVersion.isEmpty()) {
            message.append(
                    " using Bolt protocol version " + version(protocolVersion).majorMinorString());
        }

        message.append(" at ").bold(connector.connectionConfig().uri().toString());

        if (!connector.username().isEmpty()) {
            message.append(" as user ").bold(connector.username());
        }

        connector.impersonatedUser().ifPresent(impersonated -> message.orange(" impersonating ")
                .bold(impersonated));

        return message.append(".\nType ")
                .bold(":help")
                .append(" for a list of available commands or ")
                .bold(":exit")
                .append(" to exit the shell.")
                .append("\nNote that Cypher queries must end with a ")
                .bold("semicolon.")
                .resetAndRender();
    }

    public static String getExitMessage() {
        return "\nBye!";
    }
}
