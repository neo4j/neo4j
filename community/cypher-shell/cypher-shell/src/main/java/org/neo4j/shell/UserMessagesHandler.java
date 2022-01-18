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
package org.neo4j.shell;

import org.neo4j.shell.log.AnsiFormattedText;
import org.neo4j.shell.util.Version;
import org.neo4j.shell.util.Versions;

public record UserMessagesHandler(Connector connector)
{
    public String getWelcomeMessage()
    {
        String neo4j = "Neo4j";
        String protocolVersion = connector.getProtocolVersion();
        if ( !protocolVersion.isEmpty() )
        {
            Version version = Versions.version( protocolVersion );
            neo4j += " using Bolt protocol version " + version.major() + "." + version.minor();
        }
        AnsiFormattedText welcomeMessage = AnsiFormattedText.from( "Connected to " + neo4j + " at " ).bold( connector.driverUrl() );

        if ( !connector.username().isEmpty() )
        {
            welcomeMessage = welcomeMessage.append( " as user " ).bold( connector.username() );
        }

        return welcomeMessage
                .append( ".\nType " ).bold( ":help" )
                .append( " for a list of available commands or " )
                .bold( ":exit" ).append( " to exit the shell." )
                .append( "\nNote that Cypher queries must end with a " ).bold( "semicolon." )
                .formattedString();
    }

    public static String getExitMessage()
    {
        return AnsiFormattedText.s().append( "\nBye!" ).formattedString();
    }
}
