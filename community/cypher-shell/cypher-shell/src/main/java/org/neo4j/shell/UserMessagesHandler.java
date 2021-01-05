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
package org.neo4j.shell;

import javax.annotation.Nonnull;

import org.neo4j.shell.commands.Exit;
import org.neo4j.shell.commands.Help;
import org.neo4j.shell.log.AnsiFormattedText;
import org.neo4j.shell.util.Version;
import org.neo4j.shell.util.Versions;

public class UserMessagesHandler
{
    private ConnectionConfig connectionConfig;
    private String serverVersion;

    public UserMessagesHandler( @Nonnull ConnectionConfig connectionConfig, @Nonnull String serverVersion )
    {
        this.connectionConfig = connectionConfig;
        this.serverVersion = serverVersion;
    }

    @Nonnull
    public String getWelcomeMessage()
    {
        String neo4j = "Neo4j";
        if ( !serverVersion.isEmpty() )
        {
            Version version = Versions.version( serverVersion );
            neo4j += " using Bolt protocol version " + version.major() + "." + version.minor();
        }
        AnsiFormattedText welcomeMessage = AnsiFormattedText.from( "Connected to " )
                                                            .append( neo4j )
                                                            .append( " at " )
                                                            .bold().append( connectionConfig.driverUrl() ).boldOff();

        if ( !connectionConfig.username().isEmpty() )
        {
            welcomeMessage = welcomeMessage
                    .append( " as user " )
                    .bold().append( connectionConfig.username() ).boldOff();
        }

        return welcomeMessage
                .append( ".\nType " )
                .bold().append( Help.COMMAND_NAME ).boldOff()
                .append( " for a list of available commands or " )
                .bold().append( Exit.COMMAND_NAME ).boldOff()
                .append( " to exit the shell." )
                .append( "\nNote that Cypher queries must end with a " )
                .bold().append( "semicolon." ).boldOff().formattedString();
    }

    @Nonnull
    public String getExitMessage()
    {
        return AnsiFormattedText.s().append( "\nBye!" ).formattedString();
    }
}
