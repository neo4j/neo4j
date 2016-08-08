/*
 * Copyright (c) 2002-2016 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
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
package org.neo4j.commandline.admin;

import java.util.function.Consumer;

import static java.lang.String.format;
import static org.neo4j.helpers.Args.splitLongLine;

public class Usage
{
    private final String scriptName;
    private final CommandLocator commands;

    public Usage( String scriptName, CommandLocator commands )
    {
        this.scriptName = scriptName;
        this.commands = commands;
    }

    public void print( Consumer<String> output )
    {
        output.accept( "Usage:" );
        output.accept( "" );

        for ( AdminCommand.Provider command : commands.getAllProviders() )
        {
            final CommandUsage commandUsage = new CommandUsage( command, scriptName );
            commandUsage.print( output );
        }
    }

    public static class CommandUsage
    {
        private final AdminCommand.Provider command;
        private final String scriptName;

        public CommandUsage( AdminCommand.Provider command, String scriptName )
        {
            this.command = command;
            this.scriptName = scriptName;
        }

        public void print( Consumer<String> output )
        {
            String arguments = command.arguments().map( ( s ) -> " " + s ).orElse( "" );
            output.accept( format( "%s %s%s", scriptName, command.name(), arguments ) );
            output.accept( "" );
            for ( String line : splitLongLine( command.description(), 80 ) )
            {
                output.accept( "    " + line );
            }
            output.accept( "" );
        }
    }
}
