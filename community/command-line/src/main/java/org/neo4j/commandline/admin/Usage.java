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

import org.neo4j.commandline.arguments.Arguments;

import static java.lang.String.format;

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
        output.accept( format( "usage: %s <command>", scriptName ) );
        output.accept( "" );
        output.accept( "available commands:" );

        for ( AdminCommand.Provider command : commands.getAllProviders() )
        {
            final CommandUsage commandUsage = new CommandUsage( command, scriptName );
            commandUsage.printIndentedSummary( output );
        }

        output.accept( "" );
        output.accept( format( "Use %s help <command> for more details.", scriptName ) );
    }

    public void printUsageForCommand( AdminCommand.Provider command, Consumer<String> output )
    {
        final CommandUsage commandUsage = new CommandUsage( command, scriptName );
        commandUsage.printDetailed( output );
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

        public void printSummary( Consumer<String> output )
        {
            output.accept( format( "%s", command.name() ) );
            output.accept( "    " + command.summary() );
        }

        public void printIndentedSummary( Consumer<String> output )
        {
            printSummary( s -> output.accept( "    " + s ) );
        }

        public void printDetailed( Consumer<String> output )
        {
            for (Arguments arguments: command.possibleArguments())
            {
                //Arguments arguments = command.arguments();

                String left = format( "usage: %s %s", scriptName, command.name() );

                output.accept( Arguments.rightColumnFormatted( left, arguments.usage(), left.length() + 1 ) );
            }
            output.accept( "" );
            output.accept( command.allArguments().description( command.description() ) );
        }
    }
}
