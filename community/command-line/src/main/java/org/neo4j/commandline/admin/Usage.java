/*
 * Copyright (c) 2002-2018 "Neo Technology,"
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

import static java.lang.String.format;
import static org.neo4j.helpers.Args.splitLongLine;

public class Usage
{
    private final String scriptName;
    private final Output out;
    private final CommandLocator commands;
    private final String extraHelp;

    public Usage( String scriptName, Output out, CommandLocator commands, String extraHelp )
    {
        this.scriptName = scriptName;
        this.out = out;
        this.commands = commands;
        this.extraHelp = extraHelp;
    }

    public void print()
    {
        out.line( "Usage:" );
        out.line( "" );

        for ( AdminCommand.Provider command : commands.getAllProviders() )
        {
            new CommandUsage( command, out, scriptName ).print();
        }

        out.line( extraHelp );
        out.line( "" );
    }

    public static class CommandUsage
    {
        private final AdminCommand.Provider command;
        private final Output out;
        private final String scriptName;

        public CommandUsage( AdminCommand.Provider command, Output out, String scriptName )
        {
            this.command = command;
            this.out = out;
            this.scriptName = scriptName;
        }

        public void print()
        {
            String arguments = command.arguments().map( ( s ) -> " " + s ).orElse( "" );
            out.line( format( "%s %s%s", scriptName, command.name(), arguments ) );
            out.line( "" );
            for ( String line : splitLongLine( command.description(), 80 ) )
            {
                out.line( "    " + line );
            }
            out.line( "" );
        }
    }
}
