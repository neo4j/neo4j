/*
 * Copyright (c) 2002-2019 "Neo4j,"
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
package org.neo4j.commandline.admin;

import java.util.function.Consumer;

import org.neo4j.commandline.arguments.Arguments;

import static java.lang.String.format;

class CommandUsage
{
    private final AdminCommand.Provider command;
    private final String scriptName;

    CommandUsage( AdminCommand.Provider command, String scriptName )
    {
        this.command = command;
        this.scriptName = scriptName;
    }

    void printDetailed( Consumer<String> output )
    {
        for ( Arguments arguments : command.possibleArguments() )
        {
            String left = format( "usage: %s %s", scriptName, command.name() );

            output.accept( Arguments.rightColumnFormatted( left, arguments.usage(), left.length() + 1 ) );
        }
        output.accept( "" );
        Usage.printEnvironmentVariables( output );
        output.accept( command.allArguments().description( command.description() ) );
    }
}
