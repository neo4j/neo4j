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

import java.util.NoSuchElementException;
import java.util.function.Consumer;

import static java.lang.String.format;

public class HelpCommand implements AdminCommand
{

    private final Usage usage;
    private final Consumer<String> output;
    private final CommandLocator locator;

    HelpCommand( Usage usage, Consumer<String> output, CommandLocator locator )
    {
        this.usage = usage;
        this.output = output;
        this.locator = locator;
    }

    @Override
    public void execute( String... args ) throws IncorrectUsage
    {
        if ( args.length > 0 )
        {
            try
            {
                AdminCommand.Provider commandProvider = this.locator.findProvider( args[0] );
                usage.printUsageForCommand( commandProvider, output );
            }
            catch ( NoSuchElementException e )
            {
                StringBuilder validCommands = new StringBuilder( "" );
                locator.getAllProviders()
                        .forEach( commandProvider -> validCommands.append( commandProvider.name() ).append( " " ) );

                throw new IncorrectUsage(
                        format( "Unknown command: %s. Available commands are: %s\n", args[0], validCommands ) );
            }
        }
        else
        {
            usage.print( output );
        }
    }
}
