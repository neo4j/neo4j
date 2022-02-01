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
package org.neo4j.shell.commands;

import java.util.List;

import org.neo4j.shell.exception.CommandException;
import org.neo4j.shell.exception.ExitException;
import org.neo4j.shell.parameter.ParameterService;

import static org.neo4j.shell.printer.AnsiFormattedText.from;

/**
 * This command sets a variable to a name, for use as query parameter.
 */
public record Param( ParameterService parameters ) implements Command
{
    @Override
    public void execute( List<String> args ) throws ExitException, CommandException
    {
        requireArgumentCount( args, 1 );
        try
        {
            var parsed = parameters.parse( args.get( 0 ) );
            parameters.setParameter( parameters.evaluate( parsed ) );
        }
        catch ( ParameterService.ParameterParsingException e )
        {
            throw new CommandException( from( "Incorrect usage.\nusage: " ).bold( metadata().name() ).append( " " ).append( metadata().usage() ) );
        }
    }

    public static class Factory implements Command.Factory
    {
        @Override
        public Metadata metadata()
        {
            var help = "Set the specified query parameter to the value given";
            var usage = """
                    name => <Cypher Expression>

                    For example:
                        :param name => 42
                        :param name => 'string value'
                        :param name => { mapKey: 'map value' }
                        :param name => [ 1, 2, 3 ]
                    """;
            return new Metadata( ":param", "Set the value of a query parameter", usage, help, List.of() );
        }

        @Override
        public Command executor( Arguments args )
        {
            return new Param( args.parameters() );
        }
    }
}
