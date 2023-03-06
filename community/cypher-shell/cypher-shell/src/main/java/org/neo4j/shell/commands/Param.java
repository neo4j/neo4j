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

import java.util.Collections;
import java.util.List;

import org.neo4j.shell.exception.CommandException;
import org.neo4j.shell.exception.ExitException;
import org.neo4j.shell.parameter.ParameterService;

import static org.neo4j.shell.log.AnsiFormattedText.from;

/**
 * This command sets a variable to a name, for use as query parameter.
 */
public class Param implements Command
{
    private static final String COMMAND_NAME = ":param";
    private final ParameterService parameters;

    public Param( final ParameterService parameters )
    {
        this.parameters = parameters;
    }

    @Override
    public String getName()
    {
        return COMMAND_NAME;
    }

    @Override
    public String getDescription()
    {
        return "Set the value of a query parameter";
    }

    @Override
    public String getUsage()
    {
        return "name => <Cypher Expression>";
    }

    @Override
    public String getHelp()
    {
        return "Set the specified query parameter to the value given";
    }

    @Override
    public List<String> getAliases()
    {
        return Collections.emptyList();
    }

    @Override
    public void execute( String args ) throws ExitException, CommandException
    {
        try
        {
            var parsed = parameters.parse( args );
            parameters.setParameter( parameters.evaluate( parsed ) );
        }
        catch ( ParameterService.ParameterParsingException e )
        {
            throw new CommandException( from( "Incorrect usage.\nusage: " )
                                                .bold( getName() )
                                                .append( " " )
                                                .append( getUsage() ) );
        }
    }
}
