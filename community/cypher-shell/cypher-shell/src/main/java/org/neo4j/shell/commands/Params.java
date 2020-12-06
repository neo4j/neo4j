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
package org.neo4j.shell.commands;

import java.util.Arrays;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import javax.annotation.Nonnull;

import org.neo4j.shell.ParameterMap;
import org.neo4j.shell.exception.CommandException;
import org.neo4j.shell.exception.ExitException;
import org.neo4j.shell.log.Logger;
import org.neo4j.shell.prettyprint.CypherVariablesFormatter;

import static org.neo4j.shell.commands.CommandHelper.simpleArgParse;
import static org.neo4j.shell.prettyprint.CypherVariablesFormatter.escape;

/**
 * This lists all query parameters which have been set
 */
public class Params implements Command
{
    public static final String COMMAND_NAME = ":params";
    private static final Pattern backtickPattern = Pattern.compile( "^\\s*(?<key>(`([^`])*`)+?)\\s*" );
    private final Logger logger;
    private final ParameterMap parameterMap;

    public Params( @Nonnull Logger logger, @Nonnull ParameterMap parameterMap )
    {
        this.logger = logger;
        this.parameterMap = parameterMap;
    }

    @Nonnull
    @Override
    public String getName()
    {
        return COMMAND_NAME;
    }

    @Nonnull
    @Override
    public String getDescription()
    {
        return "Print all currently set query parameters and their values";
    }

    @Nonnull
    @Override
    public String getUsage()
    {
        return "[parameter]";
    }

    @Nonnull
    @Override
    public String getHelp()
    {
        return "Print a table of all currently set query parameters or the value for the given parameter";
    }

    @Nonnull
    @Override
    public List<String> getAliases()
    {
        return Arrays.asList( ":parameters" );
    }

    @Override
    public void execute( @Nonnull final String argString ) throws ExitException, CommandException
    {
        String trim = argString.trim();
        Matcher matcher = backtickPattern.matcher( trim );
        if ( trim.startsWith( "`" ) && matcher.matches() )
        {
            listParam( trim );
        }
        else
        {
            String[] args = simpleArgParse( argString, 0, 1, COMMAND_NAME, getUsage() );
            if ( args.length > 0 )
            {
                listParam( args[0] );
            }
            else
            {
                listAllParams();
            }
        }
    }

    private void listParam( @Nonnull String name ) throws CommandException
    {
        String parameterName = CypherVariablesFormatter.unescapedCypherVariable( name );
        if ( !this.parameterMap.getAllAsUserInput().containsKey( parameterName ) )
        {
            throw new CommandException( "Unknown parameter: " + name );
        }
        listParam( name.length(), name, this.parameterMap.getAllAsUserInput().get( parameterName ).getValueAsString() );
    }

    private void listParam( int leftColWidth, @Nonnull String key, @Nonnull Object value )
    {
        logger.printOut( String.format( ":param %-" + leftColWidth + "s => %s", key, value ) );
    }

    private void listAllParams()
    {
        List<String> keys = parameterMap.getAllAsUserInput().keySet().stream().sorted().collect( Collectors.toList() );

        int leftColWidth = keys.stream().map( s -> escape( s ).length() ).reduce( 0, Math::max );

        keys.forEach( key -> listParam( leftColWidth, escape( key ), parameterMap.getAllAsUserInput().get( key ).getValueAsString() ) );
    }
}
