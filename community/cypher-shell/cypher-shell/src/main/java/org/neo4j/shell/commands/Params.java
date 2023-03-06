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

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import org.neo4j.shell.exception.CommandException;
import org.neo4j.shell.exception.ExitException;
import org.neo4j.shell.log.Logger;
import org.neo4j.shell.parameter.ParameterService;
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
    private final ParameterService parameters;

    public Params( Logger logger, ParameterService parameters )
    {
        this.logger = logger;
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
        return "Print all currently set query parameters and their values";
    }

    @Override
    public String getUsage()
    {
        return "[parameter]";
    }

    @Override
    public String getHelp()
    {
        return "Print a table of all currently set query parameters or the value for the given parameter";
    }

    @Override
    public List<String> getAliases()
    {
        return Arrays.asList( ":parameters" );
    }

    @Override
    public void execute( final String argString ) throws ExitException, CommandException
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

    private void listParam( String name ) throws CommandException
    {
        String parameterName = CypherVariablesFormatter.unescapedCypherVariable( name );
        ParameterService.Parameter param = parameters.parameters().get( parameterName );
        if ( param == null )
        {
            throw new CommandException( "Unknown parameter: " + name );
        }
        listParam( name.length(), name, param.expressionString );
    }

    private void listParam( int leftColWidth, String key, Object value )
    {
        logger.printOut( String.format( ":param %-" + leftColWidth + "s => %s", key, value ) );
    }

    private void listAllParams()
    {
        final List<Map.Entry<String,ParameterService.Parameter>> sortedParams = parameters.parameters().entrySet().stream()
                                                                                          .sorted( Map.Entry.comparingByKey())
                                                                                          .map( e -> Map.entry( escape( e.getKey() ), e.getValue() ) )
                                                                                          .collect( Collectors.toList());
        int width = sortedParams.stream().map( e -> e.getKey().length() ).reduce( 0, Math::max );
        sortedParams.forEach( e -> listParam( width, e.getKey(), e.getValue().expressionString ) );
    }
}
