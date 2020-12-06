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

import java.util.Collections;
import java.util.List;
import java.util.function.BiPredicate;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import javax.annotation.Nonnull;

import org.neo4j.shell.ParameterMap;
import org.neo4j.shell.exception.CommandException;
import org.neo4j.shell.log.AnsiFormattedText;

/**
 * This command sets a variable to a name, for use as query parameter.
 */
public class Param implements Command
{
    public static final String COMMAND_NAME = ":param";
    // Match arguments such as "(key) (value with possible spaces)" where key and value are any strings
    private static final Pattern backtickPattern = Pattern.compile( "^\\s*(?<key>(`([^`])*`)+?):?\\s+(?<value>.+)$" );
    private static final Pattern backtickLambdaPattern = Pattern.compile( "^\\s*(?<key>(`([^`])*`)+?)\\s*=>\\s*(?<value>.+)$" );
    private static final Pattern argPattern = Pattern.compile( "^\\s*(?<key>[\\p{L}_][\\p{L}0-9_]*):?\\s+(?<value>.+)$" );
    private static final Pattern lambdaPattern = Pattern.compile( "^\\s*(?<key>[\\p{L}_][\\p{L}0-9_]*)\\s*=>\\s*(?<value>.+)$" );
    private static final Pattern lambdaMapPattern = Pattern.compile( "^\\s*(?<key>[\\p{L}_][\\p{L}0-9_]*):\\s*=>\\s*(?<value>.+)$" );
    private final ParameterMap parameterMap;

    public Param( @Nonnull final ParameterMap parameterMap )
    {
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
        return "Set the value of a query parameter";
    }

    @Nonnull
    @Override
    public String getUsage()
    {
        return "name => value";
    }

    @Nonnull
    @Override
    public String getHelp()
    {
        return "Set the specified query parameter to the value given";
    }

    @Nonnull
    @Override
    public List<String> getAliases()
    {
        return Collections.emptyList();
    }

    @Override
    public void execute( @Nonnull final String argString ) throws CommandException
    {
        Matcher lambdaMapMatcher = lambdaMapPattern.matcher( argString );
        if ( lambdaMapMatcher.matches() )
        {
            throw new CommandException( AnsiFormattedText.from( "Incorrect usage.\nusage: " )
                                                         .bold().append( COMMAND_NAME ).boldOff().append( " " ).append( getUsage() ) );
        }
        if ( !assignIfValidParameter( argString ) )
        {
            throw new CommandException( AnsiFormattedText.from( "Incorrect number of arguments.\nusage: " )
                                                         .bold().append( COMMAND_NAME ).boldOff().append( " " ).append( getUsage() ) );
        }
    }

    private boolean assignIfValidParameter( @Nonnull String argString ) throws CommandException
    {
        return setParameterIfItMatchesPattern( argString, lambdaPattern, assignIfValidParameter() )
               || setParameterIfItMatchesPattern( argString, argPattern, assignIfValidParameter() )
               || setParameterIfItMatchesPattern( argString, backtickLambdaPattern, backTickMatchPattern() )
               || setParameterIfItMatchesPattern( argString, backtickPattern, backTickMatchPattern() );
    }

    private boolean setParameterIfItMatchesPattern( @Nonnull String argString, Pattern pattern,
                                                    BiPredicate<String, Matcher> matchingFunction ) throws CommandException
    {
        Matcher matcher = pattern.matcher( argString );
        if ( matchingFunction.test( argString, matcher ) )
        {
            parameterMap.setParameter( matcher.group( "key" ), matcher.group( "value" ) );
            return true;
        }
        else
        {
            return false;
        }
    }

    private BiPredicate<String, Matcher> assignIfValidParameter()
    {
        return ( argString, matcher ) -> matcher.matches();
    }

    private BiPredicate<String, Matcher> backTickMatchPattern()
    {
        return ( argString, backtickLambdaMatcher ) ->
        {
            return argString.trim().startsWith( "`" )
                   && backtickLambdaMatcher.matches()
                   && backtickLambdaMatcher.group( "key" ).length() > 2;
        };
    }
}
