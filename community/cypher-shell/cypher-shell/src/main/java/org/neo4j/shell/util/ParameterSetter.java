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
package org.neo4j.shell.util;

import java.util.function.BiPredicate;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import javax.annotation.Nonnull;

import org.neo4j.shell.ParameterMap;
import org.neo4j.shell.exception.ParameterException;

/**
 * Shared logic to parse parameters and set them in a ParameterMap
 */
public abstract class ParameterSetter<E extends Exception>
{
    // Match arguments such as "(key) (value with possible spaces)" where key and value are any strings
    private static final Pattern backtickPattern = Pattern.compile( "^\\s*(?<key>(`([^`])*`)+?):?\\s+(?<value>.+)$" );
    private static final Pattern backtickLambdaPattern = Pattern.compile( "^\\s*(?<key>(`([^`])*`)+?)\\s*=>\\s*(?<value>.+)$" );
    private static final Pattern argPattern = Pattern.compile( "^\\s*(?<key>[\\p{L}_][\\p{L}0-9_]*):?\\s+(?<value>.+)$" );
    private static final Pattern lambdaPattern = Pattern.compile( "^\\s*(?<key>[\\p{L}_][\\p{L}0-9_]*)\\s*=>\\s*(?<value>.+)$" );
    private static final Pattern lambdaMapPattern = Pattern.compile( "^\\s*(?<key>[\\p{L}_][\\p{L}0-9_]*):\\s*=>\\s*(?<value>.+)$" );

    private final ParameterMap parameterMap;

    protected ParameterSetter( ParameterMap parameterMap )
    {
        this.parameterMap = parameterMap;
    }

    protected abstract void onWrongUsage() throws E;

    protected abstract void onWrongNumberOfArguments() throws E;

    protected abstract void onParameterException( ParameterException e ) throws E;

    public void execute( @Nonnull final String argString ) throws E
    {
        Matcher lambdaMapMatcher = lambdaMapPattern.matcher( argString );
        if ( lambdaMapMatcher.matches() )
        {
            onWrongUsage();
        }
        try
        {
            if ( !assignIfValidParameter( argString ) )
            {
                onWrongNumberOfArguments();
            }
        }
        catch ( ParameterException e )
        {
            onParameterException( e );
        }
    }

    private boolean assignIfValidParameter( @Nonnull String argString ) throws ParameterException
    {
        return setParameterIfItMatchesPattern( argString, lambdaPattern, assignIfValidParameter() )
               || setParameterIfItMatchesPattern( argString, argPattern, assignIfValidParameter() )
               || setParameterIfItMatchesPattern( argString, backtickLambdaPattern, backTickMatchPattern() )
               || setParameterIfItMatchesPattern( argString, backtickPattern, backTickMatchPattern() );
    }

    private boolean setParameterIfItMatchesPattern( @Nonnull String argString, Pattern pattern,
                                                    BiPredicate<String, Matcher> matchingFunction ) throws ParameterException
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
