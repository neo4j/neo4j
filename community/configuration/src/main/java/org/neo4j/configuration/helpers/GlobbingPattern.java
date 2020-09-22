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
package org.neo4j.configuration.helpers;

import java.util.ArrayList;
import java.util.List;
import java.util.regex.Pattern;
import java.util.regex.PatternSyntaxException;

import org.neo4j.annotations.api.PublicApi;

/**
 * Simple globbing pattern with support for '*' and '?'.
 */
@PublicApi
public final class GlobbingPattern
{
    private final String originalString;
    private final Pattern regexPattern;

    public GlobbingPattern( String pattern )
    {
        originalString = pattern;
        try
        {
            regexPattern = buildRegex( pattern );
        }
        catch ( PatternSyntaxException e )
        {
            throw new IllegalArgumentException( "Invalid globbing pattern '" + pattern + "'", e );
        }
    }

    public static List<GlobbingPattern> create( String... patterns )
    {
        ArrayList<GlobbingPattern> globbingPatterns = new ArrayList<>();
        for ( String pattern : patterns )
        {
            globbingPatterns.add( new GlobbingPattern( pattern ) );
        }
        return globbingPatterns;
    }

    public boolean matches( String value )
    {
        return regexPattern.matcher( value ).matches();
    }

    @Override
    public String toString()
    {
        return originalString;
    }

    @Override
    public boolean equals( Object o )
    {
        if ( this == o )
        {
            return true;
        }
        if ( o == null || getClass() != o.getClass() )
        {
            return false;
        }
        GlobbingPattern that = (GlobbingPattern) o;
        return originalString.equals( that.originalString );
    }

    @Override
    public int hashCode()
    {
        return originalString.hashCode();
    }

    private Pattern buildRegex( String globbingPattern )
    {
        StringBuilder patternString = new StringBuilder();
        for ( int i = 0; i < globbingPattern.length(); i++ )
        {
            char ch = globbingPattern.charAt( i );
            if ( ch == '*' )
            {
                patternString.append( ".*" );
            }
            else if ( ch == '?' )
            {
                patternString.append( "." );
            }
            else if ( ch == '.' || ch == '-' )
            {
                patternString.append( "\\" ).append( ch );
            }
            else
            {
                patternString.append( ch );
            }
        }
        return Pattern.compile( patternString.toString() );
    }
}
