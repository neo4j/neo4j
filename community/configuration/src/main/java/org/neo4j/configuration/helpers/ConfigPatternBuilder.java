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
package org.neo4j.configuration.helpers;

import org.apache.commons.lang3.StringUtils;

import java.util.Map;
import java.util.Optional;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

public class ConfigPatternBuilder
{
    // Map supported wildcards to their java regex definition.
    // N.b. we do not currently offer a way to escape wildcard characters (i.e. match on literal '?' or '*')
    private static final Map<Character,String> supportedWildcards = Map.of(
            '?', ".{0,1}",
            '*', ".+"
    );

    private static final String wildcardCharacters = supportedWildcards.keySet()
                                                                       .stream()
                                                                       .map( Object::toString )
                                                                       .collect( Collectors.joining( "" ) );

    /**
     * Converts a user-supplied string containing supported wildcards into a Java Regular Expression Pattern that can be used to test if strings match the
     * user-supplied string. Any input characters that are not supported wildcards will be escaped if necessary.
     * <p>
     * If the user-supplied string does not contains wildcards then an empty optional is returned.
     *
     * @param pattern      user-supplied string that may contain wildcards
     * @param patternFlags a java.util.regex.Pattern bitmask to coinfigure pattern behaviour
     * @return an optional containing a Pattern if the user-supplied string contains wildcards.
     */
    public static Optional<Pattern> optionalPatternFromConfigString( String pattern, int patternFlags )
    {
        if ( !StringUtils.containsAny( pattern, wildcardCharacters ) )
        {
            return Optional.empty();
        }

        return Optional.of( patternFromConfigString( pattern, patternFlags ) );
    }

    /**
     * Converts a user-supplied string containing supported wildcards into a Java Regular Expression Pattern that can be used to test if strings match the
     * user-supplied string.  Any input characters that are not supported wildcards will be escaped if necessary.
     *
     * @param pattern      user-supplied string that may contain wildcards
     * @param patternFlags a java.util.regex.Pattern bitmask to coinfigure pattern behaviour
     * @return a Pattern
     */
    public static Pattern patternFromConfigString( String pattern, int patternFlags )
    {
        return buildRegexPattern( pattern, patternFlags );
    }

    private static Pattern buildRegexPattern( String name, int patternFlags )
    {
        final var pattern = new StringBuilder();
        StringBuilder buffer = new StringBuilder();
        for ( int i = 0; i < name.length(); i++ )
        {
            final var ch = name.charAt( i );
            if ( supportedWildcards.containsKey( ch ) )
            {
                buffer = flushBuffer( pattern, buffer );
                pattern.append( supportedWildcards.get( ch ) );
            }
            else
            {
                buffer.append( ch );
            }
        }
        flushBuffer( pattern, buffer );
        return Pattern.compile( pattern.toString(), patternFlags );
    }

    private static StringBuilder flushBuffer( StringBuilder pattern, StringBuilder buffer )
    {
        if ( buffer.length() > 0 )
        {
            pattern.append( Pattern.quote( buffer.toString() ) );
            buffer = buffer.delete( 0, Integer.MAX_VALUE );
        }
        return buffer;
    }
}
