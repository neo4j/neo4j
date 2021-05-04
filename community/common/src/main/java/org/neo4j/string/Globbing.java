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
package org.neo4j.string;

import java.util.function.Predicate;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static java.util.regex.Pattern.CASE_INSENSITIVE;

public class Globbing
{
    // These are characters that have special meaning in java regex, * and ? are omitted since we have special handling for those
    private static final String specialCharacters = "<([{\\^-=$!|]})+.>";
    // To construct a pattern with the special characters they must first be escaped
    // the '.' is a regex matching every character in the string once and replacing it with the escaped form
    @SuppressWarnings( "ReplaceAllDot" )
    private static final String escapedSpecialCharacters = specialCharacters.replaceAll( ".", "\\\\$0" );
    private static final Pattern specialCharacterPattern = Pattern.compile( "[" + escapedSpecialCharacters + "]" );

    public static Predicate<String> globPredicate( String globbing )
    {
        Matcher m = specialCharacterPattern.matcher( globbing );
        // escape all special character that were found
        String escaped = m.replaceAll( "\\\\$0" );
        String escapedString = escaped.replaceAll( "\\*", ".*" )
                .replaceAll( "\\?", ".{1}" );
        return Pattern.compile( escapedString, CASE_INSENSITIVE ).asMatchPredicate();
    }
}
