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
package org.neo4j.internal.helpers;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class NameUtil
{

    private NameUtil()
    {
    }

    private static final String BACKTICK = "`";
    private static final String BACKTICK_ESCAPED = BACKTICK + BACKTICK;
    // Java allows for multiple 'u' in escaped unicodes
    private static final Pattern BACKTICK_UNICODE_ESCAPED = Pattern.compile( "\\\\u+0060" );
    private static final Pattern ALPHA_NUMERIC = Pattern.compile( "^[\\p{L}_][\\p{L}0-9_]*" );
    private static final Pattern GLOB = Pattern.compile("^[\\p{L}_?*][\\p{L}0-9_*?.]*");

    public static String escapeBackticks( String string )
    {
        return BACKTICK_UNICODE_ESCAPED.matcher( string ).replaceAll( BACKTICK ).replace( BACKTICK, BACKTICK_ESCAPED );
    }

    public static String forceEscapeName( String string )
    {
        return BACKTICK + escapeBackticks(string) + BACKTICK;
    }

    public static String escapeName( String string )
    {
        return escape( ALPHA_NUMERIC, string );
    }

    public static String escapeGlob( String string )
    {
        return escape( GLOB, string );
    }

    private static String escape( Pattern pattern, String string )
    {
        Matcher matcher = pattern.matcher( string );
        if ( !matcher.matches() )
        {
            return forceEscapeName( string );
        }
        return string;
    }
}
