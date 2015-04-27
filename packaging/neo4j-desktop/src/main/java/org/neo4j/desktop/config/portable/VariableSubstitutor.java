/*
 * Copyright (c) 2002-2015 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
 *
 * This file is part of Neo4j.
 *
 * Neo4j is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program. If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.desktop.config.portable;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.neo4j.helpers.Function;

class VariableSubstitutor
{
    private static final Pattern DEFAULT_PATTERN = Pattern.compile( "\\$\\{([^\\}]+)\\}" );
    private final Pattern pattern;

    public VariableSubstitutor( Pattern pattern )
    {
        this.pattern = pattern;
    }

    public VariableSubstitutor()
    {
        this( DEFAULT_PATTERN );
    }

    public String substitute( String input, Function<String, String> substitutionFunction )
    {
        if ( input.length() == 0 )
        {
            return "";
        }

        StringBuilder result = new StringBuilder(  );
        Matcher matcher = pattern.matcher( input );
        int cur = 0;
        while (matcher.find( cur ))
        {
            result.append( input.substring( cur, matcher.start() ) );
            result.append( substitutionFunction.apply( matcher.group( 1 ) ) );
            cur = matcher.end();
        }

        if ( cur < input.length() )
        {
            result.append( input.substring( cur ) );
        }

        return result.toString();
    }
}
