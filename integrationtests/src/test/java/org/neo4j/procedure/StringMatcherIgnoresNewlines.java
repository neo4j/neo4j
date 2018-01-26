/*
 * Copyright (c) 2002-2018 "Neo Technology,"
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
package org.neo4j.procedure;

import org.hamcrest.Matcher;
import org.hamcrest.core.StringContains;

import java.util.regex.Pattern;

public class StringMatcherIgnoresNewlines
{
    private StringMatcherIgnoresNewlines()
    {
    }

    public static Matcher<String> containsStringIgnoreNewlines( String substring )
    {
        return new StringContains( substring )
        {
            Pattern newLines = Pattern.compile( "\\s*[\\r\\n]+\\s*" );

            private String clean( String string )
            {
                return newLines.matcher( string ).replaceAll( "" );
            }

            @Override
            protected boolean evalSubstringOf( String s )
            {
                return clean( s ).contains( clean( substring ) );
            }
        };
    }
}
