/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 *
 * This file is part of Neo4j Enterprise Edition. The included source
 * code can be redistributed and/or modified under the terms of the
 * Neo4j Sweden Software License, as found in the associated LICENSE.txt
 * file.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * Neo4j Sweden Software License for more details.
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
