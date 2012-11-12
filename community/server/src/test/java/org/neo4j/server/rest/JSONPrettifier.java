/**
 * Copyright (c) 2002-2012 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
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
package org.neo4j.server.rest;

import java.io.StringWriter;

/*
 * Naive implementation of a JSON prettifier.
 */
public class JSONPrettifier
{
    private static final String INDENTATION = "  ";

    public static String parse( final String json )
    {
        StringWriter w = new StringWriter();
        String indent = "\n";
        boolean inString = false, escaped = false;

        for ( char c : json.toCharArray() )
        {
            if ( c == '\\' )
            { // Track escape characters
                escaped = !escaped;
                w.append( c );
            }
            else if ( c == '"' && !escaped )
            { // Enter and exit strings
                inString = !inString;
                w.append( c );
            }
            else if ( inString )
            { // Inside strings
                w.append( c );
            }
            else if ( c == '[' || c == '{' )
            { // Opening brackets
                w.append( c );
                indent += INDENTATION;
                w.append( indent );
            }
            else if ( c == ']' || c == '}' )
            { // Closing brackets
                indent = indent.substring( 0,
                        indent.length() - INDENTATION.length() );
                w.append( indent );
                w.append( c );
            }
            else if ( c == ',' )
            { // Comma
                w.append( c );
                w.append( indent );
            }
            else
            { // Everything else
                w.append( c );
            }
        }

        return w.toString();
    }
}
