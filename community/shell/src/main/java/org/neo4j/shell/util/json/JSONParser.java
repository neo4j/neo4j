/*
 * Copyright (c) 2002-2018 "Neo Technology,"
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
package org.neo4j.shell.util.json;

import org.neo4j.shell.ShellException;

public class JSONParser
{
    public static Object parse( String json ) throws ShellException
    {
        try
        {
            final String input = json.trim();
            if ( input.isEmpty() )
            {
                return null;
            }
            if ( input.charAt( 0 ) == '{' )
            {
                return new JSONObject( input ).toMap();
            }
            if ( input.charAt( 0 ) == '[' )
            {
                return new JSONArray( input ).toList();
            }
            final Object value = JSONObject.stringToValue( input );
            if ( value.equals( null ) )
            {
                return null;
            }
            return value;
        } catch ( JSONException e )
        {
            throw new ShellException( "Could not parse value " + json + " " + e.getMessage() );
        }
    }
}
