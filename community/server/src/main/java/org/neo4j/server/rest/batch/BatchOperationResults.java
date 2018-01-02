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
package org.neo4j.server.rest.batch;

import java.io.StringWriter;
import java.util.HashMap;
import java.util.Map;

import org.neo4j.server.rest.domain.JsonHelper;

/*
 * Because the batch operation API operates on the HTTP abstraction
 * level, we do not use our normal serialization system for serializing
 * its' results.
 * 
 * Doing so would require us to de-serialize each JSON response we get from
 * each operation, and we would have to extend our current type safe serialization
 * system to incorporate arbitrary responses.
 */
public class BatchOperationResults
{
    private static final String CLOSING_BRACKET = "]";
    private static final String OPENING_BRACKET = "[";
    private static final String OPENING_CURLY = "{";
    private static final String CLOSING_CURLY = "}";
    private static final String COMMA = ",";

    private StringWriter results = new StringWriter();
    private boolean firstResult = true;
    private Map<Integer, String> locations = new HashMap<Integer, String>();

    public BatchOperationResults() {
        results.append( OPENING_BRACKET );
    }

    public void addOperationResult( String from, Integer id, String body, String location )
    {
        if(firstResult)
            firstResult = false;
        else
            results.append(',');
        
        results.append( OPENING_CURLY );

        if ( id != null )
        {
            results.append( "\"id\":" )
                    .append( id.toString() )
                    .append( COMMA );
        }

        if ( location != null )
        {
            locations.put( id, location );
            results.append( "\"location\":" )
                    .append( JsonHelper.createJsonFrom( location ) )
                    .append( COMMA );
        }

        if ( body != null && body.length() != 0 )
        {
            results.append( "\"body\":" )
                    .append( body )
                    .append( COMMA );
        }

        results.append( "\"from\":" )
                .append( JsonHelper.createJsonFrom( from ) );

        results.append( CLOSING_CURLY );
    }

    public Map<Integer, String> getLocations()
    {
        return locations;
    }

    public String toJSON()
    {
        results.append( CLOSING_BRACKET );
        return results.toString();
    }
}
