/**
 * Copyright (c) 2002-2013 "Neo Technology,"
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

import org.codehaus.jackson.JsonParseException;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.map.ObjectWriter;

/*
 * Naive implementation of a JSON prettifier.
 */
public class JSONPrettifier
{
    public static String parse( final String json )
    {
        if(json==null) {
            return "";
        }
        ObjectMapper mapper = new ObjectMapper();
        ObjectWriter writer = mapper.defaultPrettyPrintingWriter();
        Object myObject;
        try
        {
            myObject = mapper.readValue( json, Object.class );
            return writer.writeValueAsString(myObject );
        }
        catch ( JsonParseException e )
        {
            //this prolly isn't JSON
            return json;
        }
        catch ( Exception e )
        {
            e.printStackTrace();
            throw new RuntimeException( "bad input " + json );
        }
    }
}
