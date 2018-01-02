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
package org.neo4j.server.rest.repr.formats;

import java.io.IOException;

import org.codehaus.jackson.JsonParseException;
import org.codehaus.jackson.JsonParser;
import org.codehaus.jackson.JsonToken;

public class StreamingJsonUtils {
    
    public static String readCurrentValueAsString(JsonParser jp, JsonToken token) throws JsonParseException, IOException {
        return readCurrentValueInto(jp, token, new StringBuilder()).toString();
    }
    
    private static StringBuilder readCurrentValueInto(JsonParser jp, JsonToken token, StringBuilder b) throws JsonParseException, IOException {
        if( token == JsonToken.START_OBJECT ) {
            boolean first = true;
            b.append('{');
            while( (token = jp.nextToken()) != JsonToken.END_OBJECT && token != null) {
                if(!first)
                    b.append(',');
                else
                    first = false;
                
                b.append('"');
                b.append(jp.getText());
                b.append('"');
                b.append(':');
                
                readCurrentValueInto(jp, jp.nextToken(), b);
            }
            b.append('}');
        } else if( token == JsonToken.START_ARRAY ) {
            boolean first = true;
            b.append('[');
            while( (token = jp.nextToken()) != JsonToken.END_ARRAY && token != null) {
                if(!first)
                    b.append(',');
                else
                    first = false;
                
                readCurrentValueInto(jp, token, b);
            }
            b.append(']');
        } else if ( token == JsonToken.VALUE_STRING ) {
            b.append('"');
            b.append(jp.getText());
            b.append('"');
        } else if ( token == JsonToken.VALUE_FALSE ) {
            b.append("false");
        } else if ( token == JsonToken.VALUE_TRUE ) {
            b.append("true");
        } else if ( token == JsonToken.VALUE_NULL ) {
            b.append("null");
        } else {
            b.append(jp.getText());
        }
        return b;
    }
    
}
