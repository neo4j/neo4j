/*
 * Copyright (c) 2002-2020 "Neo4j,"
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
package org.neo4j.server.http.cypher.format.input.json;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonToken;
import com.fasterxml.jackson.databind.JsonMappingException;

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.neo4j.server.http.cypher.format.api.ConnectionException;
import org.neo4j.server.http.cypher.format.api.InputFormatException;
import org.neo4j.server.http.cypher.format.output.json.ResultDataContent;

import static com.fasterxml.jackson.core.JsonToken.END_ARRAY;
import static com.fasterxml.jackson.core.JsonToken.END_OBJECT;
import static com.fasterxml.jackson.core.JsonToken.FIELD_NAME;
import static com.fasterxml.jackson.core.JsonToken.START_ARRAY;
import static com.fasterxml.jackson.core.JsonToken.START_OBJECT;
import static java.util.Arrays.asList;
import static java.util.Collections.unmodifiableMap;
import static org.neo4j.internal.helpers.collection.MapUtil.map;

class StatementDeserializer
{
    private static final Map<String,Object> NO_PARAMETERS = unmodifiableMap( map() );

    private final JsonParser parser;
    private State state;

    private enum State
    {
        BEFORE_OUTER_ARRAY,
        IN_BODY,
        FINISHED
    }

    StatementDeserializer( JsonFactory jsonFactory, InputStream input )
    {
        try
        {
            this.parser = jsonFactory.createParser( input );
            this.state = State.BEFORE_OUTER_ARRAY;
        }
        catch ( IOException e )
        {
            throw new IllegalStateException( "Failed to create a JSON parser", e );
        }
    }

    InputStatement read()
    {

        switch ( state )
        {
        case BEFORE_OUTER_ARRAY:
            if ( !beginsWithCorrectTokens() )
            {
                return null;
            }
            state = State.IN_BODY;
        case IN_BODY:
            String statement = null;
            Map<String,Object> parameters = null;
            List<Object> resultsDataContents = null;
            boolean includeStats = false;
            JsonToken tok;

            try
            {

                while ( (tok = parser.nextToken()) != null && tok != END_OBJECT )
                {
                    if ( tok == END_ARRAY )
                    {
                        // No more statements
                        state = State.FINISHED;
                        return null;
                    }

                    parser.nextValue();
                    String currentName = parser.getCurrentName();
                    switch ( currentName )
                    {
                    case "statement":
                        statement = parser.readValueAs( String.class );
                        break;
                    case "parameters":
                        parameters = readMap();
                        break;
                    case "resultDataContents":
                        resultsDataContents = readArray();
                        break;
                    case "includeStats":
                        includeStats = parser.getBooleanValue();
                        break;
                    default:
                        discardValue();
                    }
                }

                if ( statement == null )
                {
                    throw new InputFormatException( "No statement provided." );
                }
                return new InputStatement( statement, parameters == null ? NO_PARAMETERS : parameters, includeStats,
                        ResultDataContent.fromNames( resultsDataContents ) );
            }
            catch ( JsonParseException e )
            {
                throw new InputFormatException( "Could not parse the incoming JSON", e );
            }
            catch ( JsonMappingException e )
            {
                throw new InputFormatException( "Could not map the incoming JSON", e );
            }
            catch ( IOException e )
            {
                throw new ConnectionException( "An error encountered while reading the inbound entity", e );
            }
        case FINISHED:
            return null;

        default:
            break;
        }
        return null;
    }

    private void discardValue() throws IOException
    {
        // This could be done without building up an object
        parser.readValueAs( Object.class );
    }

    @SuppressWarnings( "unchecked" )
    private Map<String,Object> readMap() throws IOException
    {
        return parser.readValueAs( Map.class );
    }

    @SuppressWarnings( "unchecked" )
    private List<Object> readArray() throws IOException
    {
        return parser.readValueAs( List.class );
    }

    private boolean beginsWithCorrectTokens()
    {
        List<JsonToken> expectedTokens = asList( START_OBJECT, FIELD_NAME, START_ARRAY );
        String expectedField = "statements";

        List<JsonToken> foundTokens = new ArrayList<>();

        try
        {
            for ( int i = 0; i < expectedTokens.size(); i++ )
            {
                JsonToken token = parser.nextToken();
                if ( i == 0 && token == null )
                {
                    return false;
                }
                if ( token == FIELD_NAME && !expectedField.equals( parser.getText() ) )
                {
                    throw new InputFormatException(
                            String.format( "Unable to deserialize request. " + "Expected first field to be '%s', but was '%s'.", expectedField,
                                    parser.getText() ) );
                }
                foundTokens.add( token );
            }
            if ( !expectedTokens.equals( foundTokens ) )
            {
                throw new InputFormatException( String.format( "Unable to deserialize request. " + "Expected %s, found %s.", expectedTokens, foundTokens ) );
            }
        }
        catch ( JsonParseException e )
        {
            throw new InputFormatException( "Could not parse the incoming JSON", e );
        }
        catch ( IOException e )
        {
            throw new ConnectionException( "An error encountered while reading the inbound entity", e );
        }
        return true;
    }
}
