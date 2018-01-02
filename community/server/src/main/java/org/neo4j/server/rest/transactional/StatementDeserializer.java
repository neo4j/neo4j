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
package org.neo4j.server.rest.transactional;

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import org.codehaus.jackson.*;
import org.codehaus.jackson.map.JsonMappingException;

import org.neo4j.helpers.collection.PrefetchingIterator;
import org.neo4j.kernel.api.exceptions.Status;
import org.neo4j.server.rest.transactional.error.Neo4jError;

import static java.util.Arrays.asList;
import static java.util.Collections.unmodifiableMap;

import static org.codehaus.jackson.JsonToken.END_ARRAY;
import static org.codehaus.jackson.JsonToken.END_OBJECT;
import static org.codehaus.jackson.JsonToken.FIELD_NAME;
import static org.codehaus.jackson.JsonToken.START_ARRAY;
import static org.codehaus.jackson.JsonToken.START_OBJECT;

import static org.neo4j.helpers.collection.IteratorUtil.emptyIterator;
import static org.neo4j.helpers.collection.MapUtil.map;

public class StatementDeserializer extends PrefetchingIterator<Statement>
{
    private static final JsonFactory JSON_FACTORY = new JsonFactory().setCodec( new Neo4jJsonCodec() ).disable( JsonGenerator.Feature.FLUSH_PASSED_TO_STREAM );
    private static final Map<String, Object> NO_PARAMETERS = unmodifiableMap( map() );
    private static final Iterator<Neo4jError> NO_ERRORS = emptyIterator();

    private final JsonParser input;
    private State state;
    private List<Neo4jError> errors = null;

    private enum State
    {
        BEFORE_OUTER_ARRAY,
        IN_BODY,
        FINISHED
    }

    public StatementDeserializer( InputStream input )
    {
        try
        {
            this.input = JSON_FACTORY.createJsonParser( input );
            this.state = State.BEFORE_OUTER_ARRAY;
        }
        catch ( IOException e )
        {
            throw new RuntimeException( e );
        }
    }

    public Iterator<Neo4jError> errors()
    {
        return errors == null ? NO_ERRORS : errors.iterator();
    }

    @Override
    protected Statement fetchNextOrNull()
    {
        try
        {
            if ( errors != null )
            {
                return null;
            }

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
                    Map<String, Object> parameters = null;
                    List<Object> resultsDataContents = null;
                    boolean includeStats = false;
                    JsonToken tok;

                    while ( (tok = input.nextToken()) != null && tok != END_OBJECT )
                    {
                        if ( tok == END_ARRAY )
                        {
                            // No more statements
                            state = State.FINISHED;
                            return null;
                        }

                        input.nextValue();
                        String currentName = input.getCurrentName();
                        switch ( currentName )
                        {
                        case "statement":
                            statement = input.readValueAs( String.class );
                            break;
                        case "parameters":
                            parameters = readMap( input );
                            break;
                        case "resultDataContents":
                            resultsDataContents = readArray( input );
                            break;
                        case "includeStats":
                            includeStats = input.getBooleanValue();
                            break;
                        default:
                            discardValue( input );
                        }
                    }

                    if ( statement == null )
                    {
                        addError( new Neo4jError( Status.Request.InvalidFormat, new DeserializationException( "No statement provided." ) ) );
                        return null;
                    }
                    return new Statement( statement, parameters == null ? NO_PARAMETERS : parameters, includeStats,
                                          ResultDataContent.fromNames( resultsDataContents ) );


                case FINISHED:
                    return null;
            }
            return null;
        }
        catch ( JsonParseException | JsonMappingException e )
        {
            addError( new Neo4jError( Status.Request.InvalidFormat,
                        new DeserializationException( "Unable to deserialize request", e ) ) );
            return null;
        }
        catch ( IOException e )
        {
            addError( new Neo4jError( Status.Network.UnknownFailure, e ) );
            return null;
        }
        catch ( Exception e)
        {
            addError( new Neo4jError( Status.General.UnknownFailure, e ) );
            return null;
        }
    }

    private void discardValue( JsonParser input ) throws IOException
    {
        // This could be done without building up an object
        input.readValueAs( Object.class );
    }

    @SuppressWarnings("unchecked")
    private static Map<String, Object> readMap( JsonParser input ) throws IOException
    {
        return input.readValueAs( Map.class );
    }

    @SuppressWarnings("unchecked")
    private static List<Object> readArray( JsonParser input ) throws IOException
    {
        return input.readValueAs( List.class );
    }

    private void addError( Neo4jError error )
    {
        if ( errors == null )
        {
            errors = new LinkedList<>();
        }
        errors.add( error );
    }

    private boolean beginsWithCorrectTokens() throws IOException
    {
        List<JsonToken> expectedTokens = asList( START_OBJECT, FIELD_NAME, START_ARRAY );
        String expectedField = "statements";

        List<JsonToken> foundTokens = new ArrayList<>();

        for ( int i = 0; i < expectedTokens.size(); i++ )
        {
            JsonToken token = input.nextToken();
            if ( i == 0 && token == null )
            {
                return false;
            }
            if ( token == FIELD_NAME && !expectedField.equals( input.getText() ) )
            {
                addError( new Neo4jError(
                        Status.Request.InvalidFormat,
                        new DeserializationException( String.format( "Unable to deserialize request. " +
                                                                     "Expected first field to be '%s', but was '%s'.",
                                                                     expectedField, input.getText() ) ) ) );
                return false;
            }
            foundTokens.add( token );
        }
        if ( !expectedTokens.equals( foundTokens ) )
        {
            addError( new Neo4jError(
                    Status.Request.InvalidFormat,
                    new DeserializationException( String.format( "Unable to deserialize request. " +
                            "Expected %s, found %s.", expectedTokens, foundTokens ) ) ) );
            return false;
        }
        return true;
    }
}
