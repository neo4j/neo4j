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
package org.neo4j.server.rest.transactional;

import static java.util.Arrays.asList;
import static java.util.Collections.unmodifiableMap;
import static org.codehaus.jackson.JsonToken.END_ARRAY;
import static org.codehaus.jackson.JsonToken.END_OBJECT;
import static org.codehaus.jackson.JsonToken.FIELD_NAME;
import static org.codehaus.jackson.JsonToken.START_ARRAY;
import static org.codehaus.jackson.JsonToken.START_OBJECT;
import static org.neo4j.helpers.collection.IteratorUtil.emptyIterator;
import static org.neo4j.helpers.collection.MapUtil.map;

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import org.codehaus.jackson.JsonFactory;
import org.codehaus.jackson.JsonParseException;
import org.codehaus.jackson.JsonParser;
import org.codehaus.jackson.JsonToken;
import org.neo4j.helpers.collection.PrefetchingIterator;
import org.neo4j.server.rest.transactional.error.ClientCommunicationError;
import org.neo4j.server.rest.transactional.error.InvalidRequestError;
import org.neo4j.server.rest.transactional.error.Neo4jError;

public class StatementDeserializer extends PrefetchingIterator<Statement>
{
    private static final JsonFactory JSON_FACTORY = new JsonFactory().setCodec( new Neo4jJsonCodec() );
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
                    if ( !discardTokensUntilStatementArray() )
                    {
                        return null;
                    }
                    state = State.IN_BODY;
                case IN_BODY:
                    String statement = null;
                    Map<String, Object> parameters = null;
                    JsonToken tok;

                    while ( (tok = input.nextToken()) != null && tok != END_OBJECT )
                    {
                        if ( tok == END_ARRAY )
                        {
                            // No more statements
                            state = State.FINISHED;
                            return null;
                        }

                        input.nextToken();
                        String currentName = input.getCurrentName();
                        if ( "statement".equals( currentName ) )
                        {
                            statement = input.nextTextValue();
                        }
                        else if ( "parameters".equals( currentName ) )
                        {
                            parameters = readMap( input );
                        }
                    }

                    if ( statement == null )
                    {
                        throw new InvalidRequestError( "No statement provided." );
                    }
                    return new Statement( statement, parameters == null ? NO_PARAMETERS : parameters );


                case FINISHED:
                    return null;
            }
            return null;
        }
        catch ( JsonParseException e )
        {
            addError( new InvalidRequestError( "Unable to deserialize request: " + e.getMessage() ) );
            return null;
        }
        catch ( IOException e )
        {
            addError( new ClientCommunicationError( "Input error while deserializing request.", e ) );
            return null;
        }
        catch ( Neo4jError error )
        {
            addError( error );
            return null;
        }
    }

    @SuppressWarnings("unchecked")
    private static Map<String, Object> readMap( JsonParser input ) throws IOException
    {
        return input.readValueAs( Map.class );
    }

    private void addError( Neo4jError error )
    {
        if ( errors == null )
        {
            errors = new LinkedList<Neo4jError>();
        }
        errors.add( error );
    }

    private boolean discardTokensUntilStatementArray() throws Neo4jError, IOException
    {
        List<JsonToken> expectedTokens = asList( START_OBJECT, FIELD_NAME, START_ARRAY );
        String expectedField = "statements";

        List<JsonToken> foundTokens = new ArrayList<JsonToken>();

        for ( int i = 0; i < expectedTokens.size(); i++ )
        {
            JsonToken token = input.nextToken();
            if ( i == 0 && token == null )
            {
                return false;
            }
            if ( token == FIELD_NAME )
            {
                if ( !expectedField.equals( input.getText() ) )
                {
                    throw new InvalidRequestError( String.format( "Unable to deserialize request, " +
                            "expected first field to be '%s', but was '%s'", expectedField, input.getText() ) );
                }
            }
            foundTokens.add( token );
        }
        if ( !expectedTokens.equals( foundTokens ) )
        {
            throw new InvalidRequestError( String.format( "Unable to deserialize request, expected %s, found %s.",
                    expectedTokens, foundTokens ) );
        }
        return true;
    }
}
