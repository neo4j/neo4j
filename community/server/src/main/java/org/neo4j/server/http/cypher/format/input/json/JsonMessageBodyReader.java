/*
 * Copyright (c) 2002-2019 "Neo4j,"
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

import java.io.InputStream;
import java.lang.annotation.Annotation;
import java.lang.reflect.Type;
import java.util.HashMap;
import java.util.Map;
import javax.ws.rs.Consumes;
import javax.ws.rs.WebApplicationException;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.MultivaluedMap;
import javax.ws.rs.ext.MessageBodyReader;
import javax.ws.rs.ext.Provider;

import org.neo4j.server.http.cypher.format.api.InputEventStream;
import org.neo4j.server.http.cypher.format.api.Statement;

@Provider
@Consumes( MediaType.APPLICATION_JSON )
public class JsonMessageBodyReader implements MessageBodyReader<InputEventStream>
{

    private static final String STATEMENTS_KEY = "input-statements";

    @Override
    public boolean isReadable( Class<?> type, Type genericType, Annotation[] annotations, MediaType mediaType )
    {
        return type.isAssignableFrom( InputEventStream.class );
    }

    @Override
    public InputEventStream readFrom( Class<InputEventStream> type, Type genericType, Annotation[] annotations, MediaType mediaType,
            MultivaluedMap<String,String> httpHeaders, InputStream entityStream ) throws WebApplicationException
    {

        Map<Statement,InputStatement> inputStatements = new HashMap<>();
        Map<String,Object> parameters = new HashMap<>();
        parameters.put( STATEMENTS_KEY, inputStatements );
        StatementDeserializer statementDeserializer = new StatementDeserializer( entityStream );
        return new InputEventStream( parameters, () ->
        {
            InputStatement inputStatement = statementDeserializer.read();

            if ( inputStatement == null )
            {
                return null;
            }

            Statement statement = new Statement( inputStatement.statement(), inputStatement.parameters() );
            inputStatements.put( statement, inputStatement );
            return statement;
        } );
    }

    /**
     * Extracts a representation of a statement that is legacy json input format specific.
     *
     * @param parameters parameter mep from which the statement will be extracted.
     * @param statement the to be extracted statement in generic format.
     * @return a statement representation.
     */
    public static InputStatement getInputStatement( Map<String,Object> parameters, Statement statement )
    {
        if ( !parameters.containsKey( STATEMENTS_KEY ) )
        {
            return null;
        }

        Map<Statement,InputStatement> inputStatements = (Map<Statement,InputStatement>) parameters.get( STATEMENTS_KEY );
        return inputStatements.get( statement );
    }
}
