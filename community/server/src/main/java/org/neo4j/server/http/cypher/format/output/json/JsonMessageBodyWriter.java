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
package org.neo4j.server.http.cypher.format.output.json;

import java.io.OutputStream;
import java.lang.annotation.Annotation;
import java.lang.reflect.Type;
import java.util.Map;
import javax.ws.rs.Produces;
import javax.ws.rs.WebApplicationException;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.MultivaluedMap;
import javax.ws.rs.ext.MessageBodyWriter;
import javax.ws.rs.ext.Provider;

import org.neo4j.server.http.cypher.TransitionalPeriodTransactionMessContainer;
import org.neo4j.server.http.cypher.format.api.FailureEvent;
import org.neo4j.server.http.cypher.format.api.OutputEvent;
import org.neo4j.server.http.cypher.format.api.OutputEventSource;
import org.neo4j.server.http.cypher.format.api.RecordEvent;
import org.neo4j.server.http.cypher.format.api.StatementEndEvent;
import org.neo4j.server.http.cypher.format.api.StatementStartEvent;
import org.neo4j.server.http.cypher.format.api.TransactionInfoEvent;
import org.neo4j.server.http.cypher.format.api.TransactionUriScheme;
import org.neo4j.server.http.cypher.format.input.json.InputStatement;
import org.neo4j.server.http.cypher.format.input.json.JsonMessageBodyReader;

@Provider
@Produces( MediaType.APPLICATION_JSON )
public class JsonMessageBodyWriter implements MessageBodyWriter<OutputEventSource>
{

    @Override
    public boolean isWriteable( Class<?> type, Type genericType, Annotation[] annotations, MediaType mediaType )
    {
        return OutputEventSource.class.isAssignableFrom( type );
    }

    @Override
    public void writeTo( OutputEventSource outputEventSource, Class<?> type, Type genericType, Annotation[] annotations, MediaType mediaType,
            MultivaluedMap<String,Object> httpHeaders, OutputStream entityStream ) throws WebApplicationException
    {
        TransitionalPeriodTransactionMessContainer transactionContainer = outputEventSource.getTransactionContainer();
        TransactionUriScheme uriInfo = outputEventSource.getUriInfo();
        ExecutionResultSerializer serializer = new ExecutionResultSerializer( entityStream, uriInfo.dbUri(), transactionContainer );

        outputEventSource.produceEvents( outputEvent -> handleEvent( outputEvent, serializer, outputEventSource.getParameters() ) );
    }

    private void handleEvent( OutputEvent event, ExecutionResultSerializer serializer, Map<String,Object> parameters )
    {
        switch ( event.getType() )
        {
        case STATEMENT_START:
            StatementStartEvent statementStartEvent = (StatementStartEvent) event;
            InputStatement inputStatement = JsonMessageBodyReader.getInputStatement( parameters, statementStartEvent.getStatement() );
            serializer.writeStatementStart( statementStartEvent, inputStatement );
            break;
        case RECORD:
            serializer.writeRecord( (RecordEvent) event );
            break;
        case STATEMENT_END:
            StatementEndEvent statementEndEvent = (StatementEndEvent) event;
            serializer.writeStatementEnd( statementEndEvent );
            break;
        case FAILURE:
            FailureEvent failureEvent = (FailureEvent) event;
            serializer.writeFailure( failureEvent );
            break;
        case TRANSACTION_INFO:
            TransactionInfoEvent transactionInfoEvent = (TransactionInfoEvent) event;
            serializer.writeTransactionInfo( transactionInfoEvent );
            break;
        default:
            throw new IllegalStateException( "Unsupported event encountered:"  + event.getType() );
        }
    }
}
