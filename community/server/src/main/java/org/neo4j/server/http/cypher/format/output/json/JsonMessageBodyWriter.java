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
package org.neo4j.server.http.cypher.format.output.json;

import java.io.OutputStream;
import java.lang.annotation.Annotation;
import java.lang.reflect.Type;

import javax.ws.rs.Produces;
import javax.ws.rs.WebApplicationException;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.MultivaluedMap;
import javax.ws.rs.ext.MessageBodyWriter;
import javax.ws.rs.ext.Provider;

import org.neo4j.server.http.cypher.format.DefaultJsonFactory;
import org.neo4j.server.http.cypher.format.api.OutputEventSource;
import org.neo4j.server.http.cypher.format.common.Neo4jJsonCodec;

@Provider
@Produces( JsonMessageBodyWriter.JSON_MIME_TYPE_WITH_QUALITY )
public class JsonMessageBodyWriter implements MessageBodyWriter<OutputEventSource>
{
    // ensures this MessageBodyWriter is the default when no accept header is provided by giving it a higher qs score
    public static final String JSON_MIME_TYPE_WITH_QUALITY = MediaType.APPLICATION_JSON + ";qs=0.9";

    @Override
    public boolean isWriteable( Class<?> type, Type genericType, Annotation[] annotations, MediaType mediaType )
    {
        return OutputEventSource.class.isAssignableFrom( type );
    }

    @Override
    public void writeTo( OutputEventSource outputEventSource, Class<?> type, Type genericType, Annotation[] annotations, MediaType mediaType,
            MultivaluedMap<String,Object> httpHeaders, OutputStream entityStream ) throws WebApplicationException
    {
        var transaction = outputEventSource.getTransactionHandle();
        var parameters = outputEventSource.getParameters();
        var uriInfo = outputEventSource.getUriInfo();

        var jsonFactory = DefaultJsonFactory.INSTANCE.get();
        var serializer = new ExecutionResultSerializer( transaction, parameters, uriInfo.dbUri(),
            Neo4jJsonCodec.class, jsonFactory, entityStream );

        outputEventSource.produceEvents( serializer::handleEvent );
    }
}
