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
package org.neo4j.server.http.cypher.format.output.eventsource;

import java.io.OutputStream;
import java.lang.annotation.Annotation;
import java.lang.reflect.Type;
import java.util.function.Predicate;
import javax.ws.rs.Produces;
import javax.ws.rs.WebApplicationException;
import javax.ws.rs.core.HttpHeaders;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.MultivaluedMap;
import javax.ws.rs.ext.MessageBodyWriter;
import javax.ws.rs.ext.Provider;

import org.neo4j.server.http.cypher.format.DefaultJsonFactory;
import org.neo4j.server.http.cypher.format.api.OutputEventSource;
import org.neo4j.server.http.cypher.format.jolt.JoltCodec;

@Provider
@Produces( {EventSourceMessageBodyWriter.JSON_JOLT_MIME_TYPE_VALUE_WITH_QUALITY, } )
public class EventSourceMessageBodyWriter implements MessageBodyWriter<OutputEventSource>
{
    public static final String JSON_JOLT_MIME_TYPE_VALUE = "application/vnd.neo4j.jolt+json-seq";
    public static final String JSON_JOLT_MIME_TYPE_VALUE_WITH_QUALITY = JSON_JOLT_MIME_TYPE_VALUE + ";qs=0.5";
    public static final MediaType JSON_JOLT_MIME_TYPE = MediaType.valueOf( JSON_JOLT_MIME_TYPE_VALUE );

    @Override
    public boolean isWriteable( Class<?> type, Type genericType, Annotation[] annotations, MediaType mediaType )
    {
        return OutputEventSource.class.isAssignableFrom( type );
    }

    @Override
    public void writeTo( OutputEventSource outputEventSource, Class<?> type, Type genericType, Annotation[] annotations, MediaType mediaType,
                         MultivaluedMap<String,Object> httpHeaders, OutputStream output ) throws WebApplicationException
    {
        var transaction = outputEventSource.getTransactionHandle();
        var parameters = outputEventSource.getParameters();
        var joltStrictModeEnabled = isJoltStrictModeEnabled( httpHeaders );

        var jsonFactory = DefaultJsonFactory.INSTANCE.get();
        var serializer = new EventSourceSerializer( transaction, parameters, JoltCodec.class, joltStrictModeEnabled, jsonFactory, output );

        outputEventSource.produceEvents( serializer::handleEvent );
    }

    private boolean isJoltStrictModeEnabled( MultivaluedMap<String,Object> httpHeaders )
    {
        Predicate<MediaType> isStrictJolt =
                s -> s.isCompatible( JSON_JOLT_MIME_TYPE ) && Boolean.parseBoolean( s.getParameters().getOrDefault( "strict", Boolean.FALSE.toString() ) );
        return httpHeaders.containsKey( HttpHeaders.CONTENT_TYPE ) &&
               httpHeaders.get( HttpHeaders.CONTENT_TYPE ).stream().map( MediaType.class::cast ).anyMatch( isStrictJolt );
    }
}
