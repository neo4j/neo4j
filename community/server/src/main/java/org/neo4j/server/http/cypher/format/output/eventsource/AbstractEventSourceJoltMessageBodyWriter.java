/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [https://neo4j.com]
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
 * along with this program.  If not, see <https://www.gnu.org/licenses/>.
 */
package org.neo4j.server.http.cypher.format.output.eventsource;

import com.fasterxml.jackson.core.JsonFactory;
import java.io.OutputStream;
import java.lang.annotation.Annotation;
import java.lang.reflect.Type;
import java.util.Map;
import java.util.function.Predicate;
import javax.ws.rs.WebApplicationException;
import javax.ws.rs.core.HttpHeaders;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.MultivaluedMap;
import javax.ws.rs.ext.MessageBodyWriter;
import org.neo4j.server.http.cypher.format.DefaultJsonFactory;
import org.neo4j.server.http.cypher.format.api.OutputEventSource;

public abstract class AbstractEventSourceJoltMessageBodyWriter implements MessageBodyWriter<OutputEventSource> {

    @Override
    public boolean isWriteable(Class<?> type, Type genericType, Annotation[] annotations, MediaType mediaType) {
        return OutputEventSource.class.isAssignableFrom(type);
    }

    @Override
    public void writeTo(
            OutputEventSource outputEventSource,
            Class<?> type,
            Type genericType,
            Annotation[] annotations,
            MediaType mediaType,
            MultivaluedMap<String, Object> httpHeaders,
            OutputStream output)
            throws WebApplicationException {
        var parameters = outputEventSource.getParameters();
        var joltStrictModeEnabled = isJoltStrictModeEnabled(httpHeaders);

        var jsonFactory = DefaultJsonFactory.INSTANCE.get();
        var serializer = this.createSerializer(output, jsonFactory, parameters, joltStrictModeEnabled);

        outputEventSource.produceEvents(serializer::handleEvent);
    }

    private boolean isJoltStrictModeEnabled(MultivaluedMap<String, Object> httpHeaders) {
        Predicate<MediaType> isStrictJolt = s -> s.isCompatible(getMediaType())
                && Boolean.parseBoolean(s.getParameters().getOrDefault("strict", Boolean.FALSE.toString()));

        return httpHeaders.containsKey(HttpHeaders.CONTENT_TYPE)
                && httpHeaders.get(HttpHeaders.CONTENT_TYPE).stream()
                        .map(MediaType.class::cast)
                        .anyMatch(isStrictJolt);
    }

    protected abstract MediaType getMediaType();

    protected abstract EventSourceSerializer createSerializer(
            OutputStream outputStream, JsonFactory jsonFactory, Map<String, Object> parameters, boolean strict);
}
