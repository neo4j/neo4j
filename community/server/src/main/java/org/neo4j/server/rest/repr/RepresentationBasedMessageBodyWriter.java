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
package org.neo4j.server.rest.repr;

import java.io.IOException;
import java.io.OutputStream;
import java.lang.annotation.Annotation;
import java.lang.reflect.Type;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import javax.ws.rs.Produces;
import javax.ws.rs.WebApplicationException;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.MultivaluedMap;
import javax.ws.rs.core.UriInfo;
import javax.ws.rs.ext.MessageBodyWriter;
import javax.ws.rs.ext.Provider;
import org.neo4j.server.rest.repr.formats.JsonFormat;
import org.neo4j.service.Services;

@Provider
@Produces(MediaType.APPLICATION_JSON)
public class RepresentationBasedMessageBodyWriter implements MessageBodyWriter<Representation> {
    private static final JsonFormat JSON_FORMAT = Services.loadAll(RepresentationFormat.class).stream()
            .filter(JsonFormat.class::isInstance)
            .map(JsonFormat.class::cast)
            .findFirst()
            .orElseThrow(() -> new RuntimeException("Could not load JsonFormat"));

    @Context
    private UriInfo uriInfo;

    // uriInfo will be injected on a per-request base
    public RepresentationBasedMessageBodyWriter() {}

    public RepresentationBasedMessageBodyWriter(UriInfo uriInfo) {
        this.uriInfo = uriInfo;
    }

    public static String serialize(Representation representation, RepresentationFormat format, URI baseUri) {
        return representation.serialize(format, baseUri);
    }

    @Override
    public boolean isWriteable(Class<?> type, Type genericType, Annotation[] annotations, MediaType mediaType) {
        return Representation.class.isAssignableFrom(type);
    }

    @Override
    public void writeTo(
            Representation representation,
            Class<?> type,
            Type genericType,
            Annotation[] annotations,
            MediaType mediaType,
            MultivaluedMap<String, Object> httpHeaders,
            OutputStream entityStream)
            throws IOException, WebApplicationException {
        var content = serialize(representation, JSON_FORMAT, uriInfo.getBaseUri());
        entityStream.write(content.getBytes(
                mediaType.getParameters().getOrDefault(MediaType.CHARSET_PARAMETER, StandardCharsets.UTF_8.name())));
        entityStream.flush();
    }
}
