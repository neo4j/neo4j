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
package org.neo4j.server.queryapi.response.error;

import com.fasterxml.jackson.core.JsonFactory;
import java.io.IOException;
import java.io.OutputStream;
import java.lang.annotation.Annotation;
import java.lang.reflect.Type;
import javax.ws.rs.Produces;
import javax.ws.rs.WebApplicationException;
import javax.ws.rs.core.HttpHeaders;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.MultivaluedMap;
import javax.ws.rs.ext.MessageBodyWriter;
import javax.ws.rs.ext.Provider;
import org.neo4j.server.http.cypher.format.DefaultJsonFactory;
import org.neo4j.server.queryapi.QueryMimeTypes;
import org.neo4j.server.queryapi.response.format.QueryAPICodec;
import org.neo4j.server.queryapi.response.format.QueryBodyFormatter;
import org.neo4j.server.queryapi.types.View;

@Provider
@Produces(QueryMimeTypes.ALL_JSONL)
public class JsonlErrorResponseWriter implements MessageBodyWriter<HttpErrorResponse> {
    private final JsonFactory jsonFactory;

    public JsonlErrorResponseWriter() {
        this.jsonFactory = DefaultJsonFactory.INSTANCE.get().copy().setCodec(new QueryAPICodec(View.PLAIN_JSON));
    }

    @Override
    public boolean isWriteable(Class<?> type, Type genericType, Annotation[] annotations, MediaType mediaType) {
        return HttpErrorResponse.class.isAssignableFrom(type);
    }

    @Override
    public void writeTo(
            HttpErrorResponse httpErrorResponse,
            Class<?> aClass,
            Type type,
            Annotation[] annotations,
            MediaType mediaType,
            MultivaluedMap<String, Object> httpHeaders,
            OutputStream outputStream)
            throws IOException, WebApplicationException {
        var jsonGenerator = jsonFactory.createGenerator(outputStream);
        // In some situations, the content type is not detect and it
        // should fall back to JSON.
        if (!httpHeaders.containsKey(HttpHeaders.CONTENT_TYPE)) {
            // If we don't know the content type, default it to application/json
            httpHeaders.add(HttpHeaders.CONTENT_TYPE, MediaType.APPLICATION_JSON);
            jsonFactory.createGenerator(outputStream).writeObject(httpErrorResponse);
            return;
        }

        var formatter = new QueryBodyFormatter(jsonGenerator, outputStream);
        formatter.jsonl(jsonl -> {
            jsonl.error(httpErrorResponse);
        });
    }
}
