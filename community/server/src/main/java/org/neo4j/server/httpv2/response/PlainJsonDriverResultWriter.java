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
package org.neo4j.server.httpv2.response;

import com.fasterxml.jackson.core.JsonFactory;
import java.io.IOException;
import java.io.OutputStream;
import java.lang.annotation.Annotation;
import java.lang.reflect.Type;
import javax.ws.rs.Produces;
import javax.ws.rs.WebApplicationException;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.MultivaluedMap;
import javax.ws.rs.ext.Provider;
import org.neo4j.logging.InternalLog;
import org.neo4j.server.http.cypher.format.DefaultJsonFactory;
import org.neo4j.server.httpv2.request.ResultContainer;
import org.neo4j.server.httpv2.response.format.QueryAPICodec;
import org.neo4j.server.httpv2.response.format.View;

@Provider
@Produces("application/json")
public class PlainJsonDriverResultWriter extends AbstractDriverResultWriter {

    private final JsonFactory jsonFactory;

    public PlainJsonDriverResultWriter(@Context InternalLog log) {
        super(log);
        this.jsonFactory = DefaultJsonFactory.INSTANCE.get().copy().setCodec(new QueryAPICodec(View.PLAIN_JSON));
    }

    @Override
    public void writeTo(
            ResultContainer result,
            Class<?> type,
            Type genericType,
            Annotation[] annotations,
            MediaType mediaType,
            MultivaluedMap<String, Object> httpHeaders,
            OutputStream entityStream)
            throws IOException, WebApplicationException {
        writeDriverResult(jsonFactory, result, entityStream);
    }
}
