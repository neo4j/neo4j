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
package org.neo4j.server.queryapi.response.writer;

import com.fasterxml.jackson.core.JsonFactory;
import java.io.IOException;
import java.io.OutputStream;
import java.lang.annotation.Annotation;
import java.lang.reflect.Type;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.MultivaluedMap;
import javax.ws.rs.ext.MessageBodyWriter;
import org.neo4j.server.http.cypher.format.DefaultJsonFactory;
import org.neo4j.server.http.cypher.format.api.ConnectionException;
import org.neo4j.server.queryapi.exception.ExceptionsUnwrapper;
import org.neo4j.server.queryapi.response.QueryResponseAutoCommit;
import org.neo4j.server.queryapi.response.format.QueryAPICodec;
import org.neo4j.server.queryapi.response.format.QueryBodyFormatter;
import org.neo4j.server.queryapi.response.format.View;

abstract class AbstractJsonlDriverResultWriter implements MessageBodyWriter<QueryResponseAutoCommit> {

    private final JsonFactory jsonFactory;

    protected AbstractJsonlDriverResultWriter(View view) {
        this.jsonFactory = DefaultJsonFactory.INSTANCE.get().copy().setCodec(new QueryAPICodec(view));
    }

    @Override
    public boolean isWriteable(Class<?> aClass, Type type, Annotation[] annotations, MediaType mediaType) {
        return QueryResponseAutoCommit.class.isAssignableFrom(aClass);
    }

    @Override
    public void writeTo(
            QueryResponseAutoCommit container,
            Class<?> aClass,
            Type type,
            Annotation[] annotations,
            MediaType mediaType,
            MultivaluedMap<String, Object> httpHeaders,
            OutputStream outputStream)
            throws IOException {
        httpHeaders.add("Transfer-encoding", "chunked");
        var jsonGenerator = jsonFactory.createGenerator(outputStream);
        var formatter = new QueryBodyFormatter(jsonGenerator, outputStream);

        try (var session = container.session()) {
            formatter.jsonl(jsonl -> {
                var result = container.result();
                jsonl.header(result.keys());
                while (result.hasNext()) {
                    jsonl.record(result.next());
                    outputStream.flush();
                }
                container.timers().notifyResultConsumed();
                jsonl.summary(
                        result.consume(),
                        container.timers().resultAvailableAfter(),
                        container.timers().resultConsumedAfter(),
                        session.lastBookmarks(),
                        container.requireSummaryCounters());
            });
        } catch (IOException ex) {
            ExceptionsUnwrapper.unwrapAndThrowNeo4jAndQueryApiExceptions(ex);
            throw new ConnectionException("Failed to write to the connection", ex);
        } finally {
            jsonGenerator.flush();
        }
    }
}
