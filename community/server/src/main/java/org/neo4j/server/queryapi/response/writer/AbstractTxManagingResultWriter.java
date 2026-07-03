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
import javax.ws.rs.WebApplicationException;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.MultivaluedMap;
import javax.ws.rs.ext.MessageBodyWriter;
import org.neo4j.logging.InternalLog;
import org.neo4j.server.http.cypher.format.DefaultJsonFactory;
import org.neo4j.server.http.cypher.format.api.ConnectionException;
import org.neo4j.server.queryapi.exception.ExceptionsUnwrapper;
import org.neo4j.server.queryapi.response.QueryResponseTxManaged;
import org.neo4j.server.queryapi.response.format.QueryAPICodec;
import org.neo4j.server.queryapi.response.format.QueryBodyFormatter;
import org.neo4j.server.queryapi.response.format.View;
import org.neo4j.server.queryapi.tx.TransactionManager;

abstract class AbstractTxManagingResultWriter implements MessageBodyWriter<QueryResponseTxManaged> {

    private final InternalLog log;
    private final JsonFactory jsonFactory;
    private final TransactionManager transactionManager;

    public AbstractTxManagingResultWriter(InternalLog log, View view, TransactionManager transactionManager) {
        this.log = log;
        this.jsonFactory = DefaultJsonFactory.INSTANCE.get().copy().setCodec(new QueryAPICodec(view));
        this.transactionManager = transactionManager;
    }

    @Override
    public void writeTo(
            QueryResponseTxManaged queryResponseTxManaged,
            Class<?> type,
            Type genericType,
            Annotation[] annotations,
            MediaType mediaType,
            MultivaluedMap<String, Object> httpHeaders,
            OutputStream entityStream)
            throws IOException, WebApplicationException {
        writeDriverResult(queryResponseTxManaged, entityStream);
    }

    @Override
    public boolean isWriteable(Class<?> type, Type genericType, Annotation[] annotations, MediaType mediaType) {
        return QueryResponseTxManaged.class.isAssignableFrom(type);
    }

    public void writeDriverResult(QueryResponseTxManaged result, OutputStream outputStream) throws IOException {
        var success = false;
        var jsonGenerator = jsonFactory.createGenerator(outputStream);
        var formatter = new QueryBodyFormatter(jsonGenerator, outputStream);
        try {
            success = formatter.json((singleBodyFormatter) -> {
                singleBodyFormatter.data(result.result());

                var summary = result.result().consume();
                result.timers().notifyResultConsumed();
                if (result.requiresCommit()) {
                    var bookmarks = result.transaction().commit();
                    singleBodyFormatter.metadata(
                            summary,
                            result.timers().resultAvailableAfter(),
                            result.timers().resultConsumedAfter(),
                            bookmarks,
                            null,
                            null,
                            result.requireSummaryCounters());
                } else {
                    result.transaction().extendTimeout();
                    singleBodyFormatter.metadata(
                            summary,
                            result.timers().resultAvailableAfter(),
                            result.timers().resultConsumedAfter(),
                            null,
                            result.transaction().id(),
                            result.transaction().expiresAt(),
                            result.requireSummaryCounters());
                }
            });

        } catch (IOException ex) {
            ExceptionsUnwrapper.unwrapAndThrowNeo4jAndQueryApiExceptions(ex);
            throw new ConnectionException("Failed to write to the connection", ex);
        } finally {
            if (!result.transaction().isOpen() || !success) {
                transactionManager.removeTransaction(result.transaction().id());
            } else {
                transactionManager.releaseTransaction(result.transaction().id());
            }
            jsonGenerator.flush();
        }
    }
}
