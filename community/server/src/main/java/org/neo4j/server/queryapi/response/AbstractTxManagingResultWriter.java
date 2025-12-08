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
package org.neo4j.server.queryapi.response;

import com.fasterxml.jackson.core.JsonFactory;
import java.io.IOException;
import java.io.OutputStream;
import java.lang.annotation.Annotation;
import java.lang.reflect.Type;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.ext.MessageBodyWriter;
import org.neo4j.logging.InternalLog;
import org.neo4j.server.http.cypher.format.api.ConnectionException;
import org.neo4j.server.queryapi.exception.ExceptionsUnwrapper;
import org.neo4j.server.queryapi.request.TxManagedResultContainer;
import org.neo4j.server.queryapi.tx.TransactionManager;

abstract class AbstractTxManagingResultWriter implements MessageBodyWriter<TxManagedResultContainer> {

    private final InternalLog log;
    private final JsonFactory jsonFactory;
    private final TransactionManager transactionManager;

    public AbstractTxManagingResultWriter(
            InternalLog log, JsonFactory jsonFactory, TransactionManager transactionManager) {
        this.log = log;
        this.jsonFactory = jsonFactory;
        this.transactionManager = transactionManager;
    }

    @Override
    public boolean isWriteable(Class<?> type, Type genericType, Annotation[] annotations, MediaType mediaType) {
        return TxManagedResultContainer.class.isAssignableFrom(type);
    }

    public void writeDriverResult(TxManagedResultContainer result, OutputStream outputStream) throws IOException {
        var hasFailed = true;
        var jsonGenerator = jsonFactory.createGenerator(outputStream);
        var resultSerializer = new DriverResultSerializer(jsonGenerator);
        try {
            resultSerializer.writeRecords(result.transaction().retrieveResults());

            if (result.requiresCommit()) {
                var bookmarks = result.transaction().commit();
                resultSerializer.finish(
                        result.transaction().resultSummary(), bookmarks, null, null, result.requireSummaryCounters());
            } else {
                result.transaction().extendTimeout();
                resultSerializer.finish(
                        result.transaction().resultSummary(),
                        null,
                        result.transaction().id(),
                        result.transaction().expiresAt(),
                        result.requireSummaryCounters());
            }
            hasFailed = false;
        } catch (IOException ex) {
            ExceptionsUnwrapper.unwrapAndThrowNeo4jAndQueryApiExceptions(ex);
            throw new ConnectionException("Failed to write to the connection", ex);
        } finally {
            if (!result.transaction().isOpen() || hasFailed) {
                transactionManager.removeTransaction(result.transaction().id());
            } else {
                transactionManager.releaseTransaction(result.transaction().id());
            }
            jsonGenerator.flush();
        }
    }
}
