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
import org.neo4j.driver.exceptions.Neo4jException;
import org.neo4j.logging.InternalLog;
import org.neo4j.server.queryapi.exception.ExceptionsUnwrapper;
import org.neo4j.server.queryapi.exception.QueryApiException;
import org.neo4j.server.queryapi.request.AutoCommitResultContainer;
import org.neo4j.server.queryapi.response.error.HttpErrorResponse;

abstract class AbstractDriverResultWriter implements MessageBodyWriter<AutoCommitResultContainer> {

    private final InternalLog log;

    public AbstractDriverResultWriter(InternalLog log) {
        this.log = log;
    }

    @Override
    public boolean isWriteable(Class<?> type, Type genericType, Annotation[] annotations, MediaType mediaType) {
        return AutoCommitResultContainer.class.isAssignableFrom(type);
    }

    public void writeDriverResult(JsonFactory factory, AutoCommitResultContainer result, OutputStream outputStream)
            throws IOException {
        HttpErrorResponse errorResponse = null;
        var jsonGenerator = factory.createGenerator(outputStream);
        var resultSerializer = new DriverResultSerializer(jsonGenerator);

        try (var session = result.session()) {
            resultSerializer.writeRecords(result.result());
            var resultSummary = result.result().consume();
            resultSerializer.finish(
                    resultSummary,
                    session.lastBookmarks(),
                    result.queryRequest().includeCounters());
        } catch (IOException ex) {
            errorResponse = ExceptionsUnwrapper.transformNeo4jAndQueryApiExceptions(
                    HttpErrorResponse::fromDriverException, HttpErrorResponse::fromQueryApiException, ex);
        } catch (Neo4jException neo4jException) {
            errorResponse = HttpErrorResponse.fromDriverException(neo4jException);
        } catch (QueryApiException queryApiException) {
            errorResponse = HttpErrorResponse.fromQueryApiException(queryApiException);
        } finally {
            if (errorResponse != null) {
                resultSerializer.writeError(errorResponse);
            }
            jsonGenerator.flush();
        }
    }
}
