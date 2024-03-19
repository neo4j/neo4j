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
import javax.ws.rs.core.MediaType;
import javax.ws.rs.ext.MessageBodyWriter;
import org.neo4j.driver.exceptions.Neo4jException;
import org.neo4j.logging.InternalLog;
import org.neo4j.server.http.cypher.format.api.ConnectionException;
import org.neo4j.server.httpv2.request.ResultContainer;

abstract class AbstractDriverResultWriter implements MessageBodyWriter<ResultContainer> {

    private final InternalLog log;

    public AbstractDriverResultWriter(InternalLog log) {
        this.log = log;
    }

    @Override
    public boolean isWriteable(Class<?> type, Type genericType, Annotation[] annotations, MediaType mediaType) {
        return ResultContainer.class.isAssignableFrom(type);
    }

    public void writeDriverResult(JsonFactory factory, ResultContainer result, OutputStream outputStream)
            throws IOException {
        var jsonGenerator = factory.createGenerator(outputStream);

        var resultSerializer = new DriverResultSerializer(jsonGenerator);

        try (var session = result.session()) {
            resultSerializer.writeFieldNames(result.result().keys());

            while (result.result().hasNext()) {
                resultSerializer.writeValue(result.result().next());
            }

            resultSerializer.finish(result.result().consume(), session.lastBookmarks(), result.queryRequest());

        } catch (Neo4jException ex) {
            try {
                resultSerializer.writeError(ex);
            } catch (IOException errorWritingException) {
                // We have errored during writing an error implying the connection has disappeared during writing.
                // We simply log in this case.
                log.warn("An error was thrown whilst attempting to write an error.", errorWritingException);
            }
        } catch (IOException ex) {
            throw new ConnectionException("Failed to write to the connection", ex);
        } finally {
            jsonGenerator.flush();
        }
    }
}
