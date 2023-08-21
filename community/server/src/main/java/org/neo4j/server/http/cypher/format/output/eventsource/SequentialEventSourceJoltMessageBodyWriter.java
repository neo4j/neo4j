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
import java.util.Map;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.ext.Provider;
import org.neo4j.server.http.cypher.format.jolt.v1.JoltV1Codec;

@Provider
@Produces({
    SequentialEventSourceJoltMessageBodyWriter.JSON_JOLT_MIME_TYPE_VALUE_WITH_QUALITY,
    SequentialEventSourceJoltMessageBodyWriter.JSON_JOLT_MIME_TYPE_VALUE_WITH_QUALITY_V1
})
public class SequentialEventSourceJoltMessageBodyWriter extends AbstractEventSourceJoltMessageBodyWriter {
    public static final String JSON_JOLT_MIME_TYPE_VALUE = "application/vnd.neo4j.jolt+json-seq";
    public static final String JSON_JOLT_MIME_TYPE_VALUE_V1 = "application/vnd.neo4j.jolt-v1+json-seq";
    public static final String JSON_JOLT_MIME_TYPE_VALUE_WITH_QUALITY = JSON_JOLT_MIME_TYPE_VALUE + ";qs=0.5";
    public static final String JSON_JOLT_MIME_TYPE_VALUE_WITH_QUALITY_V1 = JSON_JOLT_MIME_TYPE_VALUE_V1 + ";qs=0.5";
    public static final MediaType JSON_JOLT_MIME_TYPE = MediaType.valueOf(JSON_JOLT_MIME_TYPE_VALUE);

    @Override
    protected MediaType getMediaType() {
        return JSON_JOLT_MIME_TYPE;
    }

    @Override
    protected EventSourceSerializer createSerializer(
            OutputStream outputStream, JsonFactory jsonFactory, Map<String, Object> parameters, boolean strict) {
        return new SequentialEventSourceJoltSerializer(
                parameters,
                JoltV1Codec.class,
                strict,
                jsonFactory,
                outputStream,
                getMediaType().getType());
    }
}
