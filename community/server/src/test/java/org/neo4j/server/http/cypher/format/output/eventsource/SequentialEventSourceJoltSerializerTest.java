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

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonGenerator;
import java.io.ByteArrayOutputStream;
import java.io.OutputStream;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.neo4j.kernel.api.exceptions.Status;
import org.neo4j.kernel.impl.api.KernelTransactionImplementation;
import org.neo4j.kernel.impl.coreapi.InternalTransaction;
import org.neo4j.server.http.cypher.TransitionalTxManagementKernelTransaction;
import org.neo4j.server.http.cypher.format.api.RecordEvent;
import org.neo4j.server.http.cypher.format.jolt.v1.JoltV1Codec;

public class SequentialEventSourceJoltSerializerTest extends AbstractEventSourceJoltSerializerTest {
    private static final JsonFactory JSON_FACTORY =
            new JsonFactory().disable(JsonGenerator.Feature.FLUSH_PASSED_TO_STREAM);

    private final ByteArrayOutputStream output = new ByteArrayOutputStream();
    private SequentialEventSourceJoltSerializer serializer;

    @BeforeEach
    void init() {
        var context = mock(TransitionalTxManagementKernelTransaction.class);
        InternalTransaction internalTransaction = mock(InternalTransaction.class);
        var kernelTransaction = mock(KernelTransactionImplementation.class);

        when(internalTransaction.kernelTransaction()).thenReturn(kernelTransaction);
        when(context.getInternalTransaction()).thenReturn(internalTransaction);
        serializer = getSerializerWith(output);
    }

    @Test
    void shouldSerializeWithRecordSeparator() {
        // given
        var row = Map.of(
                "column1", "value1",
                "column2", "value2");

        // when
        writeStatementStart(serializer, "column1", "column2");
        writeRecord(serializer, row, "column1", "column2");

        var recordEvent = mock(RecordEvent.class);
        when(recordEvent.getValue(any())).thenThrow(new RuntimeException("Stuff went wrong!"));
        when(recordEvent.getColumns()).thenReturn(List.of("column1", "column2"));

        var e = assertThrows(RuntimeException.class, () -> {
            serializer.writeRecord(recordEvent);
        });
        writeError(serializer, Status.Statement.ExecutionFailed, e.getMessage());

        writeTransactionInfo(serializer, "commit/uri/1");
        writeStatementEnd(serializer);

        String result = output.toString(UTF_8);
        assertEquals(
                "\u001E{\"header\":{\"fields\":[\"column1\",\"column2\"]}}\n"
                        + "\u001E{\"data\":[{\"U\":\"value1\"},{\"U\":\"value2\"}]}\n"
                        + "\u001E{\"data\":[]}\n"
                        + "\u001E{\"error\":{\"errors\":["
                        + "{\"code\":{\"U\":\"Neo.DatabaseError.Statement.ExecutionFailed\"},\"message\":{\"U\":\"Stuff went wrong!\"}}"
                        + "]}}\n"
                        + "\u001E{\"info\":{\"commit\":\"commit/uri/1\"}}\n"
                        + "\u001E{\"summary\":{}}\n",
                result);
    }

    @Test
    void shouldReturnDeprecationNotification() {
        // given
        var joltV2Serializer = new SequentialEventSourceJoltSerializer(
                Collections.emptyMap(),
                JoltV1Codec.class,
                true,
                JSON_FACTORY,
                output,
                SequentialEventSourceJoltMessageBodyWriter.JSON_JOLT_MIME_TYPE_VALUE);
        var row = Map.of(
                "column1", "value1",
                "column2", "value2");

        // when
        writeStatementStart(joltV2Serializer, "column1", "column2");
        writeRecord(joltV2Serializer, row, "column1", "column2");
        writeStatementEnd(joltV2Serializer, null, Collections.emptyList());
        writeTransactionInfo(joltV2Serializer, "commit/uri/1");

        // then
        String result = output.toString(UTF_8);

        assertEquals(
                "\u001E{\"header\":{\"fields\":[\"column1\",\"column2\"]}}\n"
                        + "\u001E{\"data\":[{\"U\":\"value1\"},{\"U\":\"value2\"}]}\n"
                        + "\u001E{\"summary\":{}}\n"
                        + "\u001E{\"info\":{\"notifications\":[{\"code\":\"Neo.ClientNotification.Request.DeprecatedFormat\","
                        + "\"severity\":\"WARNING\",\"title\":\"The client made a request for a format which "
                        + "has been deprecated.\","
                        + "\"description\":\"The requested format has been deprecated. "
                        + "('application/vnd.neo4j.jolt+json-seq' and 'application/vnd.neo4j.jolt-v1+json-seq' have "
                        + "been deprecated and will be removed in a future version. "
                        + "Please use 'application/vnd.neo4j.jolt-v2+json-seq'.)\"}],"
                        + "\"commit\":\"commit/uri/1\"}}\n",
                result);
    }

    protected static SequentialEventSourceJoltSerializer getSerializerWith(OutputStream output) {
        return new SequentialEventSourceJoltSerializer(
                Collections.emptyMap(), JoltV1Codec.class, true, JSON_FACTORY, output, null);
    }
}
