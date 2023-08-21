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
import com.fasterxml.jackson.core.ObjectCodec;
import java.io.IOException;
import java.io.OutputStream;
import java.util.Map;
import org.neo4j.graphdb.Notification;
import org.neo4j.server.http.cypher.format.api.ConnectionException;
import org.neo4j.server.http.cypher.format.api.OutputEvent;
import org.neo4j.server.http.cypher.format.api.OutputEventSource;
import org.neo4j.server.http.cypher.format.api.RecordEvent;
import org.neo4j.server.http.cypher.format.api.StatementEndEvent;
import org.neo4j.server.http.cypher.format.api.StatementStartEvent;
import org.neo4j.server.http.cypher.format.api.TransactionInfoEvent;
import org.neo4j.server.http.cypher.format.input.json.InputStatement;

/**
 * A serializer that serializes {@link OutputEvent OutputEvents} from a {@link OutputEventSource} into a stream of JSON documents in accordance with RFC 7464.
 */
public class SequentialEventSourceJoltSerializer extends LineDelimitedEventSourceJoltSerializer {

    private static final byte RECORD_SEPARATOR = 0x1E;

    public SequentialEventSourceJoltSerializer(
            Map<String, Object> parameters,
            Class<? extends ObjectCodec> classOfCodec,
            boolean isStrictMode,
            JsonFactory jsonFactory,
            OutputStream output,
            String deprecatedFormat) {
        super(parameters, classOfCodec, isStrictMode, jsonFactory, output, deprecatedFormat);
    }

    private void writeRecordSeparator() {
        try {
            this.output.write(RECORD_SEPARATOR);
        } catch (IOException e) {
            throw new ConnectionException("Failed to write to the connection", e);
        }
    }

    @Override
    protected void writeStatementStart(StatementStartEvent statementStartEvent, InputStatement inputStatement) {
        writeRecordSeparator();

        super.writeStatementStart(statementStartEvent, inputStatement);
    }

    @Override
    protected void writeStatementEnd(StatementEndEvent statementEndEvent) {
        writeRecordSeparator();

        super.writeStatementEnd(statementEndEvent);
    }

    @Override
    protected void writeRecord(RecordEvent recordEvent) {
        writeRecordSeparator();

        super.writeRecord(recordEvent);
    }

    @Override
    protected void writeTransactionInfo(TransactionInfoEvent transactionInfoEvent) {
        writeRecordSeparator();

        super.writeTransactionInfo(transactionInfoEvent);
    }

    @Override
    protected void writeErrorWrapper() {
        if (errors.isEmpty()) {
            return;
        }

        writeRecordSeparator();

        super.writeErrorWrapper();
    }

    @Override
    protected Notification deprecationWarning() {
        return deprecationWarning(
                SequentialEventSourceJoltMessageBodyWriter.JSON_JOLT_MIME_TYPE_VALUE,
                SequentialEventSourceJoltMessageBodyWriter.JSON_JOLT_MIME_TYPE_VALUE_V1,
                deprecatedFormat,
                SequentialEventSourceJoltV2MessageBodyWriter.JSON_JOLT_MIME_TYPE_VALUE_V2);
    }
}
