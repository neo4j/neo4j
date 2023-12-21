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

import static org.neo4j.notifications.NotificationCodeWithDescription.deprecatedFormat;
import static org.neo4j.server.http.cypher.format.api.TransactionNotificationState.COMMITTED;
import static org.neo4j.server.http.cypher.format.api.TransactionNotificationState.OPEN;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonGenerationException;
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.ObjectCodec;
import java.io.IOException;
import java.io.OutputStream;
import java.lang.reflect.InvocationTargetException;
import java.time.Instant;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.neo4j.graphdb.ExecutionPlanDescription;
import org.neo4j.graphdb.InputPosition;
import org.neo4j.graphdb.Notification;
import org.neo4j.graphdb.QueryStatistics;
import org.neo4j.server.http.cypher.format.api.ConnectionException;
import org.neo4j.server.http.cypher.format.api.FailureEvent;
import org.neo4j.server.http.cypher.format.api.OutputEvent;
import org.neo4j.server.http.cypher.format.api.OutputEventSource;
import org.neo4j.server.http.cypher.format.api.RecordEvent;
import org.neo4j.server.http.cypher.format.api.StatementEndEvent;
import org.neo4j.server.http.cypher.format.api.StatementStartEvent;
import org.neo4j.server.http.cypher.format.api.TransactionInfoEvent;
import org.neo4j.server.http.cypher.format.input.json.InputStatement;
import org.neo4j.server.http.cypher.format.input.json.JsonMessageBodyReader;

/**
 * A serializer that serializes {@link OutputEvent OutputEvents} from a {@link OutputEventSource} into a stream of
 * newline separated JSON documents.
 */
class LineDelimitedEventSourceJoltSerializer implements EventSourceSerializer {
    protected final JsonGenerator jsonGenerator;
    protected final List<Notification> notifications = new ArrayList<>();
    protected final List<FailureEvent> errors = new ArrayList<>();
    protected final OutputStream output;
    protected final String deprecatedFormat;

    /**
     * The original parameters from the {@link org.neo4j.server.http.cypher.format.api.OutputEventSource}.
     */
    private final Map<String, Object> parameters;

    private final EventSourceWriter writer;
    private InputStatement inputStatement;

    LineDelimitedEventSourceJoltSerializer(
            Map<String, Object> parameters,
            Class<? extends ObjectCodec> classOfCodec,
            boolean isStrictMode,
            JsonFactory jsonFactory,
            OutputStream output,
            String deprecatedFormat) {
        this.parameters = parameters;
        this.output = output;
        this.writer = new EventSourceWriter();
        this.deprecatedFormat = deprecatedFormat;

        ObjectCodec codec = instantiateCodec(isStrictMode, classOfCodec);
        this.jsonGenerator = createGenerator(jsonFactory, codec, output);
    }

    @Override
    public final void handleEvent(OutputEvent event) {
        switch (event.getType()) {
            case STATEMENT_START:
                StatementStartEvent statementStartEvent = (StatementStartEvent) event;
                InputStatement inputStatement =
                        JsonMessageBodyReader.getInputStatement(parameters, statementStartEvent.getStatement());
                writeStatementStart(statementStartEvent, inputStatement);
                break;
            case RECORD:
                writeRecord((RecordEvent) event);
                break;
            case STATEMENT_END:
                StatementEndEvent statementEndEvent = (StatementEndEvent) event;
                writeStatementEnd(statementEndEvent);
                break;
            case FAILURE:
                FailureEvent failureEvent = (FailureEvent) event;
                writeFailure(failureEvent);
                break;
            case TRANSACTION_INFO:
                TransactionInfoEvent transactionInfoEvent = (TransactionInfoEvent) event;
                writeErrorWrapper();
                writeTransactionInfo(transactionInfoEvent);
                break;
            default:
                throw new IllegalStateException("Unsupported event encountered:" + event.getType());
        }
    }

    private static ObjectCodec instantiateCodec(boolean isStrictMode, Class<? extends ObjectCodec> classOfCodec) {
        try {
            var ctor = classOfCodec.getConstructor(Boolean.TYPE);
            return ctor.newInstance(isStrictMode);
        } catch (NoSuchMethodException
                | InstantiationException
                | IllegalAccessException
                | InvocationTargetException e) {
            throw new IllegalStateException("Failed to create result mapper", e);
        }
    }

    private static JsonGenerator createGenerator(JsonFactory jsonFactory, ObjectCodec codec, OutputStream output) {
        try {
            // we set the RootValueSeparator here to empty string to avoid extra spaces between root objects
            return jsonFactory.copy().setRootValueSeparator("").setCodec(codec).createGenerator(output);
        } catch (IOException e) {
            throw new IllegalStateException("Failed to create JSON generator", e);
        }
    }

    protected void writeStatementStart(StatementStartEvent statementStartEvent, InputStatement inputStatement) {
        this.inputStatement = inputStatement;
        try {
            jsonGenerator.writeStartObject();
            jsonGenerator.writeFieldName("header");
            jsonGenerator.writeStartObject();
            Iterable<String> columns = statementStartEvent.getColumns();
            writeColumns(columns);
            jsonGenerator.writeEndObject();
            jsonGenerator.writeEndObject();
            flush();
        } catch (JsonGenerationException e) {
            throw new IllegalStateException(e);
        } catch (IOException e) {
            throw new ConnectionException("Failed to write to the connection", e);
        }
    }

    protected void writeRecord(RecordEvent recordEvent) {
        try {
            jsonGenerator.writeStartObject();
            jsonGenerator.writeFieldName("data");

            try {
                writer.write(jsonGenerator, recordEvent);
            } finally {
                jsonGenerator.writeEndObject();
                flush();
            }
        } catch (JsonGenerationException e) {
            throw new IllegalStateException(e);
        } catch (IOException e) {
            throw new ConnectionException("Failed to write to the connection", e);
        }
    }

    protected void writeStatementEnd(StatementEndEvent statementEndEvent) {
        try {
            jsonGenerator.writeStartObject();
            jsonGenerator.writeFieldName("summary");
            jsonGenerator.writeStartObject();

            if (inputStatement.includeStats()) {
                writeStats(statementEndEvent.getQueryStatistics());
            }
            if (statementEndEvent.getQueryExecutionType().requestedExecutionPlanDescription()) {
                writeRootPlanDescription(statementEndEvent.getExecutionPlanDescription());
            }

            jsonGenerator.writeEndObject();
            jsonGenerator.writeEndObject();

            statementEndEvent.getNotifications().forEach(notifications::add);

            if (deprecatedFormat != null) {
                notifications.add(deprecationWarning());
            }

            flush();
        } catch (JsonGenerationException e) {
            throw new IllegalStateException(e);
        } catch (IOException e) {
            throw new ConnectionException("Failed to write to the connection", e);
        }
    }

    protected void writeTransactionInfo(TransactionInfoEvent transactionInfoEvent) {
        try {
            jsonGenerator.writeStartObject();
            jsonGenerator.writeFieldName("info");
            jsonGenerator.writeStartObject();

            writeNotifications();

            if (transactionInfoEvent.getCommitUri() != null) {
                jsonGenerator.writeStringField(
                        "commit", transactionInfoEvent.getCommitUri().toString());
            }
            if (transactionInfoEvent.getNotification() == OPEN) {
                jsonGenerator.writeObjectFieldStart("transaction");
                if (transactionInfoEvent.getExpirationTimestamp() >= 0) {
                    String expires = Instant.ofEpochMilli(transactionInfoEvent.getExpirationTimestamp())
                            .atZone(ZoneId.of("GMT"))
                            .format(DateTimeFormatter.RFC_1123_DATE_TIME);
                    jsonGenerator.writeStringField("expires", expires);
                }
                jsonGenerator.writeEndObject();
            }

            if (transactionInfoEvent.getNotification() == COMMITTED) {

                jsonGenerator.writeArrayFieldStart("lastBookmarks");
                jsonGenerator.writeString(transactionInfoEvent.getBookmark());
                jsonGenerator.writeEndArray();
            }

            jsonGenerator.writeEndObject();
            jsonGenerator.writeEndObject();
            flush();
        } catch (JsonGenerationException e) {
            throw new IllegalStateException(e);
        } catch (IOException e) {
            throw new ConnectionException("Failed to write to the connection", e);
        }
    }

    protected void writeFailure(FailureEvent failureEvent) {
        // We collect the errors up to emit before TransactionInfoEvent
        errors.add(failureEvent);
    }

    protected void writeErrorWrapper() {
        if (errors.isEmpty()) {
            return;
        }

        try {
            jsonGenerator.writeStartObject();
            jsonGenerator.writeFieldName("error");
            jsonGenerator.writeStartObject();
            writeErrors();
            jsonGenerator.writeEndObject();
            jsonGenerator.writeEndObject();
            flush();
        } catch (JsonGenerationException e) {
            throw new IllegalStateException(e);
        } catch (IOException e) {
            throw new ConnectionException("Failed to write to the connection", e);
        }
    }

    private void writeNotifications() {
        if (notifications.isEmpty()) {
            return;
        }

        try {
            jsonGenerator.writeArrayFieldStart("notifications");
            try {
                for (Notification notification : notifications) {
                    jsonGenerator.writeStartObject();
                    try {
                        jsonGenerator.writeStringField("code", notification.getCode());
                        jsonGenerator.writeStringField(
                                "severity", notification.getSeverity().toString());
                        jsonGenerator.writeStringField("title", notification.getTitle());
                        jsonGenerator.writeStringField("description", notification.getDescription());
                        writePosition(notification.getPosition());
                    } finally {
                        jsonGenerator.writeEndObject();
                    }
                }
            } finally {
                jsonGenerator.writeEndArray();
            }
        } catch (IOException e) {
            throw new ConnectionException("Failed to write to the response stream", e);
        }
    }

    protected void writePosition(InputPosition position) throws IOException {
        // do not add position if empty
        if (position == InputPosition.empty) {
            return;
        }

        jsonGenerator.writeObjectFieldStart("position");
        try {
            jsonGenerator.writeNumberField("offset", position.getOffset());
            jsonGenerator.writeNumberField("line", position.getLine());
            jsonGenerator.writeNumberField("column", position.getColumn());
        } finally {
            jsonGenerator.writeEndObject();
        }
    }

    private void writeStats(QueryStatistics stats) throws IOException {
        jsonGenerator.writeObjectFieldStart("stats");
        try {
            jsonGenerator.writeBooleanField("contains_updates", stats.containsUpdates());
            jsonGenerator.writeNumberField("nodes_created", stats.getNodesCreated());
            jsonGenerator.writeNumberField("nodes_deleted", stats.getNodesDeleted());
            jsonGenerator.writeNumberField("properties_set", stats.getPropertiesSet());
            jsonGenerator.writeNumberField("relationships_created", stats.getRelationshipsCreated());
            jsonGenerator.writeNumberField("relationship_deleted", stats.getRelationshipsDeleted());
            jsonGenerator.writeNumberField("labels_added", stats.getLabelsAdded());
            jsonGenerator.writeNumberField("labels_removed", stats.getLabelsRemoved());
            jsonGenerator.writeNumberField("indexes_added", stats.getIndexesAdded());
            jsonGenerator.writeNumberField("indexes_removed", stats.getIndexesRemoved());
            jsonGenerator.writeNumberField("constraints_added", stats.getConstraintsAdded());
            jsonGenerator.writeNumberField("constraints_removed", stats.getConstraintsRemoved());
            jsonGenerator.writeBooleanField("contains_system_updates", stats.containsSystemUpdates());
            jsonGenerator.writeNumberField("system_updates", stats.getSystemUpdates());
        } finally {
            jsonGenerator.writeEndObject();
        }
    }

    private void writeRootPlanDescription(ExecutionPlanDescription planDescription) throws IOException {
        jsonGenerator.writeObjectFieldStart("plan");
        try {
            jsonGenerator.writeObjectFieldStart("root");
            try {
                writePlanDescriptionObjectBody(planDescription);
            } finally {
                jsonGenerator.writeEndObject();
            }
        } finally {
            jsonGenerator.writeEndObject();
        }
    }

    private void writePlanDescriptionObjectBody(ExecutionPlanDescription planDescription) throws IOException {
        jsonGenerator.writeFieldName("operatorType");
        jsonGenerator.writeObject(planDescription.getName());
        writePlanArgs(planDescription);
        writePlanIdentifiers(planDescription);

        List<ExecutionPlanDescription> children = planDescription.getChildren();
        jsonGenerator.writeArrayFieldStart("children");
        try {
            for (ExecutionPlanDescription child : children) {
                jsonGenerator.writeStartObject();
                try {
                    writePlanDescriptionObjectBody(child);
                } finally {
                    jsonGenerator.writeEndObject();
                }
            }
        } finally {
            jsonGenerator.writeEndArray();
        }
    }

    private void writePlanArgs(ExecutionPlanDescription planDescription) throws IOException {
        for (Map.Entry<String, Object> entry : planDescription.getArguments().entrySet()) {
            String fieldName = entry.getKey();
            Object fieldValue = entry.getValue();

            jsonGenerator.writeFieldName(fieldName);
            jsonGenerator.writeObject(fieldValue);
        }
    }

    private void writePlanIdentifiers(ExecutionPlanDescription planDescription) throws IOException {
        jsonGenerator.writeArrayFieldStart("identifiers");
        for (String id : planDescription.getIdentifiers()) {
            jsonGenerator.writeObject(id);
        }
        jsonGenerator.writeEndArray();
    }

    private void writeErrors() {
        try {
            jsonGenerator.writeArrayFieldStart("errors");
            try {
                for (FailureEvent error : errors) {
                    try {
                        jsonGenerator.writeStartObject();
                        jsonGenerator.writeObjectField(
                                "code", error.getStatus().code().serialize());
                        jsonGenerator.writeObjectField("message", error.getMessage());
                    } finally {
                        jsonGenerator.writeEndObject();
                    }
                }
            } finally {
                jsonGenerator.writeEndArray();
            }
        } catch (IOException e) {
            throw new ConnectionException("Failed to write to the response stream", e);
        }
    }

    private void writeColumns(Iterable<String> columns) throws IOException {
        try {
            jsonGenerator.writeArrayFieldStart("fields");
            for (String key : columns) {
                jsonGenerator.writeString(key);
            }
        } finally {
            jsonGenerator.writeEndArray();
        }
    }

    private void flush() throws IOException {
        jsonGenerator.flush();
        output.flush();
        output.write("\n".getBytes());
    }

    protected Notification deprecationWarning() {
        return deprecationWarning(
                LineDelimitedEventSourceJoltMessageBodyWriter.JSON_JOLT_MIME_TYPE_VALUE,
                LineDelimitedEventSourceJoltMessageBodyWriter.JSON_JOLT_MIME_TYPE_VALUE_V1,
                deprecatedFormat,
                LineDelimitedEventSourceJoltV2MessageBodyWriter.JSON_JOLT_MIME_TYPE_VALUE_V2);
    }

    public static Notification deprecationWarning(
            String deprecatedFormatA, String deprecatedFormatB, String actualDeprecatedFormat, String newFormat) {
        return deprecatedFormat(
                InputPosition.empty,
                String.format(
                        "'%s' and '%s' have been deprecated and will be removed in a future version. "
                                + "Please use '%s'.",
                        deprecatedFormatA, deprecatedFormatB, newFormat),
                actualDeprecatedFormat,
                newFormat);
    }
}
