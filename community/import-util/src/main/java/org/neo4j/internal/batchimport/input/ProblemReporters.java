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
package org.neo4j.internal.batchimport.input;

import static java.lang.String.format;
import static org.neo4j.internal.batchimport.input.BadCollector.ALL_SCHEMA_VIOLATIONS;
import static org.neo4j.internal.batchimport.input.BadCollector.BAD_RELATIONSHIP;
import static org.neo4j.internal.batchimport.input.BadCollector.DATA_AFTER_QUOTE;
import static org.neo4j.internal.batchimport.input.BadCollector.DUPLICATE_NODES;
import static org.neo4j.internal.batchimport.input.BadCollector.EXTRA_COLUMNS;
import static org.neo4j.internal.batchimport.input.BadCollector.ILLEGAL_QUOTE;
import static org.neo4j.internal.batchimport.input.BadCollector.INVALID_NODE_ID;
import static org.neo4j.internal.batchimport.input.BadCollector.INVALID_RELATIONSHIP_ID;
import static org.neo4j.internal.batchimport.input.BadCollector.MISSING_ID_COLUMN;
import static org.neo4j.internal.batchimport.input.BadCollector.NODE_SCHEMA_VIOLATIONS;
import static org.neo4j.internal.batchimport.input.BadCollector.OTHER_NODE_VIOLATION;
import static org.neo4j.internal.batchimport.input.BadCollector.OTHER_RELATIONSHIP_VIOLATION;
import static org.neo4j.internal.batchimport.input.BadCollector.REL_SCHEMA_VIOLATIONS;
import static org.neo4j.internal.batchimport.input.BadCollector.VIOLATING_NODES;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializerProvider;
import com.fasterxml.jackson.databind.module.SimpleModule;
import com.fasterxml.jackson.databind.ser.std.StdSerializer;
import java.io.IOException;
import java.io.OutputStream;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.Optional;
import java.util.TreeMap;
import org.neo4j.batchimport.api.input.Collector;
import org.neo4j.batchimport.api.input.Group;
import org.neo4j.common.EntityType;
import org.neo4j.internal.batchimport.cache.idmapping.string.DuplicateInputIdException;
import org.neo4j.internal.batchimport.input.BadCollector.ProblemHandler;
import org.neo4j.internal.batchimport.input.BadCollector.ProblemReporter;
import org.neo4j.internal.batchimport.input.csv.Type;
import org.neo4j.io.fs.FileUtils;
import org.neo4j.io.fs.StoreChannel;

public class ProblemReporters {

    private static final SimpleModule SERIALIZERS = new SimpleModule()
            .addSerializer(RelationshipsProblemReporter.SERIALIZER)
            .addSerializer(NodesProblemReporter.SERIALIZER)
            .addSerializer(ExtraColumnsProblemReporter.SERIALIZER)
            .addSerializer(EntityViolatingConstraintReporter.SERIALIZER)
            .addSerializer(RelationshipViolatingConstraintReporter.SERIALIZER)
            .addSerializer(SchemaCommandFailureReporter.SERIALIZER)
            .addSerializer(OtherViolationReporter.SERIALIZER)
            .addSerializer(DataAfterQuoteProblemReporter.SERIALIZER)
            .addSerializer(IllegalQuoteProblemReporter.SERIALIZER)
            .addSerializer(InvalidIdProblemReporter.SERIALIZER)
            .addSerializer(IdColumnMissingProblemReporter.SERIALIZER);

    /**
     * Prints the {@link ProblemReporter#message()} of any reported errors to the provided {@link OutputStream}
     * @param output the output to print to
     * @return the handler for a {@link Collector} to use when receiving any errors
     */
    public static ProblemHandler printingProblemHandler(OutputStream output) {
        return new ProblemHandler() {
            @Override
            public void handle(ProblemReporter reporter) {
                try {
                    output.write((reporter.message() + "\n").getBytes(StandardCharsets.UTF_8));
                } catch (IOException ex) {
                    throw new UncheckedIOException(ex);
                }
            }

            @Override
            public long checkpoint() throws IOException {
                // An OutputStream can be flushed but not forced. But this implementation is only used in tests.
                output.flush();
                // No meaningful position to report, since this handler cannot be resumed
                return 0;
            }

            @Override
            public void resumeFromCheckpoint(long position) {
                throw new UnsupportedOperationException(
                        "A report written to a plain OutputStream cannot be truncated back to position " + position);
            }

            @Override
            public void close() throws IOException {
                try (output) {
                    output.flush();
                }
            }
        };
    }

    /**
     * Prints the {@link ProblemReporter} to the provided {@link StoreChannel} as JSON objects. The handler takes
     * ownership of the channel, i.e. {@link ProblemHandler#close() closing} the handler closes the channel.
     * @param channel the channel to print to
     * @return the handler for a {@link Collector} to use when receiving any errors
     */
    public static ProblemHandler jsonOutputProblemHandler(StoreChannel channel) {
        var mapper = new ObjectMapper()
                .disable(JsonGenerator.Feature.AUTO_CLOSE_TARGET)
                .disable(JsonGenerator.Feature.FLUSH_PASSED_TO_STREAM)
                .registerModule(ProblemReporters.SERIALIZERS);
        OutputStream output = FileUtils.toBufferedStream(channel);
        return new ProblemHandler() {
            @Override
            public void handle(ProblemReporter reporter) {
                try {
                    mapper.writeValue(output, reporter);
                    output.write("\n".getBytes(StandardCharsets.UTF_8));
                } catch (IOException ex) {
                    throw new UncheckedIOException(ex);
                }
            }

            @Override
            public long checkpoint() throws IOException {
                // Flush first, so that the buffered bytes have reached the channel before it is forced.
                output.flush();
                channel.force(false);
                // Not position(), which is 0 until the first write on a channel opened for appending
                return channel.size();
            }

            @Override
            public void resumeFromCheckpoint(long position) throws IOException {
                long size = channel.size();
                if (size < position) {
                    throw new IOException(format(
                            "Cannot resume the bad entry report from position %d, it only reaches %d", position, size));
                }
                channel.truncate(position);
            }

            @Override
            public void close() throws IOException {
                try (output) {
                    output.flush();
                }
            }
        };
    }

    static ProblemReporter relationshipsProblemReporter(
            Object startId,
            Group startIdGroup,
            Object relType,
            Object endId,
            Group endIdGroup,
            Object specificValue,
            String source,
            long lineNumber) {
        return new RelationshipsProblemReporter(
                startId, startIdGroup, relType, endId, endIdGroup, specificValue, source, lineNumber);
    }

    static ProblemReporter nodesProblemReporter(Object id, Group group, String source, long lineNumber) {
        return new NodesProblemReporter(id, group, source, lineNumber);
    }

    static ProblemReporter collectExtraColumnsReporter(String source, long row, String value) {
        return new ExtraColumnsProblemReporter(source, row, value);
    }

    static ProblemReporter entityViolatingConstraintReporter(
            Object id,
            long actualId,
            Map<String, Object> properties,
            String constraintDescription,
            EntityType entityType,
            String sourceDescription,
            long lineNumber) {
        return new EntityViolatingConstraintReporter(
                id, actualId, properties, constraintDescription, entityType, sourceDescription, lineNumber);
    }

    static ProblemReporter relationshipViolatingConstraintReporter(
            Map<String, Object> properties,
            String constraintDescription,
            Object startId,
            Group startIdGroup,
            String type,
            Object endId,
            Group endIdGroup,
            String sourceDescription,
            long lineNumber) {
        return new RelationshipViolatingConstraintReporter(
                properties,
                constraintDescription,
                startId,
                startIdGroup,
                type,
                endId,
                endIdGroup,
                sourceDescription,
                lineNumber);
    }

    static ProblemReporter schemaCommandFailureReporter(EntityType entityType, String failureMessage) {
        return new SchemaCommandFailureReporter(entityType, failureMessage);
    }

    static ProblemReporter otherViolationReporter(EntityType entityType, String problem) {
        return new OtherViolationReporter(entityType, problem);
    }

    public static ProblemReporter dataAfterQuoteReporter(String source, long row, String value) {
        return new DataAfterQuoteProblemReporter(source, row, value);
    }

    public static ProblemReporter illegalQuoteReporter(String source, long row, String value) {
        return new IllegalQuoteProblemReporter(source, row, value);
    }

    public static ProblemReporter invalidIdReporter(String source, long row, String value, EntityType entityType) {
        return new InvalidIdProblemReporter(source, row, value, entityType);
    }

    public static ProblemReporter idColumnMissingReporter(String source, long row, int columnIndex) {
        return new IdColumnMissingProblemReporter(source, row, columnIndex);
    }

    private static class RelationshipsProblemReporter extends ProblemReporter {

        private static final StdSerializer<RelationshipsProblemReporter> SERIALIZER =
                new StdSerializer<>(RelationshipsProblemReporter.class) {
                    @Override
                    public void serialize(
                            RelationshipsProblemReporter reporter,
                            JsonGenerator jsonGenerator,
                            SerializerProvider serializerProvider)
                            throws IOException {
                        jsonGenerator.writeStartObject();
                        startReport(jsonGenerator, reporter);
                        writeSource(jsonGenerator, reporter.source, reporter.lineNumber);
                        writeRelationship(jsonGenerator, reporter);
                        jsonGenerator.writeEndObject();
                    }

                    private void writeRelationship(JsonGenerator jsonGenerator, RelationshipsProblemReporter reporter)
                            throws IOException {
                        jsonGenerator.writeFieldName("relationship");
                        jsonGenerator.writeStartObject();
                        writeField(jsonGenerator, "type", reporter.relType);
                        var missing = reporter.relType == null;
                        missing |= writeNodeObject(jsonGenerator, "source", reporter.startId, reporter.startIdGroup);
                        missing |= writeNodeObject(jsonGenerator, "target", reporter.endId, reporter.endIdGroup);
                        if (!missing && reporter.specificValue != null) {
                            jsonGenerator.writeStringField("invalid", reporter.specificValue.toString());
                        } else {
                            jsonGenerator.writeBooleanField("missing", true);
                        }
                        jsonGenerator.writeEndObject();
                    }
                };

        private final Object specificValue;
        private final Object startId;
        private final Group startIdGroup;
        private final Object relType;
        private final Object endId;
        private final Group endIdGroup;
        private final String source;
        private final long lineNumber;

        private String message;

        private RelationshipsProblemReporter(
                Object startId,
                Group startIdGroup,
                Object relType,
                Object endId,
                Group endIdGroup,
                Object specificValue,
                String source,
                long lineNumber) {
            super(BAD_RELATIONSHIP);
            this.startId = startId;
            this.startIdGroup = startIdGroup;
            this.relType = relType;
            this.endId = endId;
            this.endIdGroup = endIdGroup;
            this.specificValue = specificValue;
            this.source = source;
            this.lineNumber = lineNumber;
        }

        @Override
        public String message() {
            return getReportMessage();
        }

        @Override
        public InputException exception() {
            Optional<Type> maybeMissingDataField = getMissingDataField();
            if (maybeMissingDataField.isPresent()) {
                return new MissingRelationshipDataException(maybeMissingDataField.get(), getReportMessage(true));
            } else {
                return new InputException(getReportMessage());
            }
        }

        private String getReportMessage(boolean missingData) {
            if (message == null) {
                if (missingData) {
                    message = Collector.standardisedErrorMessage(
                            "Invalid relationship in import data",
                            source,
                            lineNumber,
                            format(
                                    "%s is missing data",
                                    Collector.illustrateRelationship(
                                            startId, startIdGroup, relType, endId, endIdGroup)));
                } else {
                    message = Collector.standardisedErrorMessage(
                            "Invalid relationship in import data",
                            source,
                            lineNumber,
                            format(
                                    "%s referring to missing node %s",
                                    Collector.illustrateRelationship(startId, startIdGroup, relType, endId, endIdGroup),
                                    specificValue));
                }
            }
            return message;
        }

        private String getReportMessage() {
            return getReportMessage(getMissingDataField().isPresent());
        }

        // Returns the first data field that is missing, or null if none are missing
        private Optional<Type> getMissingDataField() {
            if (startId == null) {
                return Optional.of(Type.START_ID);
            } else if (endId == null) {
                return Optional.of(Type.END_ID);
            } else if (relType == null) {
                return Optional.of(Type.TYPE);
            }
            return Optional.empty();
        }
    }

    private static class NodesProblemReporter extends ProblemReporter {

        private static final StdSerializer<NodesProblemReporter> SERIALIZER =
                new StdSerializer<>(NodesProblemReporter.class) {
                    @Override
                    public void serialize(
                            NodesProblemReporter reporter,
                            JsonGenerator jsonGenerator,
                            SerializerProvider serializerProvider)
                            throws IOException {
                        jsonGenerator.writeStartObject();
                        startReport(jsonGenerator, reporter);
                        writeSource(jsonGenerator, reporter.source, reporter.lineNumber);
                        writeNode(jsonGenerator, reporter);
                        jsonGenerator.writeEndObject();
                    }

                    private void writeNode(JsonGenerator jsonGenerator, NodesProblemReporter reporter)
                            throws IOException {
                        jsonGenerator.writeFieldName("node");
                        jsonGenerator.writeStartObject();
                        writeField(jsonGenerator, "id", reporter.id);
                        writeGroup(jsonGenerator, reporter.group);
                        jsonGenerator.writeEndObject();
                    }
                };

        private final Object id;
        private final Group group;
        private final String source;
        private final long lineNumber;

        private NodesProblemReporter(Object id, Group group, String source, long lineNumber) {
            super(DUPLICATE_NODES);
            this.id = id;
            this.group = group;
            this.source = source;
            this.lineNumber = lineNumber;
        }

        @Override
        public String message() {
            return DuplicateInputIdException.message(id, group, source, lineNumber);
        }

        @Override
        public InputException exception() {
            return new DuplicateInputIdException(id, group, source, lineNumber);
        }
    }

    private static class ExtraColumnsProblemReporter extends ProblemReporter {

        private static final StdSerializer<ExtraColumnsProblemReporter> SERIALIZER =
                new StdSerializer<>(ExtraColumnsProblemReporter.class) {
                    @Override
                    public void serialize(
                            ExtraColumnsProblemReporter reporter,
                            JsonGenerator jsonGenerator,
                            SerializerProvider serializerProvider)
                            throws IOException {
                        jsonGenerator.writeStartObject();
                        startReport(jsonGenerator, reporter);
                        writeSource(jsonGenerator, reporter.source, reporter.row);
                        writeField(jsonGenerator, "extraValue", reporter.value);
                        jsonGenerator.writeEndObject();
                    }
                };

        private final long row;
        private final String source;
        private final String value;

        private String message;

        private ExtraColumnsProblemReporter(String source, long row, String value) {
            super(EXTRA_COLUMNS);
            this.row = row;
            this.source = source;
            this.value = value;
        }

        @Override
        public String message() {
            return getReportMessage();
        }

        @Override
        public InputException exception() {
            return new InputException(getReportMessage());
        }

        private String getReportMessage() {
            if (message == null) {
                message = Collector.extraColumnsMessage(source, row, value);
            }
            return message;
        }
    }

    private static class EntityViolatingConstraintReporter extends ProblemReporter {

        private static final StdSerializer<EntityViolatingConstraintReporter> SERIALIZER =
                new StdSerializer<>(EntityViolatingConstraintReporter.class) {
                    @Override
                    public void serialize(
                            EntityViolatingConstraintReporter reporter,
                            JsonGenerator jsonGenerator,
                            SerializerProvider serializerProvider)
                            throws IOException {
                        jsonGenerator.writeStartObject();
                        startReport(jsonGenerator, reporter);
                        writeSource(jsonGenerator, reporter.sourceDescription, reporter.lineNumber);
                        writeField(jsonGenerator, "constraint", reporter.constraintDescription);
                        writeEntity(jsonGenerator, reporter);
                        jsonGenerator.writeEndObject();
                    }

                    private void writeEntity(JsonGenerator jsonGenerator, EntityViolatingConstraintReporter reporter)
                            throws IOException {
                        jsonGenerator.writeFieldName(reporter.entityType == EntityType.NODE ? "node" : "relationship");
                        jsonGenerator.writeStartObject();
                        writeField(jsonGenerator, "entityId", reporter.id);
                        writeField(jsonGenerator, "dbId", reporter.actualId);
                        writeProperties(jsonGenerator, reporter.properties);
                        jsonGenerator.writeEndObject();
                    }
                };

        private final Object id;
        private final long actualId;
        private final Map<String, Object> properties;
        private final String constraintDescription;
        private final EntityType entityType;
        private final String sourceDescription;
        private final long lineNumber;

        private EntityViolatingConstraintReporter(
                Object id,
                long actualId,
                Map<String, Object> properties,
                String constraintDescription,
                EntityType entityType,
                String sourceDescription,
                long lineNumber) {
            super(entityType == EntityType.NODE ? VIOLATING_NODES : BAD_RELATIONSHIP);
            this.id = id;
            this.actualId = actualId;
            this.properties = properties;
            this.constraintDescription = constraintDescription;
            this.entityType = entityType;
            this.sourceDescription = sourceDescription;
            this.lineNumber = lineNumber;
        }

        @Override
        String message() {
            return Collector.entityViolatingConstraintMessage(
                    id, actualId, properties, constraintDescription, entityType, sourceDescription, lineNumber);
        }

        @Override
        InputException exception() {
            return new InputException(message());
        }
    }

    private static class RelationshipViolatingConstraintReporter extends ProblemReporter {

        private static final StdSerializer<RelationshipViolatingConstraintReporter> SERIALIZER =
                new StdSerializer<>(RelationshipViolatingConstraintReporter.class) {
                    @Override
                    public void serialize(
                            RelationshipViolatingConstraintReporter reporter,
                            JsonGenerator jsonGenerator,
                            SerializerProvider serializerProvider)
                            throws IOException {
                        jsonGenerator.writeStartObject();
                        startReport(jsonGenerator, reporter);
                        writeSource(jsonGenerator, reporter.sourceDescription, reporter.lineNumber);
                        writeField(jsonGenerator, "constraint", reporter.constraintDescription);
                        writeEntity(jsonGenerator, reporter);
                        jsonGenerator.writeEndObject();
                    }

                    private void writeEntity(
                            JsonGenerator jsonGenerator, RelationshipViolatingConstraintReporter reporter)
                            throws IOException {
                        jsonGenerator.writeFieldName("relationship");
                        jsonGenerator.writeStartObject();
                        writeField(jsonGenerator, "type", reporter.type);
                        writeNodeObject(jsonGenerator, "source", reporter.startId, reporter.startIdGroup);
                        writeNodeObject(jsonGenerator, "target", reporter.endId, reporter.endIdGroup);
                        writeProperties(jsonGenerator, reporter.properties);
                        jsonGenerator.writeEndObject();
                    }
                };

        private final Map<String, Object> properties;
        private final String constraintDescription;
        private final Object startId;
        private final Group startIdGroup;
        private final String type;
        private final Object endId;
        private final Group endIdGroup;
        private final String sourceDescription;
        private final long lineNumber;

        private RelationshipViolatingConstraintReporter(
                Map<String, Object> properties,
                String constraintDescription,
                Object startId,
                Group startIdGroup,
                String type,
                Object endId,
                Group endIdGroup,
                String sourceDescription,
                long lineNumber) {
            super(BAD_RELATIONSHIP);
            this.properties = properties;
            this.constraintDescription = constraintDescription;
            this.startId = startId;
            this.startIdGroup = startIdGroup;
            this.type = type;
            this.endId = endId;
            this.endIdGroup = endIdGroup;
            this.sourceDescription = sourceDescription;
            this.lineNumber = lineNumber;
        }

        @Override
        String message() {
            return Collector.relationshipViolatingConstraintMessage(
                    properties,
                    constraintDescription,
                    startId,
                    startIdGroup,
                    type,
                    endId,
                    endIdGroup,
                    sourceDescription,
                    lineNumber);
        }

        @Override
        InputException exception() {
            return new InputException(message());
        }
    }

    private static class SchemaCommandFailureReporter extends ProblemReporter {

        private static final StdSerializer<SchemaCommandFailureReporter> SERIALIZER =
                new StdSerializer<>(SchemaCommandFailureReporter.class) {
                    @Override
                    public void serialize(
                            SchemaCommandFailureReporter reporter,
                            JsonGenerator jsonGenerator,
                            SerializerProvider serializerProvider)
                            throws IOException {
                        jsonGenerator.writeStartObject();
                        startReport(jsonGenerator, reporter);
                        jsonGenerator.writeEndObject();
                    }
                };

        private final String failureMessage;

        private SchemaCommandFailureReporter(EntityType entityType, String failureMessage) {
            super(violationType(entityType));
            this.failureMessage = failureMessage;
        }

        private static int violationType(EntityType entityType) {
            if (entityType == null) {
                // just collect them all in this case as we don't know which one it was
                return ALL_SCHEMA_VIOLATIONS;
            }

            return entityType == EntityType.NODE ? NODE_SCHEMA_VIOLATIONS : REL_SCHEMA_VIOLATIONS;
        }

        @Override
        String message() {
            return failureMessage;
        }

        @Override
        InputException exception() {
            return new InputException(message());
        }
    }

    private static class OtherViolationReporter extends ProblemReporter {

        private static final StdSerializer<OtherViolationReporter> SERIALIZER =
                new StdSerializer<>(OtherViolationReporter.class) {
                    @Override
                    public void serialize(
                            OtherViolationReporter reporter,
                            JsonGenerator jsonGenerator,
                            SerializerProvider serializerProvider)
                            throws IOException {
                        jsonGenerator.writeStartObject();
                        startReport(jsonGenerator, reporter);
                        jsonGenerator.writeEndObject();
                    }
                };

        private final String problem;

        public OtherViolationReporter(EntityType entityType, String problem) {
            super(entityType == EntityType.NODE ? OTHER_NODE_VIOLATION : OTHER_RELATIONSHIP_VIOLATION);
            this.problem = problem;
        }

        @Override
        String message() {
            return problem;
        }

        @Override
        InputException exception() {
            return new InputException(message());
        }
    }

    private static class DataAfterQuoteProblemReporter extends ProblemReporter {

        private static final StdSerializer<DataAfterQuoteProblemReporter> SERIALIZER =
                new StdSerializer<>(DataAfterQuoteProblemReporter.class) {
                    @Override
                    public void serialize(
                            DataAfterQuoteProblemReporter reporter,
                            JsonGenerator jsonGenerator,
                            SerializerProvider serializerProvider)
                            throws IOException {
                        jsonGenerator.writeStartObject();
                        startReport(jsonGenerator, reporter);
                        writeSource(jsonGenerator, reporter.source, reporter.row);
                        writeField(jsonGenerator, "value", reporter.value);
                        jsonGenerator.writeEndObject();
                    }
                };

        private final String source;
        private final long row;
        private final String value;

        private DataAfterQuoteProblemReporter(String source, long row, String value) {
            super(DATA_AFTER_QUOTE);
            this.source = source;
            this.row = row;
            this.value = value;
        }

        @Override
        public String message() {
            return Collector.dataAfterQuoteMessage(source, row, value);
        }

        @Override
        public InputException exception() {
            return new InputException(message());
        }
    }

    private static class IllegalQuoteProblemReporter extends ProblemReporter {

        private static final StdSerializer<IllegalQuoteProblemReporter> SERIALIZER =
                new StdSerializer<>(IllegalQuoteProblemReporter.class) {
                    @Override
                    public void serialize(
                            IllegalQuoteProblemReporter reporter,
                            JsonGenerator jsonGenerator,
                            SerializerProvider serializerProvider)
                            throws IOException {
                        jsonGenerator.writeStartObject();
                        startReport(jsonGenerator, reporter);
                        writeSource(jsonGenerator, reporter.source, reporter.row);
                        writeField(jsonGenerator, "value", reporter.value);
                        jsonGenerator.writeEndObject();
                    }
                };

        private final String source;
        private final long row;
        private final String value;

        private IllegalQuoteProblemReporter(String source, long row, String value) {
            super(ILLEGAL_QUOTE);
            this.source = source;
            this.row = row;
            this.value = value;
        }

        @Override
        public String message() {
            return Collector.illegalQuoteMessage(source, row, value);
        }

        @Override
        public InputException exception() {
            return new InputException(message());
        }
    }

    private static class InvalidIdProblemReporter extends ProblemReporter {

        private static final StdSerializer<InvalidIdProblemReporter> SERIALIZER =
                new StdSerializer<>(InvalidIdProblemReporter.class) {
                    @Override
                    public void serialize(
                            InvalidIdProblemReporter reporter,
                            JsonGenerator jsonGenerator,
                            SerializerProvider serializerProvider)
                            throws IOException {
                        jsonGenerator.writeStartObject();
                        startReport(jsonGenerator, reporter);
                        writeSource(jsonGenerator, reporter.source, reporter.row);
                        writeField(jsonGenerator, "value", reporter.value);
                        jsonGenerator.writeEndObject();
                    }
                };

        private final String source;
        private final long row;
        private final String value;

        private InvalidIdProblemReporter(String source, long row, String value, EntityType entityType) {
            super(entityType == EntityType.NODE ? INVALID_NODE_ID : INVALID_RELATIONSHIP_ID);
            this.source = source;
            this.row = row;
            this.value = value;
        }

        @Override
        public String message() {
            return Collector.invalidIDMessage(source, row, value);
        }

        @Override
        public InputException exception() {
            return new InputException(message());
        }
    }

    private static class IdColumnMissingProblemReporter extends ProblemReporter {

        private static final StdSerializer<IdColumnMissingProblemReporter> SERIALIZER =
                new StdSerializer<>(IdColumnMissingProblemReporter.class) {
                    @Override
                    public void serialize(
                            IdColumnMissingProblemReporter reporter,
                            JsonGenerator jsonGenerator,
                            SerializerProvider serializerProvider)
                            throws IOException {
                        jsonGenerator.writeStartObject();
                        startReport(jsonGenerator, reporter);
                        writeSource(jsonGenerator, reporter.source, reporter.row);
                        writeField(jsonGenerator, "columnIndex", reporter.columnIndex);
                        jsonGenerator.writeEndObject();
                    }
                };

        private final String source;
        private final long row;
        private final int columnIndex;

        private String message;

        private IdColumnMissingProblemReporter(String source, long row, int columnIndex) {
            super(MISSING_ID_COLUMN);
            this.source = source;
            this.row = row;
            this.columnIndex = columnIndex;
        }

        @Override
        public String message() {
            return getReportMessage();
        }

        @Override
        public InputException exception() {
            return new InputException(getReportMessage());
        }

        private String getReportMessage() {
            if (message == null) {
                message = Collector.idColumnMissingMessage(source, row, columnIndex);
            }
            return message;
        }
    }

    private static void startReport(JsonGenerator jsonGenerator, ProblemReporter reporter) throws IOException {
        jsonGenerator.writeStringField("problem", reporter.typeKey());
        jsonGenerator.writeStringField("message", reporter.message().replaceAll("\r\n", "\n"));
    }

    private static void writeSource(JsonGenerator jsonGenerator, String source, long line) throws IOException {
        jsonGenerator.writeStringField("source", source);
        jsonGenerator.writeNumberField("line", line);
    }

    private static void writeField(JsonGenerator jsonGenerator, String field, Object value) throws IOException {
        jsonGenerator.writeStringField(field, value == null ? null : value.toString());
    }

    private static void writeGroup(JsonGenerator jsonGenerator, Group group) throws IOException {
        writeField(jsonGenerator, "group", group == null ? null : group.name());
    }

    private static boolean writeNodeObject(JsonGenerator jsonGenerator, String field, Object id, Group idGroup)
            throws IOException {
        jsonGenerator.writeFieldName(field);
        jsonGenerator.writeStartObject();
        writeField(jsonGenerator, "id", id);
        writeGroup(jsonGenerator, idGroup);
        jsonGenerator.writeEndObject();
        return id == null;
    }

    private static void writeProperties(JsonGenerator jsonGenerator, Map<String, Object> properties)
            throws IOException {
        jsonGenerator.writeFieldName("properties");
        jsonGenerator.writeStartObject();
        for (var property : new TreeMap<>(properties).entrySet()) {
            writeField(jsonGenerator, property.getKey(), property.getValue());
        }
        jsonGenerator.writeEndObject();
    }
}
