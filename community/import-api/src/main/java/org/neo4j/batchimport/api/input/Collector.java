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
package org.neo4j.batchimport.api.input;

import static java.lang.String.format;

import java.util.Map;
import java.util.TreeMap;
import org.neo4j.common.EntityType;

/**
 * Collects items and is {@link #close() closed} after any and all items have been collected.
 * The {@link Collector} is responsible for closing whatever closeable resource received from the importer.
 */
public interface Collector extends AutoCloseable {
    void collectBadRelationship(
            Object startId,
            Group startIdGroup,
            Object type,
            Object endId,
            Group endIdGroup,
            Object specificValue,
            String source,
            long lineNumber);

    void collectDuplicateNode(Object id, long actualId, Group group, String source, long lineNumber);

    void collectEntityViolatingConstraint(
            Object id,
            long actualId,
            Map<String, Object> properties,
            String constraintDescription,
            EntityType entityType,
            String source,
            long lineNumber);

    void collectRelationshipViolatingConstraint(
            Map<String, Object> properties,
            String constraintDescription,
            Object startId,
            Group startIdGroup,
            String type,
            Object endId,
            Group endIdGroup,
            String source,
            long lineNumber);

    void collectDataAfterQuote(String source, long row, String value);

    void collectIllegalQuote(String source, long row, String value);

    void collectInvalidID(String source, long row, String value, EntityType entityType);

    void collectExtraColumns(String source, long row, String value);

    /**
     * @param columnIndex 1-based index of the ID column that is missing, as per the header.
     */
    void collectIdColumnMissing(String source, long row, int columnIndex);

    /**
     * @param entityType the type of entity that relates to the {@link org.neo4j.internal.schema.SchemaCommand} being
     *                  applied (or <code>null</code> if the type is unknown)
     * @param failureMessage the failure message that resulted when applying a {@link org.neo4j.internal.schema.SchemaCommand}
     */
    void collectSchemaCommandFailure(EntityType entityType, String failureMessage);

    void collectOtherNodeViolation(String problem);

    void collectOtherRelationshipViolation(String problem);

    long badEntries();

    boolean isCollectingBadRelationships();

    /**
     * Flushes whatever changes to the underlying resource supplied from the importer.
     */
    @Override
    void close();

    static String standardisedErrorMessage(String problem, String source, long line, String furtherDetails) {
        return source != null
                ? format("%s%n%s: line %d%n%s", problem, source, line, furtherDetails)
                : format("%s%n%s", problem, furtherDetails);
    }

    static String illustrateRelationship(
            Object startId, Group startIdGroup, Object type, Object endId, Group endIdGroup) {
        return format(
                "(%s%s)-[%s]->(%s%s)",
                startId,
                startIdGroup != null ? format(":%s", startIdGroup) : "",
                type,
                endId,
                endIdGroup != null ? format(":%s", endIdGroup) : "");
    }

    static String idColumnMissingMessage(String source, long row, int columnIndex) {
        return standardisedErrorMessage(
                "Row has fewer columns than expected and ID column is missing.",
                source,
                row,
                "Missing ID column index: %d.".formatted(columnIndex));
    }

    static String invalidIDMessage(String source, long row, String value) {
        return standardisedErrorMessage(
                "ID value is invalid for the id type specified.",
                source,
                row,
                "Invalid ID value: `%s`.".formatted(value));
    }

    static String illegalQuoteMessage(String source, long row, String value) {
        return standardisedErrorMessage(
                "Quotes are only allowed in quoted strings in a CSV field.",
                source,
                row,
                "Column content: `%s`.".formatted(value));
    }

    static String dataAfterQuoteMessage(String source, long row, String value) {
        return standardisedErrorMessage(
                "Characters after an ending quote in a CSV field are not supported.",
                source,
                row,
                "Column content: `%s`.".formatted(value));
    }

    static String relationshipViolatingConstraintMessage(
            Map<String, Object> properties,
            String constraintDescription,
            Object startId,
            Group startIdGroup,
            String type,
            Object endId,
            Group endIdGroup,
            String source,
            long lineNumber) {
        return standardisedErrorMessage(
                "Relationship would have violated a constraint",
                source,
                lineNumber,
                format(
                        "%s would have violated constraint: %s, with properties:%s",
                        Collector.illustrateRelationship(startId, startIdGroup, type, endId, endIdGroup),
                        constraintDescription,
                        new TreeMap<>(properties)));
    }

    static String entityViolatingConstraintMessage(
            Object id,
            long actualId,
            Map<String, Object> properties,
            String constraintDescription,
            EntityType entityType,
            String source,
            long lineNumber) {
        final String entityTypeString = entityType == EntityType.NODE ? "Node" : "Relationship";
        return standardisedErrorMessage(
                format("%s would have violated a constraint", entityTypeString),
                source,
                lineNumber,
                format(
                        "%s %s (internal id %d) would have violated constraint: %s, with properties: %s",
                        entityTypeString, id, actualId, constraintDescription, new TreeMap<>(properties)));
    }

    static String extraColumnsMessage(String source, long row, String value) {
        return standardisedErrorMessage(
                "Row has more columns than expected based on the header.",
                source,
                row,
                "Bad extra column value: '%s'.".formatted(value));
    }

    Collector EMPTY = new Collector() {
        @Override
        public void collectExtraColumns(String source, long row, String value) {}

        @Override
        public void close() {}

        @Override
        public long badEntries() {
            return 0;
        }

        @Override
        public void collectBadRelationship(
                Object startId,
                Group startIdGroup,
                Object type,
                Object endId,
                Group endIdGroup,
                Object specificValue,
                String source,
                long lineNumber) {}

        @Override
        public void collectDuplicateNode(Object id, long actualId, Group group, String source, long lineNumber) {}

        @Override
        public void collectEntityViolatingConstraint(
                Object id,
                long actualId,
                Map<String, Object> properties,
                String constraintDescription,
                EntityType entityType,
                String source,
                long lineNumber) {}

        @Override
        public void collectRelationshipViolatingConstraint(
                Map<String, Object> properties,
                String constraintDescription,
                Object startId,
                Group startIdGroup,
                String type,
                Object endId,
                Group endIdGroup,
                String source,
                long lineNumber) {}

        @Override
        public void collectSchemaCommandFailure(EntityType entityType, String failureMessage) {}

        @Override
        public void collectDataAfterQuote(String source, long row, String value) {}

        @Override
        public void collectIllegalQuote(String source, long row, String value) {}

        @Override
        public void collectInvalidID(String source, long row, String value, EntityType entityType) {}

        @Override
        public void collectOtherNodeViolation(String problem) {}

        @Override
        public void collectOtherRelationshipViolation(String problem) {}

        @Override
        public void collectIdColumnMissing(String source, long row, int columnIndex) {}

        @Override
        public boolean isCollectingBadRelationships() {
            return true;
        }
    };

    Collector STRICT = new Collector() {
        @Override
        public void collectExtraColumns(String source, long row, String value) {
            throw new IllegalStateException(extraColumnsMessage(source, row, value));
        }

        @Override
        public void close() {}

        @Override
        public long badEntries() {
            return 0;
        }

        @Override
        public void collectBadRelationship(
                Object startId,
                Group startIdGroup,
                Object type,
                Object endId,
                Group endIdGroup,
                Object specificValue,
                String source,
                long lineNumber) {
            throw new IllegalStateException(standardisedErrorMessage(
                    "Bad relationship",
                    source,
                    lineNumber,
                    format(
                            "%s %s",
                            illustrateRelationship(startId, startIdGroup, type, endId, endIdGroup), specificValue)));
        }

        @Override
        public void collectDuplicateNode(Object id, long actualId, Group group, String source, long lineNumber) {
            throw new IllegalStateException(standardisedErrorMessage(
                    "Duplicate node", source, lineNumber, format("%s:%s id:%d", id, group, actualId)));
        }

        @Override
        public void collectDataAfterQuote(String source, long row, String value) {
            throw new IllegalStateException(dataAfterQuoteMessage(source, row, value));
        }

        @Override
        public void collectIllegalQuote(String source, long row, String value) {
            throw new IllegalStateException(illegalQuoteMessage(source, row, value));
        }

        @Override
        public void collectInvalidID(String source, long row, String value, EntityType entityType) {
            throw new IllegalStateException(invalidIDMessage(source, row, value));
        }

        @Override
        public void collectEntityViolatingConstraint(
                Object id,
                long actualId,
                Map<String, Object> properties,
                String constraintDescription,
                EntityType entityType,
                String source,
                long lineNumber) {
            throw new IllegalStateException(Collector.entityViolatingConstraintMessage(
                    id, actualId, properties, constraintDescription, entityType, source, lineNumber));
        }

        @Override
        public void collectRelationshipViolatingConstraint(
                Map<String, Object> properties,
                String constraintDescription,
                Object startId,
                Group startIdGroup,
                String type,
                Object endId,
                Group endIdGroup,
                String source,
                long lineNumber) {
            throw new IllegalStateException(relationshipViolatingConstraintMessage(
                    properties,
                    constraintDescription,
                    startId,
                    startIdGroup,
                    type,
                    endId,
                    endIdGroup,
                    source,
                    lineNumber));
        }

        @Override
        public void collectIdColumnMissing(String source, long row, int columnIndex) {
            throw new IllegalStateException(idColumnMissingMessage(source, row, columnIndex));
        }

        @Override
        public void collectSchemaCommandFailure(EntityType entityType, String failureMessage) {
            throw new IllegalStateException(failureMessage);
        }

        @Override
        public void collectOtherNodeViolation(String problem) {
            throw new IllegalStateException(problem);
        }

        @Override
        public void collectOtherRelationshipViolation(String problem) {
            throw new IllegalStateException(problem);
        }

        @Override
        public boolean isCollectingBadRelationships() {
            return false;
        }
    };

    class Adapter implements Collector {
        @Override
        public void collectBadRelationship(
                Object startId,
                Group startIdGroup,
                Object type,
                Object endId,
                Group endIdGroup,
                Object specificValue,
                String source,
                long lineNumber) {}

        @Override
        public void collectDuplicateNode(Object id, long actualId, Group group, String source, long lineNumber) {}

        @Override
        public void collectEntityViolatingConstraint(
                Object id,
                long actualId,
                Map<String, Object> properties,
                String constraintDescription,
                EntityType entityType,
                String source,
                long lineNumber) {}

        @Override
        public void collectRelationshipViolatingConstraint(
                Map<String, Object> properties,
                String constraintDescription,
                Object startId,
                Group startIdGroup,
                String type,
                Object endId,
                Group endIdGroup,
                String source,
                long lineNumber) {}

        @Override
        public void collectExtraColumns(String source, long row, String value) {}

        @Override
        public void collectSchemaCommandFailure(EntityType entityType, String failureMessage) {}

        @Override
        public void collectDataAfterQuote(String source, long row, String value) {}

        @Override
        public void collectIllegalQuote(String source, long row, String value) {}

        @Override
        public void collectInvalidID(String source, long row, String value, EntityType entityType) {}

        @Override
        public void collectOtherNodeViolation(String problem) {}

        @Override
        public void collectOtherRelationshipViolation(String problem) {}

        @Override
        public void collectIdColumnMissing(String source, long row, int columnIndex) {}

        @Override
        public long badEntries() {
            return 0;
        }

        @Override
        public boolean isCollectingBadRelationships() {
            return false;
        }

        @Override
        public void close() {}
    }
}
