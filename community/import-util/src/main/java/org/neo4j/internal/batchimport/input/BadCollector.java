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

import java.io.OutputStream;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.LockSupport;
import org.neo4j.batchimport.api.input.Collector;
import org.neo4j.batchimport.api.input.Group;
import org.neo4j.common.EntityType;
import org.neo4j.util.VisibleForTesting;
import org.neo4j.util.concurrent.AsyncEvent;
import org.neo4j.util.concurrent.AsyncEvents;

public final class BadCollector implements Collector {
    public static final String BAD_FILE_NAME = "bad.log";

    /**
     * Introduced to avoid creating an exception for every reported bad thing, since it can be
     * quite the performance hogger for scenarios where there are many many bad things to collect.
     */
    public abstract static class ProblemReporter extends AsyncEvent {
        private final int type;

        ProblemReporter(int type) {
            this.type = type;
        }

        int type() {
            return type;
        }

        String typeKey() {
            return PROBLEM_TYPES.getOrDefault(type, "UnknownProblem");
        }

        abstract String message();

        abstract InputException exception();

        @Override
        public String toString() {
            return "ProblemReporter[%s]".formatted(typeKey());
        }
    }

    /**
     * Handles any problems that get reported to the collector, ex. print to {@link OutputStream}
     */
    public interface ProblemHandler extends AutoCloseable {
        /**
         * Callback to handle any errors being reported during an import.
         * @param reporter the error being reported
         */
        void handle(ProblemReporter reporter);

        @Override
        void close();
    }

    interface Monitor {
        default void beforeProcessEvent() {}
    }

    static final Monitor NO_MONITOR = new Monitor() {};

    static final int BAD_RELATIONSHIPS = 0x1;
    static final int DUPLICATE_NODES = 0x2;
    static final int EXTRA_COLUMNS = 0x4;
    static final int VIOLATING_NODES = 0x8;
    static final int VIOLATING_SCHEMA = 0x10;
    static final int OTHER_NODE_VIOLATION = 0x20;
    static final int OTHER_RELATIONSHIP_VIOLATION = 0x40;
    static final int DATA_AFTER_QUOTE = 0x80;
    static final int ILLEGAL_QUOTE = 0x100;
    static final int INVALID_ID = 0x200;
    static final int MISSING_ID_COLUMN = 0x400;
    static final int BAD_NODES = DUPLICATE_NODES | VIOLATING_NODES | OTHER_NODE_VIOLATION;

    static final int ALL_SCHEMA_VIOLATIONS = VIOLATING_SCHEMA | BAD_NODES | BAD_RELATIONSHIPS;
    static final int NODE_SCHEMA_VIOLATIONS = VIOLATING_SCHEMA | BAD_NODES;
    static final int REL_SCHEMA_VIOLATIONS = VIOLATING_SCHEMA | BAD_RELATIONSHIPS;

    private static final Map<Integer, String> PROBLEM_TYPES = Map.ofEntries(
            Map.entry(BAD_RELATIONSHIPS, "BadRelationship"),
            Map.entry(DUPLICATE_NODES, "DuplicateNode"),
            Map.entry(EXTRA_COLUMNS, "ExtraColumn"),
            Map.entry(VIOLATING_NODES, "NodeViolation"),
            Map.entry(VIOLATING_SCHEMA, "RelationshipViolation"),
            Map.entry(OTHER_NODE_VIOLATION, "OtherNodeViolation"),
            Map.entry(OTHER_RELATIONSHIP_VIOLATION, "OtherRelationshipViolation"),
            Map.entry(ALL_SCHEMA_VIOLATIONS, "SchemaViolation"),
            Map.entry(NODE_SCHEMA_VIOLATIONS, "NodeSchemaViolation"),
            Map.entry(REL_SCHEMA_VIOLATIONS, "RelationshipSchemaViolation"),
            Map.entry(DATA_AFTER_QUOTE, "DataAfterQuote"),
            Map.entry(ILLEGAL_QUOTE, "IllegalQuote"),
            Map.entry(INVALID_ID, "InvalidId"),
            Map.entry(MISSING_ID_COLUMN, "MissingIdColumn"));

    static final int COLLECT_ALL = -1;
    public static final long UNLIMITED_TOLERANCE = -1;
    static final int DEFAULT_BACK_PRESSURE_THRESHOLD = 10_000;

    private final ProblemHandler problemHandler;
    private final long tolerance;
    private final int collect;
    private final int backPressureThreshold;
    private final boolean logBadEntries;
    private final Monitor monitor;

    // volatile since one importer thread calls collect(), where this value is incremented and later the "main"
    // thread calls badEntries() to get a count.
    private final AtomicLong badEntries = new AtomicLong();
    private final AsyncEvents<ProblemReporter> logger;
    private final Thread eventProcessor;
    private final AtomicLong queueSize = new AtomicLong();

    @VisibleForTesting
    BadCollector(
            OutputStream out,
            long tolerance,
            int collect,
            int backPressureThreshold,
            boolean skipBadEntriesLogging,
            Monitor monitor) {
        this(
                ProblemReporters.printingProblemHandler(out),
                tolerance,
                collect,
                backPressureThreshold,
                skipBadEntriesLogging,
                monitor);
    }

    BadCollector(
            ProblemHandler problemHandler,
            long tolerance,
            int collect,
            int backPressureThreshold,
            boolean skipBadEntriesLogging,
            Monitor monitor) {
        this.problemHandler = problemHandler;
        this.tolerance = tolerance;
        this.collect = collect;
        this.backPressureThreshold = backPressureThreshold;
        this.logBadEntries = !skipBadEntriesLogging;
        this.monitor = monitor;
        this.logger = new AsyncEvents<>(this::processEvent);
        this.eventProcessor = new Thread(logger);
        this.eventProcessor.start();
    }

    @VisibleForTesting
    public static Collector create(OutputStream out, long tolerance) {
        return create(out, tolerance, COLLECT_ALL, false);
    }

    @VisibleForTesting
    public static Collector create(OutputStream out, long tolerance, int collect) {
        return create(out, tolerance, collect, false);
    }

    public static Collector create(OutputStream out, long tolerance, int collect, boolean skipBadEntriesLogging) {
        return create(ProblemReporters.printingProblemHandler(out), tolerance, collect, skipBadEntriesLogging);
    }

    public static Collector create(
            ProblemHandler problemHandler, long tolerance, int collect, boolean skipBadEntriesLogging) {
        return new BadCollector(
                problemHandler, tolerance, collect, DEFAULT_BACK_PRESSURE_THRESHOLD, skipBadEntriesLogging, NO_MONITOR);
    }

    public static int collectFlag(
            boolean skipBadRelationships,
            boolean skipDuplicateNodes,
            boolean ignoreExtraColumns,
            boolean hasSchemaCommands) {
        return (skipBadRelationships ? BAD_RELATIONSHIPS : 0)
                // for now, we use the skipDuplicateNodes for both duplicate and violating nodes
                // We probably need to split this into multiple ones
                | (skipDuplicateNodes ? BAD_NODES : 0)
                | (ignoreExtraColumns ? EXTRA_COLUMNS : 0)
                | (hasSchemaCommands ? VIOLATING_SCHEMA : 0);
    }

    private void processEvent(ProblemReporter report) {
        monitor.beforeProcessEvent();
        problemHandler.handle(report);
        queueSize.addAndGet(-1);
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
        collect(ProblemReporters.relationshipsProblemReporter(
                startId, startIdGroup, type, endId, endIdGroup, specificValue, source, lineNumber));
    }

    @Override
    public void collectExtraColumns(final String source, final long row, final String value) {
        collect(ProblemReporters.collectExtraColumnsReporter(source, row, value));
    }

    @Override
    public void collectDuplicateNode(Object id, long actualId, Group group, String source, long lineNumber) {
        collect(ProblemReporters.nodesProblemReporter(id, group, source, lineNumber));
    }

    @Override
    public void collectEntityViolatingConstraint(
            Object id,
            long actualId,
            Map<String, Object> properties,
            String constraintDescription,
            EntityType entityType,
            String sourceDescription,
            long lineNumber) {
        collect(ProblemReporters.entityViolatingConstraintReporter(
                id, actualId, properties, constraintDescription, entityType, sourceDescription, lineNumber));
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
            String sourceDescription,
            long lineNumber) {
        collect(ProblemReporters.relationshipViolatingConstraintReporter(
                properties,
                constraintDescription,
                startId,
                startIdGroup,
                type,
                endId,
                endIdGroup,
                sourceDescription,
                lineNumber));
    }

    @Override
    public void collectIdColumnMissing(String source, long row, int columnIndex) {
        collect(ProblemReporters.idColumnMissingReporter(source, row, columnIndex));
    }

    @Override
    public void collectSchemaCommandFailure(EntityType entityType, String failureMessage) {
        collect(ProblemReporters.schemaCommandFailureReporter(entityType, failureMessage));
    }

    @Override
    public void collectOtherNodeViolation(String problem) {
        collect(ProblemReporters.otherViolationReporter(EntityType.NODE, problem));
    }

    @Override
    public void collectOtherRelationshipViolation(String problem) {
        collect(ProblemReporters.otherViolationReporter(EntityType.RELATIONSHIP, problem));
    }

    @Override
    public void collectDataAfterQuote(String source, long row, String value) {
        collect(ProblemReporters.dataAfterQuoteReporter(source, row, value));
    }

    @Override
    public void collectIllegalQuote(String source, long row, String value) {
        collect(ProblemReporters.illegalQuoteReporter(source, row, value));
    }

    @Override
    public void collectInvalidID(String source, long row, String value) {
        collect(ProblemReporters.invalidIdReporter(source, row, value));
    }

    @Override
    public boolean isCollectingBadRelationships() {
        return collects(BAD_RELATIONSHIPS);
    }

    private void collect(ProblemReporter report) {
        boolean collect = collects(report.type());
        if (collect) {
            // This type of problem is collected and we're within the max threshold, so it's OK
            long count = badEntries.incrementAndGet();
            if (tolerance == UNLIMITED_TOLERANCE || count <= tolerance) {
                // We're within the threshold
                if (logBadEntries) {
                    // Send this to the logger... but first apply some back pressure if queue is growing big
                    while (queueSize.get() >= backPressureThreshold) {
                        LockSupport.parkNanos(TimeUnit.MILLISECONDS.toNanos(10));
                    }
                    logger.send(report);
                    queueSize.addAndGet(1);
                }
                return; // i.e. don't treat this as an exception
            }
        }

        InputException exception = report.exception();
        throw collect
                ? new InputException(
                        format(
                                "Too many bad entries %d, where last one was: %s",
                                badEntries.longValue(), exception.getMessage()),
                        exception)
                : exception;
    }

    @Override
    public void close() {
        try (problemHandler) {
            logger.shutdown();
            logger.awaitTermination();
            eventProcessor.join();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    @Override
    public long badEntries() {
        return badEntries.get();
    }

    private boolean collects(int bit) {
        return (collect & bit) != 0;
    }
}
