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
import java.io.PrintStream;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.LockSupport;
import org.neo4j.common.EntityType;
import org.neo4j.internal.batchimport.cache.idmapping.string.DuplicateInputIdException;
import org.neo4j.util.concurrent.AsyncEvent;
import org.neo4j.util.concurrent.AsyncEvents;

public final class BadCollector implements Collector {
    public static final String BAD_FILE_NAME = "bad.log";

    /**
     * Introduced to avoid creating an exception for every reported bad thing, since it can be
     * quite the performance hogger for scenarios where there are many many bad things to collect.
     */
    abstract static class ProblemReporter extends AsyncEvent {
        private final int type;

        ProblemReporter(int type) {
            this.type = type;
        }

        int type() {
            return type;
        }

        abstract String message();

        abstract InputException exception();
    }

    interface Monitor {
        default void beforeProcessEvent() {}
    }

    static final Monitor NO_MONITOR = new Monitor() {};

    static final int BAD_RELATIONSHIPS = 0x1;
    static final int DUPLICATE_NODES = 0x2;
    static final int EXTRA_COLUMNS = 0x4;
    static final int VIOLATING_NODES = 0x8;
    static final int BAD_NODES = DUPLICATE_NODES | VIOLATING_NODES;

    static final int COLLECT_ALL = BAD_RELATIONSHIPS | BAD_NODES | EXTRA_COLUMNS;
    public static final long UNLIMITED_TOLERANCE = -1;
    static final int DEFAULT_BACK_PRESSURE_THRESHOLD = 10_000;

    private final PrintStream out;
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

    public BadCollector(OutputStream out, long tolerance, int collect) {
        this(out, tolerance, collect, DEFAULT_BACK_PRESSURE_THRESHOLD, false, NO_MONITOR);
    }

    BadCollector(
            OutputStream out,
            long tolerance,
            int collect,
            int backPressureThreshold,
            boolean skipBadEntriesLogging,
            Monitor monitor) {
        this.out = new PrintStream(out);
        this.tolerance = tolerance;
        this.collect = collect;
        this.backPressureThreshold = backPressureThreshold;
        this.logBadEntries = !skipBadEntriesLogging;
        this.monitor = monitor;
        this.logger = new AsyncEvents<>(this::processEvent, AsyncEvents.Monitor.NONE);
        this.eventProcessor = new Thread(logger);
        this.eventProcessor.start();
    }

    private void processEvent(ProblemReporter report) {
        monitor.beforeProcessEvent();
        out.println(report.message());
        queueSize.addAndGet(-1);
    }

    @Override
    public void collectBadRelationship(
            Object startId, Group startIdGroup, Object type, Object endId, Group endIdGroup, Object specificValue) {
        collect(new RelationshipsProblemReporter(startId, startIdGroup, type, endId, endIdGroup, specificValue));
    }

    @Override
    public void collectExtraColumns(final String source, final long row, final String value) {
        collect(new ExtraColumnsProblemReporter(row, source, value));
    }

    @Override
    public void collectDuplicateNode(Object id, long actualId, Group group) {
        collect(new NodesProblemReporter(id, group));
    }

    @Override
    public void collectEntityViolatingConstraint(
            Object id,
            long actualId,
            Map<String, Object> properties,
            String constraintDescription,
            EntityType entityType) {
        collect(new EntityViolatingConstraintReporter(id, actualId, properties, constraintDescription, entityType));
    }

    @Override
    public void collectRelationshipViolatingConstraint(
            Map<String, Object> properties,
            String constraintDescription,
            Object startId,
            Group startIdGroup,
            String type,
            Object endId,
            Group endIdGroup) {
        collect(new RelationshipViolatingConstraintReporter(
                properties, constraintDescription, startId, startIdGroup, type, endId, endIdGroup));
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
        logger.shutdown();
        try {
            logger.awaitTermination();
            eventProcessor.join();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        } finally {
            out.flush();
            out.close();
        }
    }

    @Override
    public long badEntries() {
        return badEntries.get();
    }

    private boolean collects(int bit) {
        return (collect & bit) != 0;
    }

    private static class RelationshipsProblemReporter extends ProblemReporter {
        private String message;
        private final Object specificValue;
        private final Object startId;
        private final Group startIdGroup;
        private final Object type;
        private final Object endId;
        private final Group endIdGroup;

        RelationshipsProblemReporter(
                Object startId, Group startIdGroup, Object type, Object endId, Group endIdGroup, Object specificValue) {
            super(BAD_RELATIONSHIPS);
            this.startId = startId;
            this.startIdGroup = startIdGroup;
            this.type = type;
            this.endId = endId;
            this.endIdGroup = endIdGroup;
            this.specificValue = specificValue;
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
                message = !isMissingData()
                        ? format(
                                "%s (%s)-[%s]->%s (%s) referring to missing node %s",
                                startId, startIdGroup, type, endId, endIdGroup, specificValue)
                        : format(
                                "%s (%s)-[%s]->%s (%s) is missing data",
                                startId, startIdGroup, type, endId, endIdGroup);
            }
            return message;
        }

        private boolean isMissingData() {
            return startId == null || endId == null || type == null;
        }
    }

    private static class NodesProblemReporter extends ProblemReporter {
        private final Object id;
        private final Group group;

        NodesProblemReporter(Object id, Group group) {
            super(DUPLICATE_NODES);
            this.id = id;
            this.group = group;
        }

        @Override
        public String message() {
            return DuplicateInputIdException.message(id, group);
        }

        @Override
        public InputException exception() {
            return new DuplicateInputIdException(id, group);
        }
    }

    private static class ExtraColumnsProblemReporter extends ProblemReporter {
        private String message;
        private final long row;
        private final String source;
        private final String value;

        ExtraColumnsProblemReporter(long row, String source, String value) {
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
                message =
                        format("Extra column not present in header on line %d in %s with value %s", row, source, value);
            }
            return message;
        }
    }

    private static class EntityViolatingConstraintReporter extends ProblemReporter {
        private final Object id;
        private final long actualId;
        private final Map<String, Object> properties;
        private final String constraintDescription;
        private final EntityType entityType;

        EntityViolatingConstraintReporter(
                Object id,
                long actualId,
                Map<String, Object> properties,
                String constraintDescription,
                EntityType entityType) {
            super(entityType == EntityType.NODE ? VIOLATING_NODES : BAD_RELATIONSHIPS);
            this.id = id;
            this.actualId = actualId;
            this.properties = properties;
            this.constraintDescription = constraintDescription;
            this.entityType = entityType;
        }

        @Override
        String message() {
            return format(
                    "%s %s (internal id %d) would have violated constraint:%s with properties:%s",
                    entityType == EntityType.NODE ? "Node" : "Relationship",
                    id,
                    actualId,
                    constraintDescription,
                    properties);
        }

        @Override
        InputException exception() {
            return new InputException(message());
        }
    }

    private static class RelationshipViolatingConstraintReporter extends ProblemReporter {
        private final Map<String, Object> properties;
        private final String constraintDescription;
        private final Object startId;
        private final Group startIdGroup;
        private final String type;
        private final Object endId;
        private final Group endIdGroup;

        RelationshipViolatingConstraintReporter(
                Map<String, Object> properties,
                String constraintDescription,
                Object startId,
                Group startIdGroup,
                String type,
                Object endId,
                Group endIdGroup) {
            super(BAD_RELATIONSHIPS);
            this.properties = properties;
            this.constraintDescription = constraintDescription;
            this.startId = startId;
            this.startIdGroup = startIdGroup;
            this.type = type;
            this.endId = endId;
            this.endIdGroup = endIdGroup;
        }

        @Override
        String message() {
            return format(
                    "%s%s-[%s]->%s%s would have violated constraint:%s with properties:%s",
                    startId,
                    startIdGroup != null ? " (" + startIdGroup + ")" : "",
                    type,
                    endId,
                    endIdGroup != null ? " (" + endIdGroup + ")" : "",
                    constraintDescription,
                    properties);
        }

        @Override
        InputException exception() {
            return new InputException(message());
        }
    }
}
