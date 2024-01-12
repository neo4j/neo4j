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
package org.neo4j.kernel.internal;

import static java.lang.Math.max;
import static java.lang.String.format;
import static java.util.Comparator.comparing;
import static org.neo4j.internal.helpers.Exceptions.SILENT_UNCAUGHT_EXCEPTION_HANDLER;
import static org.neo4j.internal.helpers.collection.Iterables.count;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import org.apache.commons.lang3.mutable.MutableInt;
import org.eclipse.collections.api.factory.primitive.LongLists;
import org.eclipse.collections.api.factory.primitive.LongObjectMaps;
import org.eclipse.collections.api.iterator.MutableLongIterator;
import org.eclipse.collections.api.list.primitive.MutableLongList;
import org.eclipse.collections.api.map.primitive.MutableLongObjectMap;
import org.neo4j.graphdb.Direction;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.graphdb.Transaction;
import org.neo4j.internal.helpers.progress.ProgressMonitorFactory;
import org.neo4j.internal.kernel.api.TokenRead;
import org.neo4j.kernel.impl.coreapi.TransactionImpl;
import org.neo4j.values.storable.Value;
import org.neo4j.values.storable.Values;

/**
 * Storage engine agnostic way of comparing contents of two databases, given that node IDs are the same,
 * i.e. node with ID X in the first database corresponds to the same logical node X in the other database.
 */
public class DatabaseComparator {
    public static void assertDatabasesHaveTheSameLogicalContents(
            GraphDatabaseService from,
            GraphDatabaseService to,
            boolean checkDegrees,
            int totalNumThreads,
            ProgressMonitorFactory progressMonitorFactory)
            throws Exception {
        int numThreads = max(1, totalNumThreads - 1);
        ThreadPoolExecutor executor = new ThreadPoolExecutor(
                numThreads,
                numThreads,
                1,
                TimeUnit.HOURS,
                new ArrayBlockingQueue<>(100),
                dontPrintOnExceptionThreadFactory());
        executor.setRejectedExecutionHandler(new ThreadPoolExecutor.CallerRunsPolicy());
        int batchSize = 1_000;
        List<Future<?>> futures = new ArrayList<>();
        try (var fromTx = from.beginTx();
                var progress = progressMonitorFactory.singlePart(
                        "Validation",
                        ((TransactionImpl) fromTx)
                                .kernelTransaction()
                                .dataRead()
                                .estimateCountsForNode(TokenRead.ANY_LABEL))) {
            MutableLongList batch = LongLists.mutable.withInitialCapacity(batchSize);
            for (Node fromNode : fromTx.getAllNodes()) {
                batch.add(fromNode.getId());
                if (batch.size() == batchSize) {
                    scheduleAndCheckFailure(futures, executor.submit(storeValidator(from, to, batch, checkDegrees)));
                    batch = LongLists.mutable.withInitialCapacity(batchSize);
                    progress.add(batchSize);
                }
            }
            if (!batch.isEmpty()) {
                futures.add(executor.submit(storeValidator(from, to, batch, checkDegrees)));
            }
            for (Future<?> future : futures) {
                future.get();
            }
        } finally {
            executor.shutdown();
            if (!executor.awaitTermination(1, TimeUnit.HOURS)) {
                throw new IllegalStateException("Comparison jobs didn't finish in time");
            }
        }
    }

    private static ThreadFactory dontPrintOnExceptionThreadFactory() {
        return runnable -> {
            Thread thread = new Thread(runnable);
            thread.setUncaughtExceptionHandler(SILENT_UNCAUGHT_EXCEPTION_HANDLER);
            return thread;
        };
    }

    private static void scheduleAndCheckFailure(List<Future<?>> futures, Future<?> future) throws Exception {
        futures.add(future);
        Iterator<Future<?>> iterator = futures.iterator();
        while (iterator.hasNext()) {
            Future<?> candidate = iterator.next();
            if (candidate.isDone() || candidate.isCancelled()) {
                // Let any exception propagate
                iterator.remove();
                candidate.get();
            } else {
                break;
            }
        }
    }

    private static Runnable storeValidator(
            GraphDatabaseService from, GraphDatabaseService to, MutableLongList batch, boolean checkDegrees) {
        return () -> {
            try (Transaction fromTx = from.beginTx();
                    Transaction toTx = to.beginTx()) {
                MutableLongIterator ids = batch.longIterator();
                while (ids.hasNext()) {
                    long fromNodeId = ids.next();
                    long toNodeId = fromNodeId;
                    Node fromNode = fromTx.getNodeById(fromNodeId);
                    Node toNode = toTx.getNodeById(toNodeId);
                    var report = compareNodes(fromNode, toNode, checkDegrees);
                    if (report.hasErrors()) {
                        String errorString = format(
                                "%s listing contents:%nfrom:%n%s%nto:%n%s",
                                report.report(), contentsOfNode(fromNode), contentsOfNode(toNode));
                        throw new RuntimeException(errorString);
                    }
                }
            }
        };
    }

    public static boolean nodesHaveEqualLogicalContents(Node fromNode, Node toNode) {
        return !compareNodes(fromNode, toNode, true).hasErrors();
    }

    private static ComparisonReport compareNodes(Node fromNode, Node toNode, boolean checkRelationships) {
        ComparisonReport report = new ComparisonReport(fromNode, toNode);
        HashSet<String> fromLabels = new HashSet<>();
        HashSet<String> toLabels = new HashSet<>();
        fromNode.getLabels().forEach(l -> fromLabels.add(l.name()));
        toNode.getLabels().forEach(l -> toLabels.add(l.name()));
        if (!fromLabels.equals(toLabels)) {
            report.add("Broken labels %s should be %s diff:", toLabels, fromLabels, setDiff(fromLabels, toLabels));
        }

        HashMap<String, Value> fromProps = new HashMap<>();
        HashMap<String, Value> toProps = new HashMap<>();
        fromNode.getAllProperties().forEach((s, o) -> fromProps.put(s, Values.of(o)));
        toNode.getAllProperties().forEach((s, o) -> toProps.put(s, Values.of(o)));
        if (!fromProps.equals(toProps)) {
            report.add("Broken properties %s should be %s diff:%s", toProps, fromProps, mapDiff(fromProps, toProps));
        }

        if (checkRelationships) {
            // Degrees
            long fromDegree = fromNode.getDegree();
            long toDegree = toNode.getDegree();
            if (fromDegree != toDegree) {
                // This is silly, but it may be that the record storage db is wrong in this sense... it happens all the
                // time
                // due to the old, old, really old, degrees decrement for loops bug. So go and actually double-check by
                // counting manually.
                long fromDegreeByManuallyCounting = degreeByManuallyCounting(fromNode);
                if (fromDegreeByManuallyCounting != fromDegree) {
                    fromDegree = fromDegreeByManuallyCounting;
                }

                if (fromDegree != toDegree) {
                    report.add(
                            "Broken relationships (degrees) %s should be %d diff:%s",
                            toNode.getDegree(), fromNode.getDegree(), degreesDiff(fromNode, toNode));
                }
            }

            // Relationship types
            Set<String> fromRelationshipTypes = new HashSet<>();
            Set<String> toRelationshipTypes = new HashSet<>();
            fromNode.getRelationshipTypes().forEach(t -> fromRelationshipTypes.add(t.name()));
            toNode.getRelationshipTypes().forEach(t -> toRelationshipTypes.add(t.name()));
            if (!fromRelationshipTypes.equals(toRelationshipTypes)) {
                report.add("Broken relationship types %s should be %s", fromRelationshipTypes, toRelationshipTypes);
            }

            // Relationships (start) -[type]-> (end), not properties yet because we don't know how to map from -> to
            // relationships
            for (String relationshipType : fromRelationshipTypes) {
                RelationshipType type = RelationshipType.withName(relationshipType);
                compareRelationships(fromNode, toNode, Direction.OUTGOING, type, report);
                compareRelationships(fromNode, toNode, Direction.INCOMING, type, report);
                compareRelationships(fromNode, toNode, Direction.BOTH, type, report);
            }
        }
        return report;
    }

    private static long degreeByManuallyCounting(Node node) {
        return count(node.getRelationships());
    }

    private static void compareRelationships(
            Node fromNode, Node toNode, Direction direction, RelationshipType type, ComparisonReport report) {
        MutableLongObjectMap<MutableInt> fromOtherNodes = LongObjectMaps.mutable.empty();
        MutableLongObjectMap<MutableInt> toOtherNodes = LongObjectMaps.mutable.empty();
        int fromCount = countRelationships(fromNode, direction, type, fromOtherNodes);
        int toCount = countRelationships(toNode, direction, type, toOtherNodes);
        if (fromCount != toCount) {
            report.add("Broken relationship count %s, %s %d should be %d", direction, type.name(), toCount, fromCount);
        }
        fromOtherNodes.forEachKeyValue((otherFromNodeId, fromOtherNodeCount) -> {
            long otherToNodeId = otherFromNodeId;
            MutableInt toOtherNodeCount = toOtherNodes.get(otherToNodeId);
            if (toOtherNodeCount == null || fromOtherNodeCount.intValue() != toOtherNodeCount.intValue()) {
                report.add(
                        "Broken number of relationships for %s, %s should be %s",
                        relationshipDataToString(
                                fromNode.getId(),
                                toNode.getId(),
                                direction,
                                type.name(),
                                otherFromNodeId,
                                otherToNodeId),
                        toOtherNodeCount,
                        fromOtherNodeCount);
            }
        });
    }

    private static String relationshipDataToString(
            long fromNodeId,
            long toNodeId,
            Direction direction,
            String type,
            long otherFromNodeId,
            long otherToNodeId) {
        return format(
                "(%d/%d)%s[%s]%s(%d/%d)",
                fromNodeId,
                toNodeId,
                direction == Direction.INCOMING ? "<-" : "--",
                type,
                direction == Direction.INCOMING ? "--" : "->",
                otherFromNodeId,
                otherToNodeId);
    }

    private static int countRelationships(
            Node node, Direction direction, RelationshipType type, MutableLongObjectMap<MutableInt> otherNodes) {
        int count = 0;
        try (var relationships = node.getRelationships(direction, type)) {
            var iterator = relationships.iterator();
            while (iterator.hasNext()) {
                var relationship = iterator.next();
                otherNodes
                        .getIfAbsentPut(relationship.getOtherNodeId(node.getId()), MutableInt::new)
                        .increment();
                count++;
            }
        }
        return count;
    }

    private static String contentsOfNode(Node node) {
        StringBuilder builder = new StringBuilder();
        builder.append("Labels:");
        node.getLabels().forEach(label -> builder.append(format("%n  ")).append(label.name()));
        builder.append(format("%nProperties:"));
        node.getAllProperties().forEach((key, value) -> builder.append(format("%n  %s=%s", key, value)));
        builder.append(format("%nRelationships:"));
        TreeSet<RelationshipType> types = new TreeSet<>(comparing(RelationshipType::name));
        node.getRelationshipTypes().forEach(types::add);
        for (RelationshipType type : types) {
            node.getRelationships(Direction.BOTH, type).forEach(rel -> builder.append(format("%n  %s", rel)));
        }
        return builder.append(format("%n")).toString();
    }

    private static String degreesDiff(Node fromNode, Node toNode) {
        Set<String> fromTypes = new HashSet<>();
        Set<String> toTypes = new HashSet<>();
        fromNode.getRelationshipTypes().forEach(type -> fromTypes.add(type.name()));
        toNode.getRelationshipTypes().forEach(type -> toTypes.add(type.name()));
        if (!fromTypes.equals(toTypes)) {
            return "Relationship types differ: " + setDiff(fromTypes, toTypes);
        }

        StringBuilder builder = new StringBuilder();
        for (String typeName : fromTypes) {
            RelationshipType type = RelationshipType.withName(typeName);
            checkDegreeDiff(fromNode, toNode, builder, type, Direction.OUTGOING);
            checkDegreeDiff(fromNode, toNode, builder, type, Direction.INCOMING);
            checkDegreeDiff(fromNode, toNode, builder, type, Direction.BOTH);
        }
        return builder.toString();
    }

    private static void checkDegreeDiff(
            Node fromNode, Node toNode, StringBuilder builder, RelationshipType type, Direction direction) {
        int from = fromNode.getDegree(type, direction);
        int to = toNode.getDegree(type, direction);
        if (from != to) {
            builder.append(format("degree:%s,%s:%d vs %d", type.name(), direction.name(), from, to));
        }
    }

    private static <T> String setDiff(Set<T> from, Set<T> to) {
        StringBuilder builder = new StringBuilder();
        Set<T> combined = new HashSet<>(from);
        combined.removeAll(to);
        combined.forEach(label -> builder.append(format("%n<%s", label)));
        combined = new HashSet<>(to);
        combined.removeAll(from);
        combined.forEach(label -> builder.append(format("%n>%s", label)));
        return builder.toString();
    }

    private static <T> String mapDiff(Map<String, T> from, Map<String, T> to) {
        StringBuilder builder = new StringBuilder();
        Set<String> allKeys = new HashSet<>(from.keySet());
        allKeys.addAll(to.keySet());
        for (String key : allKeys) {
            T fromValue = from.get(key);
            T toValue = to.get(key);
            if (toValue == null) {
                builder.append(format("%n<%s=%s", key, fromValue));
            } else if (fromValue == null) {
                builder.append(format("%n>%s=%s", key, toValue));
            } else if (!fromValue.equals(toValue)) {
                builder.append(format("%n!%s=%s vs %s", key, fromValue, toValue));
            }
        }
        return builder.toString();
    }

    private static class ComparisonReport {
        private final Node fromNode;
        private final Node toNode;
        private StringBuilder builder;

        ComparisonReport(Node fromNode, Node toNode) {
            this.fromNode = fromNode;
            this.toNode = toNode;
        }

        void add(String format, Object... parameters) {
            builder().append(format("%n  " + format, parameters));
        }

        private StringBuilder builder() {
            if (builder == null) {
                builder = new StringBuilder(format("Validation failed for %s --> %s", fromNode, toNode));
            }
            return builder;
        }

        boolean hasErrors() {
            return builder != null;
        }

        String report() {
            return builder.toString();
        }
    }
}
