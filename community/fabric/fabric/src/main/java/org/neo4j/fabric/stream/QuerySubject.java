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
package org.neo4j.fabric.stream;

import java.util.Arrays;
import java.util.concurrent.CompletableFuture;
import org.neo4j.fabric.executor.FabricException;
import org.neo4j.fabric.executor.LocalExecutionSummary;
import org.neo4j.fabric.stream.summary.EmptySummary;
import org.neo4j.fabric.stream.summary.Summary;
import org.neo4j.graphdb.QueryStatistics;
import org.neo4j.kernel.api.exceptions.Status;
import org.neo4j.kernel.impl.query.QueryExecution;
import org.neo4j.kernel.impl.query.QuerySubscriber;
import org.neo4j.values.AnyValue;
import org.neo4j.values.virtual.ListValue;
import org.neo4j.values.virtual.MapValue;
import org.neo4j.values.virtual.MapValueBuilder;
import org.neo4j.values.virtual.NodeValue;
import org.neo4j.values.virtual.PathValue;
import org.neo4j.values.virtual.RelationshipValue;
import org.neo4j.values.virtual.VirtualNodeValue;
import org.neo4j.values.virtual.VirtualRelationshipValue;
import org.neo4j.values.virtual.VirtualValues;
import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;
import reactor.core.publisher.Mono;

public interface QuerySubject extends QuerySubscriber, Publisher<Record> {
    void setQueryExecution(QueryExecution queryExecution);

    Mono<Summary> getSummary();

    class BasicQuerySubject extends RecordQuerySubscriber implements QuerySubject {
        private final CompletableFuture<Summary> summaryFuture = new CompletableFuture<>();

        private Subscriber<? super Record> subscriber;
        private QueryExecution queryExecution;
        private QueryStatistics statistics;
        private Throwable cachedError;
        private boolean cachedCompleted;
        private boolean errorReceived;

        @Override
        public void setQueryExecution(QueryExecution queryExecution) {
            this.queryExecution = queryExecution;
        }

        @Override
        public Mono<Summary> getSummary() {
            return Mono.fromFuture(summaryFuture);
        }

        @Override
        public void onNext(Record record) {
            subscriber.onNext(record);
        }

        @Override
        public void onError(Throwable throwable) {
            errorReceived = true;

            if (subscriber == null) {
                cachedError = throwable;
            } else {
                subscriber.onError(throwable);
            }

            summaryFuture.completeExceptionally(throwable);
        }

        @Override
        public void onResultCompleted(QueryStatistics statistics) {
            this.statistics = statistics;
            if (subscriber == null) {
                cachedCompleted = true;
            } else {
                subscriber.onComplete();
                completeSummary();
            }
        }

        private void completeSummary() {
            summaryFuture.complete(new LocalExecutionSummary(queryExecution, statistics));
        }

        @Override
        public void subscribe(Subscriber<? super Record> subscriber) {

            if (this.subscriber != null) {
                throw new FabricException(Status.General.UnknownError, "Already subscribed");
            }
            this.subscriber = subscriber;
            Subscription subscription = new Subscription() {

                private final Object requestLock = new Object();
                private long pendingRequests;
                // a flag indicating if there is a thread requesting from upstream
                private boolean producing;

                @Override
                public void request(long size) {
                    synchronized (requestLock) {
                        pendingRequests += size;
                        // check if another thread is already requesting
                        if (producing) {
                            return;
                        }

                        producing = true;
                    }

                    try {
                        while (true) {
                            long toRequest;
                            synchronized (requestLock) {
                                toRequest = pendingRequests;
                                if (toRequest == 0) {
                                    return;
                                }

                                pendingRequests = 0;
                            }

                            doRequest(toRequest);
                        }
                    } finally {
                        synchronized (requestLock) {
                            producing = false;
                        }
                    }
                }

                private void doRequest(long size) {
                    maybeSendCachedEvents();
                    try {
                        queryExecution.request(size);
                        var hasMore = queryExecution.await();
                        // Workaround for some queryExecution:s where there are no results but onResultCompleted is
                        // never called.
                        if (!hasMore) {
                            cachedCompleted = true;
                            maybeSendCachedEvents();
                        }
                    } catch (Exception e) {
                        subscriber.onError(e);
                    }
                }

                @Override
                public void cancel() {
                    try {
                        queryExecution.cancel();
                    } catch (Throwable e) {
                        // ignore
                    }

                    if (!summaryFuture.isDone()) {
                        summaryFuture.complete(new EmptySummary());
                    }
                }
            };
            subscriber.onSubscribe(subscription);
            maybeSendCachedEvents();
        }

        private void maybeSendCachedEvents() {
            if (cachedError != null) {
                subscriber.onError(cachedError);
                cachedError = null;
            } else if (cachedCompleted) {
                subscriber.onComplete();
                cachedCompleted = false;
                completeSummary();
            }
        }
    }

    abstract class RecordQuerySubscriber implements QuerySubscriber {
        private int numberOfFields;
        private AnyValue[] fields;

        @Override
        public void onResult(int numberOfFields) {
            this.numberOfFields = numberOfFields;
        }

        @Override
        public void onRecord() {
            fields = new AnyValue[numberOfFields];
        }

        @Override
        public void onField(int offset, AnyValue value) {
            fields[offset] = value;
        }

        @Override
        public void onRecordCompleted() {
            onNext(Records.of(fields));
        }

        abstract void onNext(Record record);
    }

    class CompositeQuerySubject extends BasicQuerySubject implements QuerySubject {
        private final long sourceTagId;
        private final long sourceId;

        public CompositeQuerySubject(long sourceId) {
            this.sourceTagId = SourceTagging.makeSourceTag(sourceId);
            this.sourceId = sourceId;
        }

        @Override
        public void onField(int offset, AnyValue value) {
            AnyValue compositeDatabaseValue = toCompositeDatabaseValue(value);
            super.onField(offset, compositeDatabaseValue);
        }

        private AnyValue toCompositeDatabaseValue(AnyValue value) {
            if (value instanceof VirtualNodeValue) {
                if (value instanceof NodeValue node) {
                    return toCompositeDatabaseValue(node);
                } else {
                    throw unableToTagError(value);
                }
            } else if (value instanceof VirtualRelationshipValue) {
                if (value instanceof RelationshipValue rel) {
                    return toCompositeDatabaseValue(rel);
                } else {
                    throw unableToTagError(value);
                }
            } else if (value instanceof PathValue) {
                return toCompositeDatabaseValue((PathValue) value);
            } else if (value instanceof ListValue) {
                return toCompositeDatabaseValue((ListValue) value);
            } else if (value instanceof MapValue) {
                return toCompositeDatabaseValue((MapValue) value);
            } else {
                return value;
            }
        }

        private NodeValue toCompositeDatabaseValue(NodeValue n) {
            return VirtualValues.compositeGraphNodeValue(
                    tag(n.id()), n.elementId(), sourceId, n.labels(), n.properties());
        }

        private RelationshipValue toCompositeDatabaseValue(RelationshipValue r) {
            return VirtualValues.compositeGraphRelationshipValue(
                    r.id(),
                    r.elementId(),
                    sourceId,
                    VirtualValues.node(tag(r.startNodeId()), r.startNode().elementId(), sourceId),
                    VirtualValues.node(tag(r.endNodeId()), r.endNode().elementId(), sourceId),
                    r.type(),
                    r.properties());
        }

        private PathValue toCompositeDatabaseValue(PathValue pathValue) {
            return VirtualValues.path(
                    Arrays.stream(pathValue.nodes())
                            .map(this::toCompositeDatabaseValue)
                            .toArray(NodeValue[]::new),
                    Arrays.stream(pathValue.relationships())
                            .map(this::toCompositeDatabaseValue)
                            .toArray(RelationshipValue[]::new));
        }

        private ListValue toCompositeDatabaseValue(ListValue listValue) {
            return VirtualValues.list(Arrays.stream(listValue.asArray())
                    .map(this::toCompositeDatabaseValue)
                    .toArray(AnyValue[]::new));
        }

        private MapValue toCompositeDatabaseValue(MapValue mapValue) {
            if (mapValue.isEmpty()) {
                return mapValue;
            }
            MapValueBuilder builder = new MapValueBuilder(mapValue.size());
            mapValue.foreach((key, value) -> builder.add(key, toCompositeDatabaseValue(value)));
            return builder.build();
        }

        private long tag(long id) {
            return SourceTagging.tagId(id, sourceTagId);
        }

        private static FabricException unableToTagError(AnyValue value) {
            return new FabricException(
                    Status.General.UnknownError, "Unable to add graph id to entity of type " + value.getTypeName());
        }
    }
}
