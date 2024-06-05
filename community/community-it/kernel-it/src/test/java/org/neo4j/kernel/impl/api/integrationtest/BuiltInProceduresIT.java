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
package org.neo4j.kernel.impl.api.integrationtest;

import static java.util.concurrent.TimeUnit.MINUTES;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.neo4j.configuration.GraphDatabaseSettings.SYSTEM_DATABASE_NAME;
import static org.neo4j.internal.helpers.collection.Iterators.asList;
import static org.neo4j.internal.kernel.api.procs.ProcedureCallContext.EMPTY;
import static org.neo4j.internal.kernel.api.procs.ProcedureSignature.procedureName;
import static org.neo4j.values.storable.Values.stringValue;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.FutureTask;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.neo4j.collection.RawIterator;
import org.neo4j.cypher.internal.cache.CypherQueryCaches;
import org.neo4j.graphdb.Resource;
import org.neo4j.internal.kernel.api.IndexMonitor;
import org.neo4j.internal.kernel.api.Procedures;
import org.neo4j.internal.kernel.api.SchemaWrite;
import org.neo4j.internal.kernel.api.TokenWrite;
import org.neo4j.internal.kernel.api.exceptions.ProcedureException;
import org.neo4j.internal.kernel.api.procs.ProcedureCallContext;
import org.neo4j.internal.schema.IndexPrototype;
import org.neo4j.internal.schema.SchemaDescriptors;
import org.neo4j.kernel.api.CypherScope;
import org.neo4j.kernel.api.KernelTransaction;
import org.neo4j.kernel.api.security.AnonymousContext;
import org.neo4j.kernel.impl.api.index.IndexSamplingMode;
import org.neo4j.kernel.impl.query.CacheMetrics;
import org.neo4j.kernel.impl.query.QueryCacheStatistics;
import org.neo4j.kernel.internal.Version;
import org.neo4j.monitoring.Monitors;
import org.neo4j.values.AnyValue;
import org.neo4j.values.virtual.VirtualValues;

class BuiltInProceduresIT extends KernelIntegrationTest implements ProcedureITBase {
    @Test
    void listAllLabels() throws Throwable {
        // Given
        KernelTransaction transaction = newTransaction(AnonymousContext.writeToken());
        long nodeId = transaction.dataWrite().nodeCreate();
        int labelId = transaction.tokenWrite().labelGetOrCreateForName("MyLabel");
        transaction.dataWrite().nodeAddLabel(nodeId, labelId);
        commit();

        // When
        Procedures procs = procs();
        try (var statement = kernelTransaction.acquireStatement()) {
            RawIterator<AnyValue[], ProcedureException> stream = procs.procedureCallRead(
                    procs.procedureGet(procedureName("db", "labels"), CypherScope.CYPHER_5)
                            .id(),
                    new AnyValue[0],
                    ProcedureCallContext.EMPTY);

            // Then
            assertThat(asList(stream)).containsExactly(new AnyValue[] {stringValue("MyLabel")});
        }
    }

    @Test
    void databaseInfo() throws ProcedureException {
        Procedures procs = procs();
        try (var statement = kernelTransaction.acquireStatement()) {
            RawIterator<AnyValue[], ProcedureException> stream = procs.procedureCallRead(
                    procs.procedureGet(procedureName("db", "info"), CypherScope.CYPHER_5)
                            .id(),
                    new AnyValue[0],
                    EMPTY);

            var procedureResult = asList(stream);
            assertFalse(procedureResult.isEmpty());
            var dbInfoRow = procedureResult.get(0);
            assertThat(dbInfoRow).contains(stringValue(db.databaseName()));
            assertThat(dbInfoRow).hasSize(3);
        }
    }

    @Test
    void dbmsInfo() throws ProcedureException {
        Procedures procs = procs();
        try (var statement = kernelTransaction.acquireStatement()) {
            RawIterator<AnyValue[], ProcedureException> stream = procs.procedureCallRead(
                    procs.procedureGet(procedureName("dbms", "info"), CypherScope.CYPHER_5)
                            .id(),
                    new AnyValue[0],
                    EMPTY);

            var procedureResult = asList(stream);
            assertFalse(procedureResult.isEmpty());
            var dbmsInfoRow = procedureResult.get(0);
            assertThat(dbmsInfoRow).contains(stringValue(SYSTEM_DATABASE_NAME));
            assertThat(dbmsInfoRow).hasSize(3);
        }
    }

    @Test
    @Timeout(value = 6, unit = MINUTES)
    void listAllLabelsMustNotBlockOnConstraintCreatingTransaction() throws Throwable {
        // Given
        KernelTransaction transaction = newTransaction(AnonymousContext.writeToken());
        long nodeId = transaction.dataWrite().nodeCreate();
        int labelId = transaction.tokenWrite().labelGetOrCreateForName("MyLabel");
        int propKey = transaction.tokenWrite().propertyKeyCreateForName("prop", false);
        transaction.dataWrite().nodeAddLabel(nodeId, labelId);
        commit();

        CountDownLatch constraintLatch = new CountDownLatch(1);
        CountDownLatch commitLatch = new CountDownLatch(1);
        FutureTask<Void> createConstraintTask = new FutureTask<>(() -> {
            SchemaWrite schemaWrite = schemaWriteInNewTransaction();
            try (Resource ignore = captureTransaction()) {
                IndexPrototype prototype = IndexPrototype.uniqueForSchema(SchemaDescriptors.forLabel(labelId, propKey))
                        .withName("constraint name");
                schemaWrite.uniquePropertyConstraintCreate(prototype);
                // We now hold a schema lock on the "MyLabel" label. Let the procedure calling transaction have a go.
                constraintLatch.countDown();
                commitLatch.await();
            }
            rollback();
            return null;
        });
        Thread constraintCreator = new Thread(createConstraintTask);
        constraintCreator.start();

        // When
        constraintLatch.await();
        Procedures procs = procs();
        try (var statement = kernelTransaction.acquireStatement()) {
            RawIterator<AnyValue[], ProcedureException> stream = procs.procedureCallRead(
                    procs.procedureGet(procedureName("db", "labels"), CypherScope.CYPHER_5)
                            .id(),
                    new AnyValue[0],
                    ProcedureCallContext.EMPTY);

            // Then
            try {
                assertThat(asList(stream)).containsExactly(new AnyValue[] {stringValue("MyLabel")});
            } finally {
                commitLatch.countDown();
            }
        }
        createConstraintTask.get();
        constraintCreator.join();
    }

    @Test
    void listPropertyKeys() throws Throwable {
        // Given
        TokenWrite ops = tokenWriteInNewTransaction();
        ops.propertyKeyGetOrCreateForName("MyProp");
        commit();

        // When
        Procedures procs = procs();
        try (var statement = kernelTransaction.acquireStatement()) {
            RawIterator<AnyValue[], ProcedureException> stream = procs.procedureCallRead(
                    procs.procedureGet(procedureName("db", "propertyKeys"), CypherScope.CYPHER_5)
                            .id(),
                    new AnyValue[0],
                    ProcedureCallContext.EMPTY);

            // Then
            assertThat(asList(stream)).containsExactly(new AnyValue[] {stringValue("MyProp")});
        }
    }

    @Test
    void listRelationshipTypes() throws Throwable {
        // Given
        KernelTransaction transaction = newTransaction(AnonymousContext.writeToken());
        int relType = transaction.tokenWrite().relationshipTypeGetOrCreateForName("MyRelType");
        long startNodeId = transaction.dataWrite().nodeCreate();
        long endNodeId = transaction.dataWrite().nodeCreate();
        transaction.dataWrite().relationshipCreate(startNodeId, relType, endNodeId);
        commit();

        // When
        Procedures procs = procs();
        try (var statement = kernelTransaction.acquireStatement()) {
            RawIterator<AnyValue[], ProcedureException> stream = procs.procedureCallRead(
                    procs.procedureGet(procedureName("db", "relationshipTypes"), CypherScope.CYPHER_5)
                            .id(),
                    new AnyValue[0],
                    ProcedureCallContext.EMPTY);

            // Then
            assertThat(asList(stream)).containsExactly(new AnyValue[] {stringValue("MyRelType")});
        }
    }

    @Test
    void failWhenCallingNonExistingProcedures() {
        Procedures procs = procs();
        try (var statement = kernelTransaction.acquireStatement()) {
            assertThrows(
                    ProcedureException.class,
                    () -> procs.procedureCallDbms(-1, new AnyValue[0], ProcedureCallContext.EMPTY));
        }
    }

    @Test
    void listAllComponents() throws Throwable {
        // Given a running database

        // When
        Procedures procs = procs();
        try (var statement = kernelTransaction.acquireStatement()) {
            RawIterator<AnyValue[], ProcedureException> stream = procs.procedureCallRead(
                    procs.procedureGet(procedureName("dbms", "components"), CypherScope.CYPHER_5)
                            .id(),
                    new AnyValue[0],
                    ProcedureCallContext.EMPTY);

            // Then
            assertThat(asList(stream)).containsExactly(new AnyValue[] {
                stringValue("Neo4j Kernel"),
                VirtualValues.list(stringValue(Version.getNeo4jVersion())),
                stringValue("community")
            });
        }

        commit();
    }

    @Test
    void prepareForReplanningShouldEmptyQueryCache() {
        // Given, something is cached
        try (org.neo4j.graphdb.Transaction transaction = db.beginTx()) {
            transaction.execute("MATCH (n) RETURN n").close();
            transaction.commit();
        }

        var cacheMetrics = preParserCacheMetricsMonitor();
        var flushesBefore = cacheMetrics.getCacheFlushes();
        var discardsBefore = cacheMetrics.getDiscards();

        // When
        try (org.neo4j.graphdb.Transaction transaction = db.beginTx()) {
            transaction.execute("CALL db.prepareForReplanning()").close();
            transaction.commit();
        }

        // Then, the initial query and the procedure call should now have been cleared
        assertThat(cacheMetrics.getCacheFlushes() - flushesBefore).isEqualTo(1L);
        assertThat(cacheMetrics.getDiscards() - discardsBefore).isEqualTo(2L);
    }

    @Test
    void prepareForReplanningShouldTriggerIndexesSampling() {
        // Given
        IndexSamplingMonitor monitor = indexSamplingMonitor();

        // When
        try (org.neo4j.graphdb.Transaction transaction = db.beginTx()) {
            transaction.execute("CALL db.prepareForReplanning()").close();
            transaction.commit();
        }

        // Then
        IndexSamplingMode mode = monitor.samplingMode();
        assertNotEquals(IndexSamplingMode.NO_WAIT, mode.millisToWaitForCompletion());
        assertThat(mode.millisToWaitForCompletion()).isGreaterThan(0L);
    }

    private IndexSamplingMonitor indexSamplingMonitor() {
        Monitors monitors = dependencyResolver.resolveDependency(Monitors.class);

        IndexSamplingMonitor monitorListener = new IndexSamplingMonitor();
        monitors.addMonitorListener(monitorListener);
        return monitorListener;
    }

    private CacheMetrics preParserCacheMetricsMonitor() {
        var statistics = dependencyResolver.resolveDependency(QueryCacheStatistics.class);
        return statistics.metricsPerCacheKind().get(CypherQueryCaches.PreParserCache$.MODULE$.kind());
    }

    private static class IndexSamplingMonitor extends IndexMonitor.MonitorAdapter {
        private IndexSamplingMode samplingMode;

        @Override
        public void indexSamplingTriggered(IndexSamplingMode mode) {
            samplingMode = mode;
        }

        IndexSamplingMode samplingMode() {
            return samplingMode;
        }
    }
}
