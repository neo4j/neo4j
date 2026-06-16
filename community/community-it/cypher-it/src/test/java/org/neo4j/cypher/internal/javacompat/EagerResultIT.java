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
package org.neo4j.cypher.internal.javacompat;

import static org.assertj.core.api.Assertions.assertThat;
import static org.neo4j.collection.Dependencies.dependenciesOf;
import static org.neo4j.configuration.GraphDatabaseSettings.DEFAULT_DATABASE_NAME;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.neo4j.common.DependencyResolver;
import org.neo4j.configuration.GraphDatabaseInternalSettings;
import org.neo4j.dbms.api.DatabaseManagementService;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.QueryExecutionType;
import org.neo4j.graphdb.ResourceIterator;
import org.neo4j.graphdb.Result;
import org.neo4j.graphdb.Transaction;
import org.neo4j.io.pagecache.context.CursorContext;
import org.neo4j.io.pagecache.context.TransactionIdSnapshot;
import org.neo4j.io.pagecache.context.TransactionIdSnapshotFactory;
import org.neo4j.kernel.impl.coreapi.InternalTransaction;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.snapshot.TestTransactionVersionContextSupplier;
import org.neo4j.storageengine.api.TransactionIdStore;
import org.neo4j.test.TestDatabaseManagementServiceBuilder;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.SkipOnSpd;
import org.neo4j.test.extension.testdirectory.TestDirectoryExtension;
import org.neo4j.test.utils.TestDirectory;

@TestDirectoryExtension
@SkipOnSpd(reason = "Will be superseded by MVCC", notes = SkipOnSpd.Note.irrelevant)
class EagerResultIT {
    @Inject
    private TestDirectory testDirectory;

    private GraphDatabaseService database;
    private TestTransactionVersionContextSupplier.Factory testContextSupplierFactory;
    private Path storeDir;
    private DatabaseManagementService managementService;

    @BeforeEach
    void setUp() {
        storeDir = testDirectory.homePath();
        testContextSupplierFactory = new TestTransactionVersionContextSupplier.Factory();
        database = startRestartableDatabase();
        prepareData();
        TransactionIdStore transactionIdStore = getTransactionIdStore();
        testContextSupplierFactory.setTestVersionContextSupplier(databaseName -> {
            TestVersionContext context;
            if (databaseName.equals(database.databaseName())) {
                context = new TestVersionContext(
                        () -> new TransactionIdSnapshot(transactionIdStore.getHighestGapFreeClosedTransactionId()),
                        databaseName);

            } else {
                context = new TestVersionContext(TransactionIdSnapshotFactory.EMPTY_SNAPSHOT_FACTORY, databaseName);
            }
            context.setWrongLastClosedTxId(false);
            context.initRead();
            return context;
        });
    }

    @AfterEach
    void tearDown() {
        if (managementService != null) {
            managementService.shutdown();
        }
    }

    @Test
    void eagerResultContainsAllData() {
        try (Transaction transaction = database.beginTx()) {
            var versionContext = getTestVersionContext(transaction);
            Result result = transaction.execute("MATCH (n) RETURN n.c");
            assertThat(versionContext.getNumIsDirtyCalls()).isOne();
            int rows = 0;
            while (result.hasNext()) {
                result.next();
                rows++;
            }
            assertThat(rows).isEqualTo(2);
            transaction.commit();
        }
    }

    @Test
    void eagerResultContainsExecutionType() {
        try (Transaction transaction = database.beginTx()) {
            var versionContext = getTestVersionContext(transaction);
            Result result = transaction.execute("MATCH (n) RETURN n.c");
            assertThat(versionContext.getNumIsDirtyCalls()).isOne();
            assertThat(result.getQueryExecutionType())
                    .isEqualTo(QueryExecutionType.query(QueryExecutionType.QueryType.READ_ONLY));
            transaction.commit();
        }
    }

    @Test
    void eagerResultContainsColumns() {
        try (Transaction transaction = database.beginTx()) {
            var versionContext = getTestVersionContext(transaction);
            Result result = transaction.execute("MATCH (n) RETURN n.c as a, count(n) as b");
            assertThat(versionContext.getNumIsDirtyCalls()).isOne();
            assertThat(result.columns()).containsExactlyElementsOf(Arrays.asList("a", "b"));
            transaction.commit();
        }
    }

    @Test
    void useColumnAsOnEagerResult() {
        try (Transaction transaction = database.beginTx()) {
            var versionContext = getTestVersionContext(transaction);
            Result result = transaction.execute("MATCH (n) RETURN n.c as c, n.b as b");
            assertThat(versionContext.getNumIsDirtyCalls()).isOne();
            ResourceIterator<Object> cValues = result.columnAs("c");
            int rows = 0;
            while (cValues.hasNext()) {
                cValues.next();
                rows++;
            }
            assertThat(rows).isEqualTo(2);
            transaction.commit();
        }
    }

    @Test
    void eagerResultHaveQueryStatistic() {
        try (Transaction transaction = database.beginTx()) {
            var versionContext = getTestVersionContext(transaction);
            Result result = transaction.execute("MATCH (n) RETURN n.c");
            assertThat(versionContext.getNumIsDirtyCalls()).isOne();
            assertThat(result.getQueryStatistics().containsUpdates()).isFalse();
            transaction.commit();
        }
    }

    @Test
    void eagerResultHaveExecutionPlan() {
        try (Transaction transaction = database.beginTx()) {
            var versionContext = getTestVersionContext(transaction);
            Result result = transaction.execute("profile MATCH (n) RETURN n.c");
            assertThat(versionContext.getNumIsDirtyCalls()).isOne();
            assertThat(result.getExecutionPlanDescription()
                            .getProfilerStatistics()
                            .getRows())
                    .isEqualTo(2);
            transaction.commit();
        }
    }

    @Test
    void eagerResultToString() {
        try (Transaction transaction = database.beginTx()) {
            var versionContext = getTestVersionContext(transaction);
            Result result = transaction.execute("MATCH (n) RETURN n.c, n.d");
            assertThat(versionContext.getNumIsDirtyCalls()).isOne();
            String resultString = result.resultAsString();
            assertThat(resultString)
                    .contains("n.c | n.d")
                    .contains("\"d\" | \"a\"")
                    .contains("\"y\" | \"k\"");
            transaction.commit();
        }
    }

    @Test
    void eagerResultWriteAsStringToStream() {
        try (Transaction transaction = database.beginTx()) {
            var versionContext = getTestVersionContext(transaction);
            Result result = transaction.execute("MATCH (n) RETURN n.c");
            assertThat(versionContext.getNumIsDirtyCalls()).isOne();
            String expected = "+-----+" + System.lineSeparator() + "| n.c |"
                    + System.lineSeparator() + "+-----+"
                    + System.lineSeparator() + "| \"d\" |"
                    + System.lineSeparator() + "| \"y\" |"
                    + System.lineSeparator() + "+-----+"
                    + System.lineSeparator() + "2 rows"
                    + System.lineSeparator();
            assertThat(printToStream(result)).isEqualTo(expected);
            transaction.commit();
        }
    }

    @Test
    void eagerResultVisit() throws Exception {
        try (Transaction transaction = database.beginTx()) {
            Result result = transaction.execute("MATCH (n) RETURN n.c");
            List<String> values = new ArrayList<>();
            result.accept((Result.ResultVisitor<Exception>) row -> {
                values.add(row.getString("n.c"));
                return true;
            });
            assertThat(values).hasSize(2);
            assertThat(values).containsExactlyInAnyOrder("d", "y");
            transaction.commit();
        }
    }

    private static String printToStream(Result result) {
        StringWriter stringWriter = new StringWriter();
        PrintWriter printWriter = new PrintWriter(stringWriter);
        result.writeAsStringTo(printWriter);
        printWriter.flush();
        return stringWriter.toString();
    }

    private void prepareData() {
        Label label = Label.label("label");
        try (Transaction transaction = database.beginTx()) {
            Node node = transaction.createNode(label);
            node.setProperty("c", "d");
            node.setProperty("d", "a");
            transaction.commit();
        }
        try (Transaction transaction = database.beginTx()) {
            Node node = transaction.createNode(label);
            node.setProperty("c", "y");
            node.setProperty("d", "k");
            transaction.commit();
        }
    }

    private GraphDatabaseService startRestartableDatabase() {
        managementService = new TestDatabaseManagementServiceBuilder(storeDir)
                .setExternalDependencies(dependenciesOf(testContextSupplierFactory))
                .setConfig(GraphDatabaseInternalSettings.snapshot_query, true)
                .build();
        return managementService.database(DEFAULT_DATABASE_NAME);
    }

    private TransactionIdStore getTransactionIdStore() {
        DependencyResolver dependencyResolver = ((GraphDatabaseAPI) database).getDependencyResolver();
        return dependencyResolver.resolveDependency(TransactionIdStore.class);
    }

    private static class TestVersionContext extends org.neo4j.snapshot.TestVersionContext {
        private boolean useCorrectLastCommittedTxId;

        TestVersionContext(TransactionIdSnapshotFactory snapshotFactory, String databaseName) {
            super(snapshotFactory, databaseName, true);
        }

        @Override
        public long highestGapFree() {
            return useCorrectLastCommittedTxId ? TransactionIdStore.BASE_TX_ID : super.highestGapFree();
        }

        @Override
        public void markAsDirty() {
            super.markAsDirty();
            useCorrectLastCommittedTxId = true;
        }
    }

    private static org.neo4j.snapshot.TestVersionContext getTestVersionContext(Transaction transaction) {
        CursorContext cursorContext =
                ((InternalTransaction) transaction).kernelTransaction().cursorContext();
        return (org.neo4j.snapshot.TestVersionContext) cursorContext.getVersionContext();
    }
}
