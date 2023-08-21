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
package org.neo4j.fabric;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.neo4j.configuration.GraphDatabaseSettings.DEFAULT_DATABASE_NAME;
import static org.neo4j.configuration.GraphDatabaseSettings.SYSTEM_DATABASE_NAME;
import static org.neo4j.internal.helpers.Strings.joinAsLines;
import static org.neo4j.internal.kernel.api.connectioninfo.ClientConnectionInfo.EMBEDDED_CONNECTION;
import static org.neo4j.internal.kernel.api.security.LoginContext.AUTH_DISABLED;

import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.function.Function;
import java.util.stream.Collectors;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.neo4j.bolt.protocol.common.message.request.connection.RoutingContext;
import org.neo4j.configuration.connectors.ConnectorPortRegister;
import org.neo4j.driver.AccessMode;
import org.neo4j.driver.Driver;
import org.neo4j.driver.Session;
import org.neo4j.driver.SessionConfig;
import org.neo4j.driver.Transaction;
import org.neo4j.driver.exceptions.ClientException;
import org.neo4j.fabric.bookmark.TransactionBookmarkManager;
import org.neo4j.fabric.executor.FabricExecutor;
import org.neo4j.fabric.transaction.FabricTransactionInfo;
import org.neo4j.fabric.transaction.TransactionManager;
import org.neo4j.kernel.database.DatabaseIdFactory;
import org.neo4j.kernel.database.DatabaseReferenceImpl;
import org.neo4j.kernel.database.NormalizedDatabaseName;
import org.neo4j.kernel.impl.query.QueryExecutionConfiguration;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.test.extension.BoltDbmsExtension;
import org.neo4j.test.extension.Inject;
import org.neo4j.values.virtual.MapValue;

// TODO: this test has been replaced by CommunityQueryRoutingAcceptanceTest
// for the new Query router stack, so it can be removed when the old stack goes away
@BoltDbmsExtension
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class CommunityEditionEndToEndTest {

    @Inject
    private static GraphDatabaseAPI graphDatabase;

    @Inject
    private static ConnectorPortRegister connectorPortRegister;

    private static Driver driver;

    @BeforeAll
    static void beforeAll() {
        driver = DriverUtils.createDriver(connectorPortRegister);
    }

    @BeforeEach
    void beforeEach() {
        try (Transaction tx = driver.session().beginTransaction()) {
            tx.run("MATCH (n) DETACH DELETE n");
            tx.run("CREATE (:Person {name: 'Anna', uid: 0, age: 30})").consume();
            tx.run("CREATE (:Person {name: 'Bob',  uid: 1, age: 40})").consume();
            tx.commit();
        }
    }

    @AfterAll
    static void afterAll() {
        driver.close();
    }

    @Test
    void testUse() {
        doTestUse(DEFAULT_DATABASE_NAME);
        doTestUse(SYSTEM_DATABASE_NAME);
    }

    @Test
    void testSystemCommandRouting() {
        execute(DEFAULT_DATABASE_NAME, session -> session.run("CREATE USER foo SET PASSWORD 'password'")
                .consume());

        List<String> result = execute(DEFAULT_DATABASE_NAME, session -> session.run("SHOW USERS")
                        .list())
                .stream()
                .map(r -> r.get("user").asString())
                .collect(Collectors.toList());

        assertThat(result).contains("foo");
    }

    @Test
    void testSchemaCommand() {
        doTestSchemaCommands(DEFAULT_DATABASE_NAME);
        doTestSchemaCommands(SYSTEM_DATABASE_NAME);
    }

    @Test
    void testWriteInReadModeShouldFail() {
        ClientException ex = assertThrows(ClientException.class, () -> {
            try (var session = driver.session(SessionConfig.builder()
                    .withDefaultAccessMode(AccessMode.READ)
                    .build())) {
                var query = joinAsLines("CREATE (n:Test)", "RETURN n");
                session.run(query).list();
            }
        });

        assertThat(ex.getMessage()).containsIgnoringCase("Writing in read access mode not allowed");
    }

    @Test
    void testNoRollbackOnStatementFailure() {
        // this is intentionally not using the driver, because the driver closes transactions on any failure

        var dependencyResolver = graphDatabase.getDependencyResolver();
        var transactionManager = dependencyResolver.resolveDependency(TransactionManager.class);
        var fabricExecutor = dependencyResolver.resolveDependency(FabricExecutor.class);
        var databaseName = new NormalizedDatabaseName("neo4j");
        var databaseId = DatabaseIdFactory.from(databaseName.name(), UUID.randomUUID());
        var databaseRef = new DatabaseReferenceImpl.Internal(databaseName, databaseId, true);
        var transactionInfo = new FabricTransactionInfo(
                org.neo4j.bolt.protocol.common.message.AccessMode.READ,
                AUTH_DISABLED,
                EMBEDDED_CONNECTION,
                databaseRef,
                false,
                Duration.ZERO,
                Map.of(),
                new RoutingContext(false, Map.of()),
                QueryExecutionConfiguration.DEFAULT_CONFIG);
        var bookmarkManager = mock(TransactionBookmarkManager.class);

        var tx1 = transactionManager.begin(transactionInfo, bookmarkManager);
        var tx2 = transactionManager.begin(transactionInfo, bookmarkManager);

        assertEquals(2, transactionManager.getOpenTransactions().size());

        var query1 = joinAsLines("USE neo4j", "RETURN 1/0 AS res");

        assertThrows(org.neo4j.exceptions.ArithmeticException.class, () -> fabricExecutor
                .run(tx1, query1, MapValue.EMPTY)
                .records()
                .collectList()
                .block());

        var query2 = joinAsLines("USE neo4j", "UNWIND [1, 0] AS a", "RETURN 1/a AS res");

        assertThrows(org.neo4j.exceptions.ArithmeticException.class, () -> fabricExecutor
                .run(tx2, query2, MapValue.EMPTY)
                .records()
                .collectList()
                .block());

        // The Fabric layer should not rollback transactions automatically on failure,
        // but the community Cypher runtime will mark the transaction as terminated
        assertTrue(tx1.getReasonIfTerminated().isPresent());
        assertTrue(tx2.getReasonIfTerminated().isPresent());
        assertFalse(transactionManager.getOpenTransactions().isEmpty());

        tx1.rollback();
        tx2.rollback();
    }

    private static void doTestUse(String database) {
        List<String> result = execute(database, session -> session.run(
                                "USE " + DEFAULT_DATABASE_NAME + " MATCH (n) RETURN n.name AS name")
                        .list())
                .stream()
                .map(r -> r.get("name").asString())
                .collect(Collectors.toList());

        assertThat(result).contains("Anna", "Bob");
    }

    private static void doTestSchemaCommands(String database) {
        execute(database, session -> session.run(
                        "USE " + DEFAULT_DATABASE_NAME + " CREATE INDEX myIndex FOR (n:Person) ON (n.name)")
                .consume());

        List<String> result = execute(database, session -> session.run(
                                "USE " + DEFAULT_DATABASE_NAME + " SHOW INDEXES YIELD name RETURN name")
                        .list())
                .stream()
                .map(r -> r.get("name").asString())
                .collect(Collectors.toList());
        assertThat(result).contains("myIndex");

        execute(database, session -> session.run("USE " + DEFAULT_DATABASE_NAME + " DROP INDEX myIndex")
                .consume());
    }

    private static <T> T execute(String database, Function<Session, T> workload) {
        try (var session = driver.session(SessionConfig.forDatabase(database))) {
            return workload.apply(session);
        }
    }
}
