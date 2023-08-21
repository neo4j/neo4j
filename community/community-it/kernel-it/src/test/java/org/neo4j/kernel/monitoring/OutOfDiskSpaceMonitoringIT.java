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
package org.neo4j.kernel.monitoring;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatNoException;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.neo4j.configuration.GraphDatabaseSettings.DEFAULT_DATABASE_NAME;
import static org.neo4j.configuration.GraphDatabaseSettings.SYSTEM_DATABASE_NAME;
import static org.neo4j.configuration.GraphDatabaseSettings.read_only_databases;

import org.junit.jupiter.api.Test;
import org.neo4j.dbms.api.DatabaseManagementService;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.event.DatabaseEventContext;
import org.neo4j.graphdb.event.DatabaseEventListenerAdapter;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.logging.AssertableLogProvider;
import org.neo4j.logging.LogAssertions;
import org.neo4j.monitoring.DatabaseHealth;
import org.neo4j.test.TestDatabaseManagementServiceBuilder;
import org.neo4j.test.extension.ExtensionCallback;
import org.neo4j.test.extension.ImpermanentDbmsExtension;
import org.neo4j.test.extension.Inject;

@ImpermanentDbmsExtension(configurationCallback = "configuration")
public class OutOfDiskSpaceMonitoringIT {
    @ExtensionCallback
    void configuration(TestDatabaseManagementServiceBuilder builder) {
        var listener = new DatabaseEventListenerAdapter() {
            @Override
            public void databaseOutOfDiskSpace(DatabaseEventContext event) {
                eventContext = event;
            }
        };
        builder.addDatabaseListener(listener);
        builder.setInternalLogProvider(logProvider);
    }

    private final AssertableLogProvider logProvider = new AssertableLogProvider(true);
    private DatabaseEventContext eventContext;

    @Inject
    private DatabaseHealth databaseHealth;

    @Inject
    private GraphDatabaseService db;

    @Inject
    private DatabaseManagementService dbms;

    @Test
    void shouldPropagateOutOfDiskSpaceEventToRegisteredListener() {
        // When
        databaseHealth.outOfDiskSpace(new RuntimeException("Leeeroooy!"));

        // Then
        assertThat(eventContext).isNotNull();
        assertThat(eventContext.getDatabaseName()).isEqualTo(DEFAULT_DATABASE_NAME);
    }

    @Test
    void shouldPutDatabaseIntoReadOnlyState() {
        // When
        databaseHealth.outOfDiskSpace(new RuntimeException("C'mon Leroy, it's not funny!"));

        // Then
        try (Transaction tx = db.beginTx()) {
            assertThatThrownBy(tx::createNode)
                    .hasMessageContaining(
                            "No write operations are allowed on this database. The database is in read-only mode on this Neo4j instance.");
            tx.commit();
        }
    }

    @Test
    void shouldLogAboutOutOfDiskSpace() {
        // When
        var cause = new RuntimeException("Again Leroy?!");
        databaseHealth.outOfDiskSpace(cause);

        // Then
        LogAssertions.assertThat(logProvider)
                .containsMessages("Database out of disk space: ")
                .containsMessages(DatabaseHealth.outOfDiskSpaceMessage)
                .containsException(cause);
    }

    @Test
    void shouldLogAboutReadOnly() {
        // When
        var cause = new RuntimeException("Leroy, please...");
        databaseHealth.outOfDiskSpace(cause);

        // Then
        LogAssertions.assertThat(logProvider)
                .containsMessages(
                        "As a result of the database failing to allocate enough disk space, it has been put into read-only mode to protect from system failure and ensure data integrity. ",
                        "Please free up more disk space before changing access mode for database back to read-write state. ",
                        "Making database writable again can be done by:",
                        "    CALL dbms.listConfig(\"" + read_only_databases.name() + "\") YIELD value",
                        "    WITH value",
                        "    CALL dbms.setConfigValue(\"" + read_only_databases.name()
                                + "\", replace(value, \"<databaseName>\", \"\"))");
    }

    @Test
    void outOfDiskSpaceOnSystemDbShouldNotAffectReadOnly() {
        RuntimeException cause = new RuntimeException("System db exception");

        GraphDatabaseAPI system = (GraphDatabaseAPI) dbms.database(SYSTEM_DATABASE_NAME);
        var systemHealth = system.getDependencyResolver().resolveDependency(DatabaseHealth.class);
        systemHealth.outOfDiskSpace(cause);

        // Out of disk space logging but no read-only logging.
        LogAssertions.assertThat(logProvider)
                .containsMessages("Database out of disk space: ")
                .containsMessages(DatabaseHealth.outOfDiskSpaceMessage)
                .containsException(cause)
                .doesNotContainMessage("has been put into read-only mode");

        // Still writeable
        try (Transaction tx = system.beginTx()) {
            assertThatNoException().isThrownBy(tx::createNode);
            tx.commit();
        }
    }
}
