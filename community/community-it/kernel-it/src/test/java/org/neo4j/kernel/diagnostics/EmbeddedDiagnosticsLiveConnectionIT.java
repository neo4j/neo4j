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
package org.neo4j.kernel.diagnostics;

import static org.assertj.core.api.Assertions.assertThat;
import static org.neo4j.configuration.GraphDatabaseSettings.DEFAULT_DATABASE_NAME;
import static org.neo4j.configuration.GraphDatabaseSettings.SYSTEM_DATABASE_NAME;

import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;
import org.neo4j.dbms.api.DatabaseManagementService;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Transaction;
import org.neo4j.test.extension.ImpermanentDbmsExtension;
import org.neo4j.test.extension.Inject;

/**
 * Verifies that {@link EmbeddedDiagnosticsLiveConnection} runs the authenticated-provider queries directly against the
 * embedded databases - i.e. without a Bolt connection or credentials - which is what lets the diagnostics report be
 * collected from within a running instance (fleet management).
 */
@ImpermanentDbmsExtension
class EmbeddedDiagnosticsLiveConnectionIT {
    @Inject
    private DatabaseManagementService managementService;

    @Inject
    private GraphDatabaseService graphdb;

    @Test
    void runsQueryAgainstNamedDatabaseWithoutAuthentication() {
        try (Transaction tx = graphdb.beginTx()) {
            tx.execute("CREATE INDEX my_index IF NOT EXISTS FOR (n:Person) ON (n.name)");
            tx.commit();
        }

        try (EmbeddedDiagnosticsLiveConnection connection =
                new EmbeddedDiagnosticsLiveConnection(managementService, DEFAULT_DATABASE_NAME)) {
            DiagnosticsQueryResult result = connection.execute(DEFAULT_DATABASE_NAME, "SHOW INDEXES YIELD *");

            assertThat(result.columns()).contains("name");
            assertThat(result.rows().stream().map(row -> row.get("name"))).contains("my_index");
        }
    }

    @Test
    void runsDbmsLevelQueryAgainstSystemDatabase() {
        try (EmbeddedDiagnosticsLiveConnection connection =
                new EmbeddedDiagnosticsLiveConnection(managementService, DEFAULT_DATABASE_NAME)) {
            DiagnosticsQueryResult result = connection.execute(SYSTEM_DATABASE_NAME, "SHOW DATABASES YIELD *");

            assertThat(result.rows().stream().map(row -> row.get("name")))
                    .contains(DEFAULT_DATABASE_NAME, SYSTEM_DATABASE_NAME);
        }
    }

    @Test
    void usesDefaultDatabaseWhenNoneSpecified() {
        try (EmbeddedDiagnosticsLiveConnection connection =
                new EmbeddedDiagnosticsLiveConnection(managementService, DEFAULT_DATABASE_NAME)) {
            DiagnosticsQueryResult viaNull = connection.execute(null, "RETURN 1 AS value");

            assertThat(viaNull.columns()).containsExactly("value");
            assertThat(viaNull.rows()).containsExactly(Map.of("value", 1L));
            assertThat(viaNull.rows()).isEqualTo(List.of(Map.of("value", 1L)));
        }
    }
}
