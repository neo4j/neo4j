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

import static org.neo4j.logging.LogAssertions.assertThat;

import org.junit.jupiter.api.Test;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Transaction;
import org.neo4j.logging.AssertableLogProvider;
import org.neo4j.test.extension.ImpermanentDbmsExtension;
import org.neo4j.test.extension.Inject;

@ImpermanentDbmsExtension
public class CypherLoggingTest {
    private final AssertableLogProvider logProvider = new AssertableLogProvider();

    @Inject
    private GraphDatabaseService database;

    @Test
    public void shouldNotLogQueries() {
        // when
        try (Transaction transaction = database.beginTx()) {
            transaction
                    .execute("CREATE (n:Reference) CREATE (foo {test:'me'}) RETURN n")
                    .close();
            transaction.execute("MATCH (n) RETURN n").close();
        }

        // then
        assertThat(logProvider).doesNotHaveAnyLogs();
    }
}
