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
package org.neo4j.harness.junit.extension;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertNotNull;

import org.junit.jupiter.api.RepeatedTest;
import org.junit.jupiter.api.RepetitionInfo;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Transaction;
import org.neo4j.harness.Neo4j;
import org.neo4j.internal.helpers.collection.Iterables;
import org.neo4j.test.server.HTTP;

class Neo4jExtensionRegisterStaticIT {
    @RegisterExtension
    static Neo4jExtension neo4jExtension = Neo4jExtension.builder().build();

    @Test
    void neo4jAvailable(Neo4j neo4j) {
        assertNotNull(neo4j);
        assertThat(HTTP.GET(neo4j.httpURI().toString()).status()).isEqualTo(200);
    }

    @RepeatedTest(2)
    void accumulateNodes(GraphDatabaseService databaseService, RepetitionInfo repetitionInfo) {
        try (Transaction transaction = databaseService.beginTx()) {
            transaction.createNode();
            transaction.commit();
        }
        try (Transaction transaction = databaseService.beginTx()) {
            assertThat(Iterables.count(transaction.getAllNodes())).isEqualTo(repetitionInfo.getCurrentRepetition());
        }
    }
}
