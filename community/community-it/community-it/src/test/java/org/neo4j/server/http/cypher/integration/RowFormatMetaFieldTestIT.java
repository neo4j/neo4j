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
package org.neo4j.server.http.cypher.integration;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.fail;
import static org.neo4j.server.http.cypher.integration.TransactionConditions.containsNoErrors;
import static org.neo4j.server.http.cypher.integration.TransactionConditions.rowContainsAMetaListAtIndex;
import static org.neo4j.server.http.cypher.integration.TransactionConditions.rowContainsMetaNodesAtIndex;
import static org.neo4j.server.http.cypher.integration.TransactionConditions.rowContainsMetaRelsAtIndex;
import static org.neo4j.test.server.HTTP.RawPayload.quotedJson;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Transaction;
import org.neo4j.server.rest.AbstractRestFunctionalTestBase;
import org.neo4j.server.rest.domain.JsonParseException;
import org.neo4j.test.server.HTTP;
import org.neo4j.test.server.HTTP.Response;

public class RowFormatMetaFieldTestIT extends AbstractRestFunctionalTestBase {
    private String commitResource;

    @BeforeEach
    public void setUp() {
        // begin
        Response begin = http.POST(txUri());

        assertThat(begin.status()).isEqualTo(201);
        assertHasTxLocation(begin);
        try {
            commitResource = begin.stringFromContent("commit");
        } catch (JsonParseException e) {
            fail("Exception caught when setting up test: " + e.getMessage());
        }
        assertThat(commitResource).isEqualTo(begin.location() + "/commit");
    }

    @AfterEach
    public void tearDown() {
        // empty the database
        executeTransactionally("MATCH (n) DETACH DELETE n");
    }

    @Test
    public void metaFieldShouldGetCorrectIndex() {
        // given
        executeTransactionally("CREATE (:Start)-[:R]->(:End)");

        // execute and commit
        Response commit = http.POST(commitResource, queryAsJsonRow("MATCH (s:Start)-[r:R]->(e:End) RETURN s, r, 1, e"));

        assertThat(commit).satisfies(containsNoErrors());
        assertThat(commit).satisfies(rowContainsMetaNodesAtIndex(0, 3));
        assertThat(commit).satisfies(rowContainsMetaRelsAtIndex(1));
        assertThat(commit.status()).isEqualTo(200);
    }

    @Test
    public void metaFieldShouldGivePathInfoInList() {
        // given
        executeTransactionally("CREATE (:Start)-[:R]->(:End)");

        // execute and commit
        Response commit = http.POST(commitResource, queryAsJsonRow("MATCH p=(s)-[r:R]->(e) RETURN p"));

        assertThat(commit).satisfies(containsNoErrors());
        assertThat(commit).satisfies(rowContainsAMetaListAtIndex(0));
        assertThat(commit.status()).isEqualTo(200);
    }

    @Test
    public void metaFieldShouldPutPathListAtCorrectIndex() {
        // given
        executeTransactionally("CREATE (:Start)-[:R]->(:End)");

        // execute and commit
        Response commit = http.POST(commitResource, queryAsJsonRow("MATCH p=(s)-[r:R]->(e) RETURN 10, p"));

        assertThat(commit).satisfies(containsNoErrors());
        assertThat(commit).satisfies(rowContainsAMetaListAtIndex(1));
        assertThat(commit.status()).isEqualTo(200);
    }

    private void executeTransactionally(String query) {
        GraphDatabaseService database = graphdb();
        try (Transaction transaction = database.beginTx()) {
            transaction.execute(query);
            transaction.commit();
        }
    }

    private static HTTP.RawPayload queryAsJsonRow(String query) {
        return quotedJson("{ 'statements': [ { 'statement': '" + query + "', 'resultDataContents': [ 'row' ] } ] }");
    }
}
