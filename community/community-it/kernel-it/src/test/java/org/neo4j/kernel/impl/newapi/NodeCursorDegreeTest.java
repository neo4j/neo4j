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
package org.neo4j.kernel.impl.newapi;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.neo4j.graphdb.Direction.INCOMING;
import static org.neo4j.io.pagecache.context.CursorContext.NULL_CONTEXT;

import org.junit.jupiter.api.Test;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.storageengine.api.RelationshipSelection;

public class NodeCursorDegreeTest extends KernelAPIReadTestBase<ReadTestSupport> {

    @Test
    void degreeWithRelationshipDeletedInTx() throws Exception {
        int relType;
        long n1, n2, n3, r1, r2;

        // CREATE (n1)-[r1]->(n2)<-[r2]-(n3)
        try (var tx = beginTransaction()) {
            n1 = tx.dataWrite().nodeCreate();
            n2 = tx.dataWrite().nodeCreate();
            n3 = tx.dataWrite().nodeCreate();

            relType = tx.tokenWrite().relationshipTypeGetOrCreateForName("R");

            r1 = tx.dataWrite().relationshipCreate(n1, relType, n2);
            r2 = tx.dataWrite().relationshipCreate(n3, relType, n2);

            tx.commit();
        }

        try (var tx = beginTransaction()) {
            tx.dataWrite().relationshipDelete(r1);

            try (var nodeCursor = tx.cursors().allocateNodeCursor(NULL_CONTEXT)) {
                tx.dataRead().singleNode(n2, nodeCursor);
                assertTrue(nodeCursor.next());
                var degree = nodeCursor.degree(RelationshipSelection.selection(INCOMING));
                int maxDegree = 1;
                var degreeWithMax = nodeCursor.degreeWithMax(maxDegree, RelationshipSelection.selection(INCOMING));

                assertEquals(1, degree);
                assertEquals(
                        Math.min(degree, maxDegree),
                        degreeWithMax); // Fails here, degreeWithMax will be 0 but should be 1
            }
            tx.rollback();
        }
    }

    @Override
    public ReadTestSupport newTestSupport() {
        return new ReadTestSupport();
    }

    @Override
    public void createTestGraph(GraphDatabaseService graphDb) {}
}
