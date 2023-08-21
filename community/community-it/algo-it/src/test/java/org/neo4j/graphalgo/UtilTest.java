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
package org.neo4j.graphalgo;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;
import org.neo4j.graphalgo.impl.shortestpath.Util;
import org.neo4j.graphalgo.impl.shortestpath.Util.PathCounter;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.Transaction;

class UtilTest extends Neo4jAlgoTestCase {
    @Test
    void testPathCounter() {
        // Nodes
        try (Transaction tx = graphDb.beginTx()) {
            Node a = tx.createNode();
            Node b = tx.createNode();
            Node c = tx.createNode();
            Node d = tx.createNode();
            Node e = tx.createNode();
            Node f = tx.createNode();
            // Predecessor lists
            List<Relationship> ap = new LinkedList<>();
            List<Relationship> bp = new LinkedList<>();
            List<Relationship> cp = new LinkedList<>();
            List<Relationship> dp = new LinkedList<>();
            List<Relationship> ep = new LinkedList<>();
            List<Relationship> fp = new LinkedList<>();
            // Predecessor map
            Map<Node, List<Relationship>> predecessors = new HashMap<>();
            predecessors.put(a, ap);
            predecessors.put(b, bp);
            predecessors.put(c, cp);
            predecessors.put(d, dp);
            predecessors.put(e, ep);
            predecessors.put(f, fp);
            // Add relations
            fp.add(f.createRelationshipTo(c, MyRelTypes.R1));
            fp.add(f.createRelationshipTo(e, MyRelTypes.R1));
            ep.add(e.createRelationshipTo(b, MyRelTypes.R1));
            ep.add(e.createRelationshipTo(d, MyRelTypes.R1));
            dp.add(d.createRelationshipTo(a, MyRelTypes.R1));
            cp.add(c.createRelationshipTo(b, MyRelTypes.R1));
            bp.add(b.createRelationshipTo(a, MyRelTypes.R1));
            // Count
            PathCounter counter = new PathCounter(predecessors);
            assertEquals(1, counter.getNumberOfPathsToNode(a));
            assertEquals(1, counter.getNumberOfPathsToNode(b));
            assertEquals(1, counter.getNumberOfPathsToNode(c));
            assertEquals(1, counter.getNumberOfPathsToNode(d));
            assertEquals(2, counter.getNumberOfPathsToNode(e));
            assertEquals(3, counter.getNumberOfPathsToNode(f));
            // Reverse
            counter = new PathCounter(Util.reversedPredecessors(predecessors));
            assertEquals(3, counter.getNumberOfPathsToNode(a));
            assertEquals(2, counter.getNumberOfPathsToNode(b));
            assertEquals(1, counter.getNumberOfPathsToNode(c));
            assertEquals(1, counter.getNumberOfPathsToNode(d));
            assertEquals(1, counter.getNumberOfPathsToNode(e));
            assertEquals(1, counter.getNumberOfPathsToNode(f));
            tx.commit();
        }
    }
}
