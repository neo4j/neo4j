/*
 * Copyright (c) 2008-2009 "Neo Technology,"
 *     Network Engine for Objects in Lund AB [http://neotechnology.com]
 *
 * This file is part of Neo4j.
 * 
 * Neo4j is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 * 
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 * 
 * You should have received a copy of the GNU Affero General Public License
 * along with this program. If not, see <http://www.gnu.org/licenses/>.
 */
package local;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.graphdb.Transaction;

/**
 * A few test cases.
 * 
 * @author Tobias Ivarsson
 */
public class BasicTest extends Base {

    private enum TestRelations implements RelationshipType {
        TEST
    }

    @Test
    public void testNothing() throws Exception {
        System.out.println("\ntestNothing");
    }

    @Test
    public void testGetReferenceNode() throws Exception {
        System.out.println("\ntestGetReferenceNode");
        Transaction tx = graphDb().beginTx();
        try {
            println("reference node: " + graphDb().getReferenceNode());
        } finally {
            tx.finish();
        }
    }

    @Test
    public void testCreateRelationship() throws Exception {
        System.out.println("\ntestCreateRelationship");
        Transaction tx = graphDb().beginTx();
        try {
            Node start = graphDb().getReferenceNode();
            Node end = graphDb().createNode();
            Relationship rel = start.createRelationshipTo(end,
                    TestRelations.TEST);
            println("Created relationship: " + rel + " from "
                    + rel.getStartNode() + " to " + rel.getEndNode());
            tx.success();
        } finally {
            tx.finish();
        }
    }

    @Before
    public void setUp() {
        super.setUp();
    }

    @After
    public void tearDown() {
        super.tearDown();
    }
}
