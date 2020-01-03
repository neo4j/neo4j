/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
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
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.kernel.impl.core;

import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.graphdb.Transaction;
import org.neo4j.test.TestGraphDatabaseFactory;

import static org.neo4j.graphdb.RelationshipType.withName;

public abstract class RelationshipsIterationTestSupport
{
    private static final TestGraphDatabaseFactory FACTORY = new TestGraphDatabaseFactory();
    protected static final int DENSE_NODE_THRESHOLD = 51;
    private static GraphDatabaseService DATABASE;

    protected GraphDatabaseService db;
    protected RelationshipType typeA = withName( "A" );
    protected RelationshipType typeB = withName( "B" );
    protected RelationshipType typeC = withName( "C" );
    protected RelationshipType typeD = withName( "D" );
    protected RelationshipType typeX = withName( "X" );

    @BeforeClass
    public static void setUp()
    {
        DATABASE = FACTORY.newImpermanentDatabase();
    }

    @AfterClass
    public static void tearDown()
    {
        DATABASE.shutdown();
    }

    @Before
    public void setUpEach()
    {
        db = DATABASE;
        try ( Transaction tx = db.beginTx() )
        {
            db.getAllRelationships().forEach( Relationship::delete );
            db.getAllNodes().forEach( Node::delete );
            tx.success();
        }
    }

    public interface Check
    {
        void check( Node first, Node unrelated, Node second );
    }
}
