/*
 * Copyright (c) 2002-2019 "Neo4j,"
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

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import org.neo4j.dbms.api.DatabaseManagementService;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.graphdb.Transaction;
import org.neo4j.test.TestDatabaseManagementServiceBuilder;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.neo4j.configuration.GraphDatabaseSettings.DEFAULT_DATABASE_NAME;
import static org.neo4j.graphdb.Direction.BOTH;
import static org.neo4j.graphdb.Direction.INCOMING;
import static org.neo4j.graphdb.Direction.OUTGOING;
import static org.neo4j.graphdb.RelationshipType.withName;


class RelationshipsIterationTest
{
    private static final TestDatabaseManagementServiceBuilder BUILDER = new TestDatabaseManagementServiceBuilder();
    private static final int DENSE_NODE_THRESHOLD = 51;
    private static DatabaseManagementService DATABASE;

    private GraphDatabaseService db;
    private RelationshipType typeA = withName( "A" );
    private RelationshipType typeB = withName( "B" );
    private RelationshipType typeC = withName( "C" );
    private RelationshipType typeD = withName( "D" );
    private RelationshipType typeX = withName( "X" );

    @BeforeAll
    static void setUp()
    {
        DATABASE = BUILDER.impermanent().build();
    }

    @AfterAll
    static void tearDown()
    {
        DATABASE.shutdown();
    }

    @BeforeEach
    void setUpEach()
    {
        db = DATABASE.database( DEFAULT_DATABASE_NAME );
        try ( Transaction tx = db.beginTx() )
        {
            tx.getAllRelationships().forEach( Relationship::delete );
            tx.getAllNodes().forEach( Node::delete );
            tx.commit();
        }
    }

    abstract class RelationshipTraversalCursorReuseMustNotFalselyMatchRelationships
    {
        long matchingFirst;
        long notMatching;
        long matchingSecond;

        @BeforeEach
        void setUp()
        {
            try ( Transaction tx = db.beginTx() )
            {
                Node first = tx.createNode();
                Node unrelated = tx.createNode();
                Node second = tx.createNode();
                matchingFirst = first.getId();
                notMatching = unrelated.getId();
                matchingSecond = second.getId();
                first.createRelationshipTo( second, typeA );
                first.createRelationshipTo( second, typeB );
                first.createRelationshipTo( unrelated, typeC );
                second.createRelationshipTo( unrelated, typeD );
                tx.commit();
            }
            try ( Transaction tx = db.beginTx() )
            {
                Node first = tx.getNodeById( matchingFirst );
                Node second = tx.getNodeById( matchingSecond );
                first.createRelationshipTo( second, typeA );
                first.createRelationshipTo( second, typeB );
                tx.commit();
            }
        }

        @Test
        void matchNotMatch1()
        {
            check( ( first, unrelated, second ) ->
            {
                assertTrue( first.hasRelationship( typeA ) );
                assertFalse( unrelated.hasRelationship( typeA ) );
                assertTrue( second.hasRelationship( typeA ) );
            } );
        }

        @Test
        void matchNotMatch2()
        {
            check( ( first, unrelated, second ) ->
            {
                assertTrue( first.hasRelationship( typeA ) );
                assertFalse( unrelated.hasRelationship( typeA ) );
                assertTrue( first.hasRelationship( typeA ) );
            } );
        }

        @Test
        void matchNotMatch3()
        {
            check( ( first, unrelated, second ) ->
            {
                assertTrue( first.hasRelationship( typeA ) );
                assertFalse( first.hasRelationship( typeD ) );
                assertTrue( first.hasRelationship( typeA ) );
            } );
        }

        @Test
        void matchNotMatch4()
        {
            check( ( first, unrelated, second ) ->
            {
                assertTrue( second.hasRelationship( typeA ) );
                assertFalse( unrelated.hasRelationship( typeA ) );
                assertTrue( first.hasRelationship( typeA ) );
            } );
        }

        @Test
        void matchNotMisdir1()
        {
            check( ( first, unrelated, second ) ->
            {
                assertTrue( first.hasRelationship( OUTGOING, typeA ) );
                assertFalse( unrelated.hasRelationship( BOTH, typeA ) );
                assertFalse( second.hasRelationship( OUTGOING, typeA ) );
            } );
        }

        @Test
        void matchNotMisdir2()
        {
            check( ( first, unrelated, second ) ->
            {
                assertTrue( first.hasRelationship( OUTGOING, typeA ) );
                assertFalse( unrelated.hasRelationship( BOTH, typeA ) );
                assertFalse( first.hasRelationship( INCOMING, typeA ) );
            } );
        }

        @Test
        void misdirNotMatch1()
        {
            check( ( first, unrelated, second ) ->
            {
                assertFalse( first.hasRelationship( INCOMING, typeA ) );
                assertFalse( unrelated.hasRelationship( BOTH, typeA ) );
                assertTrue( second.hasRelationship( INCOMING, typeA ) );
            } );
        }

        @Test
        void misdirNotMatch2()
        {
            check( ( first, unrelated, second ) ->
            {
                assertFalse( second.hasRelationship( OUTGOING, typeA ) );
                assertFalse( unrelated.hasRelationship( BOTH, typeA ) );
                assertTrue( first.hasRelationship( OUTGOING, typeA ) );
            } );
        }

        @Test
        void notMatch1()
        {
            check( ( first, unrelated, second ) ->
            {
                assertFalse( unrelated.hasRelationship( typeA ) );
                assertTrue( second.hasRelationship( typeA ) );
            } );
        }

        @Test
        void notMatch2()
        {
            check( ( first, unrelated, second ) ->
            {
                assertFalse( unrelated.hasRelationship( typeA ) );
                assertTrue( first.hasRelationship( typeA ) );
            } );
        }

        @Test
        void misdirMatch1()
        {
            check( ( first, unrelated, second ) ->
            {
                assertFalse( first.hasRelationship( INCOMING, typeA ) );
                assertTrue( second.hasRelationship( INCOMING, typeA ) );
            } );
        }

        @Test
        void misdirMatch2()
        {
            check( ( first, unrelated, second ) ->
            {
                assertFalse( second.hasRelationship( OUTGOING, typeA ) );
                assertTrue( first.hasRelationship( OUTGOING, typeA ) );
            } );
        }

        @Test
        void matchMisdirMatch1()
        {
            check( ( first, unrelated, second ) ->
            {
                assertTrue( first.hasRelationship( OUTGOING, typeA ) );
                assertFalse( first.hasRelationship( INCOMING, typeA ) );
                assertTrue( second.hasRelationship( INCOMING, typeA ) );
            } );
        }

        @Test
        void matchMisdirMatch2()
        {
            check( ( first, unrelated, second ) ->
            {
                assertTrue( second.hasRelationship( INCOMING, typeA ) );
                assertFalse( first.hasRelationship( INCOMING, typeA ) );
                assertTrue( second.hasRelationship( INCOMING, typeA ) );
            } );
        }

        @Test
        void matchMisdirMatch3()
        {
            check( ( first, unrelated, second ) ->
            {
                assertTrue( second.hasRelationship( INCOMING, typeA ) );
                assertFalse( first.hasRelationship( INCOMING, typeA ) );
                assertTrue( first.hasRelationship( OUTGOING, typeA ) );
            } );
        }

        @Test
        void matchMisdirMatch4()
        {
            check( ( first, unrelated, second ) ->
            {
                assertTrue( first.hasRelationship( OUTGOING, typeA ) );
                assertFalse( first.hasRelationship( INCOMING, typeA ) );
                assertTrue( first.hasRelationship( OUTGOING, typeA ) );
            } );
        }

        @Test
        void matchMisdirMatch5()
        {
            check( ( first, unrelated, second ) ->
            {
                assertTrue( second.hasRelationship( INCOMING, typeA ) );
                assertFalse( second.hasRelationship( OUTGOING, typeA ) );
                assertTrue( second.hasRelationship( INCOMING, typeA ) );
            } );
        }

        @Test
        void notMisdir1()
        {
            check( ( first, unrelated, second ) ->
            {
                assertFalse( unrelated.hasRelationship( BOTH, typeA ) );
                assertFalse( second.hasRelationship( OUTGOING, typeA ) );
            } );
        }

        @Test
        void notMisdir2()
        {
            check( ( first, unrelated, second ) ->
            {
                assertFalse( unrelated.hasRelationship( BOTH, typeA ) );
                assertFalse( first.hasRelationship( INCOMING, typeA ) );
            } );
        }

        @Test
        void notMatchNot1()
        {
            check( ( first, unrelated, second ) ->
            {
                assertFalse( unrelated.hasRelationship( typeA ) );
                assertTrue( first.hasRelationship( typeA ) );
                assertFalse( unrelated.hasRelationship( typeA ) );
            } );
        }

        @Test
        void notMatchNot2()
        {
            check( ( first, unrelated, second ) ->
            {
                assertFalse( unrelated.hasRelationship( typeA ) );
                assertTrue( second.hasRelationship( typeA ) );
                assertFalse( unrelated.hasRelationship( typeA ) );
            } );
        }

        // =================================================

        @Test
        void countMatchNotMatch1()
        {
            check( ( first, unrelated, second ) ->
            {
                assertEquals( 2, countTypes( typeA, first.getRelationships( typeA ) ) );
                assertEquals( 0, countTypes( typeA, unrelated.getRelationships( typeA ) ) );
                assertEquals( 2, countTypes( typeA, second.getRelationships( typeA ) ) );
            } );
        }

        @Test
        void countMatchNotMatch2()
        {
            check( ( first, unrelated, second ) ->
            {
                assertEquals( 2, countTypes( typeA, first.getRelationships( typeA ) ) );
                assertEquals( 0, countTypes( typeA, unrelated.getRelationships( typeA ) ) );
                assertEquals( 2, countTypes( typeA, first.getRelationships( typeA ) ) );
            } );
        }

        @Test
        void countMatchNotMatch3()
        {
            check( ( first, unrelated, second ) ->
            {
                assertEquals( 2, countTypes( typeA, first.getRelationships( typeA ) ) );
                assertEquals( 0, countTypes( typeD, first.getRelationships( typeD ) ) );
                assertEquals( 2, countTypes( typeA, first.getRelationships( typeA ) ) );
            } );
        }

        @Test
        void countMatchNotMatch4()
        {
            check( ( first, unrelated, second ) ->
            {
                assertEquals( 2, countTypes( typeA, second.getRelationships( typeA ) ) );
                assertEquals( 0, countTypes( typeA, unrelated.getRelationships( typeA ) ) );
                assertEquals( 2, countTypes( typeA, first.getRelationships( typeA ) ) );
            } );
        }

        @Test
        void countMatchNotMisdir1()
        {
            check( ( first, unrelated, second ) ->
            {
                assertEquals( 2, countTypes( typeA, first.getRelationships( OUTGOING, typeA ) ) );
                assertEquals( 0, countTypes( typeA, unrelated.getRelationships( BOTH, typeA ) ) );
                assertEquals( 0, countTypes( typeA, second.getRelationships( OUTGOING, typeA ) ) );
            } );
        }

        @Test
        void countMatchNotMisdir2()
        {
            check( ( first, unrelated, second ) ->
            {
                assertEquals( 2, countTypes( typeA, first.getRelationships( OUTGOING, typeA ) ) );
                assertEquals( 0, countTypes( typeA, unrelated.getRelationships( BOTH, typeA ) ) );
                assertEquals( 0, countTypes( typeA, first.getRelationships( INCOMING, typeA ) ) );
            } );
        }

        @Test
        void countMisdirNotMatch1()
        {
            check( ( first, unrelated, second ) ->
            {
                assertEquals( 0, countTypes( typeA, first.getRelationships( INCOMING, typeA ) ) );
                assertEquals( 0, countTypes( typeA, unrelated.getRelationships( BOTH, typeA ) ) );
                assertEquals( 2, countTypes( typeA, second.getRelationships( INCOMING, typeA ) ) );
            } );
        }

        @Test
        void countMisdirNotMatch2()
        {
            check( ( first, unrelated, second ) ->
            {
                assertEquals( 0, countTypes( typeA, second.getRelationships( OUTGOING, typeA ) ) );
                assertEquals( 0, countTypes( typeA, unrelated.getRelationships( BOTH, typeA ) ) );
                assertEquals( 2, countTypes( typeA, first.getRelationships( OUTGOING, typeA ) ) );
            } );
        }

        @Test
        void countNotMatch1()
        {
            check( ( first, unrelated, second ) ->
            {
                assertEquals( 0, countTypes( typeA, unrelated.getRelationships( typeA ) ) );
                assertEquals( 2, countTypes( typeA, second.getRelationships( typeA ) ) );
            } );
        }

        @Test
        void countNotMatch2()
        {
            check( ( first, unrelated, second ) ->
            {
                assertEquals( 0, countTypes( typeA, unrelated.getRelationships( typeA ) ) );
                assertEquals( 2, countTypes( typeA, first.getRelationships( typeA ) ) );
            } );
        }

        @Test
        void countMisdirMatch1()
        {
            check( ( first, unrelated, second ) ->
            {
                assertEquals( 0, countTypes( typeA, first.getRelationships( INCOMING, typeA ) ) );
                assertEquals( 2, countTypes( typeA, second.getRelationships( INCOMING, typeA ) ) );
            } );
        }

        @Test
        void countMisdirMatch2()
        {
            check( ( first, unrelated, second ) ->
            {
                assertEquals( 0, countTypes( typeA, second.getRelationships( OUTGOING, typeA ) ) );
                assertEquals( 2, countTypes( typeA, first.getRelationships( OUTGOING, typeA ) ) );
            } );
        }

        @Test
        void countMatchMisdirMatch1()
        {
            check( ( first, unrelated, second ) ->
            {
                assertEquals( 2, countTypes( typeA, first.getRelationships( OUTGOING, typeA ) ) );
                assertEquals( 0, countTypes( typeA, first.getRelationships( INCOMING, typeA ) ) );
                assertEquals( 2, countTypes( typeA, second.getRelationships( INCOMING, typeA ) ) );
            } );
        }

        @Test
        void countMatchMisdirMatch2()
        {
            check( ( first, unrelated, second ) ->
            {
                assertEquals( 2, countTypes( typeA, second.getRelationships( INCOMING, typeA ) ) );
                assertEquals( 0, countTypes( typeA, first.getRelationships( INCOMING, typeA ) ) );
                assertEquals( 2, countTypes( typeA, second.getRelationships( INCOMING, typeA ) ) );
            } );
        }

        @Test
        void countMatchMisdirMatch3()
        {
            check( ( first, unrelated, second ) ->
            {
                assertEquals( 2, countTypes( typeA, second.getRelationships( INCOMING, typeA ) ) );
                assertEquals( 0, countTypes( typeA, first.getRelationships( INCOMING, typeA ) ) );
                assertEquals( 2, countTypes( typeA, first.getRelationships( OUTGOING, typeA ) ) );
            } );
        }

        @Test
        void countMatchMisdirMatch4()
        {
            check( ( first, unrelated, second ) ->
            {
                assertEquals( 2, countTypes( typeA, first.getRelationships( OUTGOING, typeA ) ) );
                assertEquals( 0, countTypes( typeA, first.getRelationships( INCOMING, typeA ) ) );
                assertEquals( 2, countTypes( typeA, first.getRelationships( OUTGOING, typeA ) ) );
            } );
        }

        @Test
        void countMatchMisdirMatch5()
        {
            check( ( first, unrelated, second ) ->
            {
                assertEquals( 2, countTypes( typeA, second.getRelationships( INCOMING, typeA ) ) );
                assertEquals( 0, countTypes( typeA, second.getRelationships( OUTGOING, typeA ) ) );
                assertEquals( 2, countTypes( typeA, second.getRelationships( INCOMING, typeA ) ) );
            } );
        }

        @Test
        void countNotMisdir1()
        {
            check( ( first, unrelated, second ) ->
            {
                assertEquals( 0, countTypes( typeA, unrelated.getRelationships( BOTH, typeA ) ) );
                assertEquals( 0, countTypes( typeA, second.getRelationships( OUTGOING, typeA ) ) );
            } );
        }

        @Test
        void countNotMisdir2()
        {
            check( ( first, unrelated, second ) ->
            {
                assertEquals( 0, countTypes( typeA, unrelated.getRelationships( BOTH, typeA ) ) );
                assertEquals( 0, countTypes( typeA, first.getRelationships( INCOMING, typeA ) ) );
            } );
        }

        @Test
        void countNotMatchNot1()
        {
            check( ( first, unrelated, second ) ->
            {
                assertEquals( 0, countTypes( typeA, unrelated.getRelationships( typeA ) ) );
                assertEquals( 2, countTypes( typeA, first.getRelationships( typeA ) ) );
                assertEquals( 0, countTypes( typeA, unrelated.getRelationships( typeA ) ) );
            } );
        }

        @Test
        void countNotMatchNot2()
        {
            check( ( first, unrelated, second ) ->
            {
                assertEquals( 0, countTypes( typeA, unrelated.getRelationships( typeA ) ) );
                assertEquals( 2, countTypes( typeA, second.getRelationships( typeA ) ) );
                assertEquals( 0, countTypes( typeA, unrelated.getRelationships( typeA ) ) );
            } );
        }

        private void check( Check check )
        {
            try ( Transaction tx = db.beginTx() )
            {
                Node first = tx.getNodeById( matchingFirst );
                Node unrelated = tx.getNodeById( notMatching );
                Node second = tx.getNodeById( matchingSecond );
                check.check( first, unrelated, second );
            }
        }

        private int countTypes( RelationshipType type, Iterable<Relationship> iterable )
        {
            int count = 0;
            for ( Relationship relationship : iterable )
            {
                assertEquals( type, relationship.getType() );
                count++;
            }
            return count;
        }
    }

    @Nested
    class WithNoDenseNodes extends RelationshipTraversalCursorReuseMustNotFalselyMatchRelationships
    {
    }

    @Nested
    class WithDenseFirstNode extends RelationshipTraversalCursorReuseMustNotFalselyMatchRelationships
    {
        @BeforeEach
        @Override
        void setUp()
        {
            super.setUp();
            try ( Transaction tx = db.beginTx() )
            {
                Node first = tx.getNodeById( matchingFirst );
                for ( int i = 0; i < DENSE_NODE_THRESHOLD; i++ )
                {
                    first.createRelationshipTo( first, typeX );
                }
                tx.commit();
            }
        }
    }

    @Nested
    class WithDenseSecondNode extends RelationshipTraversalCursorReuseMustNotFalselyMatchRelationships
    {
        @BeforeEach
        @Override
        void setUp()
        {
            super.setUp();
            try ( Transaction tx = db.beginTx() )
            {
                Node second = tx.getNodeById( matchingSecond );
                for ( int i = 0; i < DENSE_NODE_THRESHOLD; i++ )
                {
                    second.createRelationshipTo( second, typeX );
                }
                tx.commit();
            }
        }
    }

    @Nested
    class WithDenseUnrelatedNode extends RelationshipTraversalCursorReuseMustNotFalselyMatchRelationships
    {
        @BeforeEach
        @Override
        void setUp()
        {
            super.setUp();
            try ( Transaction tx = db.beginTx() )
            {
                Node unrelated = tx.getNodeById( notMatching );
                for ( int i = 0; i < DENSE_NODE_THRESHOLD; i++ )
                {
                    unrelated.createRelationshipTo( unrelated, typeX );
                }
                tx.commit();
            }
        }
    }

    private interface Check
    {
        void check( Node first, Node unrelated, Node second );
    }
}
