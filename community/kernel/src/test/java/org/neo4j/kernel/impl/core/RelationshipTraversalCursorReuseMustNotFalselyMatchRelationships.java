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

import org.junit.Before;
import org.junit.Test;

import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.graphdb.Transaction;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.neo4j.graphdb.Direction.BOTH;
import static org.neo4j.graphdb.Direction.INCOMING;
import static org.neo4j.graphdb.Direction.OUTGOING;

public abstract class RelationshipTraversalCursorReuseMustNotFalselyMatchRelationships extends RelationshipsIterationTestSupport
{
    long matchingFirst;
    long notMatching;
    long matchingSecond;

    @Before
    @Override
    public void setUpEach()
    {
        super.setUpEach();
        try ( Transaction tx = db.beginTx() )
        {
            Node first = db.createNode();
            Node unrelated = db.createNode();
            Node second = db.createNode();
            matchingFirst = first.getId();
            notMatching = unrelated.getId();
            matchingSecond = second.getId();
            first.createRelationshipTo( second, typeA );
            first.createRelationshipTo( second, typeB );
            first.createRelationshipTo( unrelated, typeC );
            second.createRelationshipTo( unrelated, typeD );
            tx.success();
        }
        try ( Transaction tx = db.beginTx() )
        {
            Node first = db.getNodeById( matchingFirst );
            Node second = db.getNodeById( matchingSecond );
            first.createRelationshipTo( second, typeA );
            first.createRelationshipTo( second, typeB );
            tx.success();
        }
    }

    @Test
    public void matchNotMatch1()
    {
        check( ( first, unrelated, second ) ->
        {
            assertTrue( first.hasRelationship( typeA ) );
            assertFalse( unrelated.hasRelationship( typeA ) );
            assertTrue( second.hasRelationship( typeA ) );
        } );
    }

    @Test
    public void matchNotMatch2()
    {
        check( ( first, unrelated, second ) ->
        {
            assertTrue( first.hasRelationship( typeA ) );
            assertFalse( unrelated.hasRelationship( typeA ) );
            assertTrue( first.hasRelationship( typeA ) );
        } );
    }

    @Test
    public void matchNotMatch3()
    {
        check( ( first, unrelated, second ) ->
        {
            assertTrue( first.hasRelationship( typeA ) );
            assertFalse( first.hasRelationship( typeD ) );
            assertTrue( first.hasRelationship( typeA ) );
        } );
    }

    @Test
    public void matchNotMatch4()
    {
        check( ( first, unrelated, second ) ->
        {
            assertTrue( second.hasRelationship( typeA ) );
            assertFalse( unrelated.hasRelationship( typeA ) );
            assertTrue( first.hasRelationship( typeA ) );
        } );
    }

    @Test
    public void matchNotMisdir1()
    {
        check( ( first, unrelated, second ) ->
        {
            assertTrue( first.hasRelationship( typeA, OUTGOING ) );
            assertFalse( unrelated.hasRelationship( typeA, BOTH ) );
            assertFalse( second.hasRelationship( typeA, OUTGOING ) );
        } );
    }

    @Test
    public void matchNotMisdir2()
    {
        check( ( first, unrelated, second ) ->
        {
            assertTrue( first.hasRelationship( typeA, OUTGOING ) );
            assertFalse( unrelated.hasRelationship( typeA, BOTH ) );
            assertFalse( first.hasRelationship( typeA, INCOMING ) );
        } );
    }

    @Test
    public void misdirNotMatch1()
    {
        check( ( first, unrelated, second ) ->
        {
            assertFalse( first.hasRelationship( typeA, INCOMING ) );
            assertFalse( unrelated.hasRelationship( typeA, BOTH ) );
            assertTrue( second.hasRelationship( typeA, INCOMING ) );
        } );
    }

    @Test
    public void misdirNotMatch2()
    {
        check( ( first, unrelated, second ) ->
        {
            assertFalse( second.hasRelationship( typeA, OUTGOING ) );
            assertFalse( unrelated.hasRelationship( typeA, BOTH ) );
            assertTrue( first.hasRelationship( typeA, OUTGOING ) );
        } );
    }

    @Test
    public void notMatch1()
    {
        check( ( first, unrelated, second ) ->
        {
            assertFalse( unrelated.hasRelationship( typeA ) );
            assertTrue( second.hasRelationship( typeA ) );
        } );
    }

    @Test
    public void notMatch2()
    {
        check( ( first, unrelated, second ) ->
        {
            assertFalse( unrelated.hasRelationship( typeA ) );
            assertTrue( first.hasRelationship( typeA ) );
        } );
    }

    @Test
    public void misdirMatch1()
    {
        check( ( first, unrelated, second ) ->
        {
            assertFalse( first.hasRelationship( typeA, INCOMING ) );
            assertTrue( second.hasRelationship( typeA, INCOMING ) );
        } );
    }

    @Test
    public void misdirMatch2()
    {
        check( ( first, unrelated, second ) ->
        {
            assertFalse( second.hasRelationship( typeA, OUTGOING ) );
            assertTrue( first.hasRelationship( typeA, OUTGOING ) );
        } );
    }

    @Test
    public void matchMisdirMatch1()
    {
        check( ( first, unrelated, second ) ->
        {
            assertTrue( first.hasRelationship( typeA, OUTGOING ) );
            assertFalse( first.hasRelationship( typeA, INCOMING ) );
            assertTrue( second.hasRelationship( typeA, INCOMING ) );
        } );
    }

    @Test
    public void matchMisdirMatch2()
    {
        check( ( first, unrelated, second ) ->
        {
            assertTrue( second.hasRelationship( typeA, INCOMING ) );
            assertFalse( first.hasRelationship( typeA, INCOMING ) );
            assertTrue( second.hasRelationship( typeA, INCOMING ) );
        } );
    }

    @Test
    public void matchMisdirMatch3()
    {
        check( ( first, unrelated, second ) ->
        {
            assertTrue( second.hasRelationship( typeA, INCOMING ) );
            assertFalse( first.hasRelationship( typeA, INCOMING ) );
            assertTrue( first.hasRelationship( typeA, OUTGOING ) );
        } );
    }

    @Test
    public void matchMisdirMatch4()
    {
        check( ( first, unrelated, second ) ->
        {
            assertTrue( first.hasRelationship( typeA, OUTGOING ) );
            assertFalse( first.hasRelationship( typeA, INCOMING ) );
            assertTrue( first.hasRelationship( typeA, OUTGOING ) );
        } );
    }

    @Test
    public void matchMisdirMatch5()
    {
        check( ( first, unrelated, second ) ->
        {
            assertTrue( second.hasRelationship( typeA, INCOMING ) );
            assertFalse( second.hasRelationship( typeA, OUTGOING ) );
            assertTrue( second.hasRelationship( typeA, INCOMING ) );
        } );
    }

    @Test
    public void notMisdir1()
    {
        check( ( first, unrelated, second ) ->
        {
            assertFalse( unrelated.hasRelationship( typeA, BOTH ) );
            assertFalse( second.hasRelationship( typeA, OUTGOING ) );
        } );
    }

    @Test
    public void notMisdir2()
    {
        check( ( first, unrelated, second ) ->
        {
            assertFalse( unrelated.hasRelationship( typeA, BOTH ) );
            assertFalse( first.hasRelationship( typeA, INCOMING ) );
        } );
    }

    @Test
    public void notMatchNot1()
    {
        check( ( first, unrelated, second ) ->
        {
            assertFalse( unrelated.hasRelationship( typeA ) );
            assertTrue( first.hasRelationship( typeA ) );
            assertFalse( unrelated.hasRelationship( typeA ) );
        } );
    }

    @Test
    public void notMatchNot2()
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
    public void countMatchNotMatch1()
    {
        check( ( first, unrelated, second ) ->
        {
            assertEquals( 2, countTypes( typeA, first.getRelationships( typeA ) ) );
            assertEquals( 0, countTypes( typeA, unrelated.getRelationships( typeA ) ) );
            assertEquals( 2, countTypes( typeA, second.getRelationships( typeA ) ) );
        } );
    }

    @Test
    public void countMatchNotMatch2()
    {
        check( ( first, unrelated, second ) ->
        {
            assertEquals( 2, countTypes( typeA, first.getRelationships( typeA ) ) );
            assertEquals( 0, countTypes( typeA, unrelated.getRelationships( typeA ) ) );
            assertEquals( 2, countTypes( typeA, first.getRelationships( typeA ) ) );
        } );
    }

    @Test
    public void countMatchNotMatch3()
    {
        check( ( first, unrelated, second ) ->
        {
            assertEquals( 2, countTypes( typeA, first.getRelationships( typeA ) ) );
            assertEquals( 0, countTypes( typeD, first.getRelationships( typeD ) ) );
            assertEquals( 2, countTypes( typeA, first.getRelationships( typeA ) ) );
        } );
    }

    @Test
    public void countMatchNotMatch4()
    {
        check( ( first, unrelated, second ) ->
        {
            assertEquals( 2, countTypes( typeA, second.getRelationships( typeA ) ) );
            assertEquals( 0, countTypes( typeA, unrelated.getRelationships( typeA ) ) );
            assertEquals( 2, countTypes( typeA, first.getRelationships( typeA ) ) );
        } );
    }

    @Test
    public void countMatchNotMisdir1()
    {
        check( ( first, unrelated, second ) ->
        {
            assertEquals( 2, countTypes( typeA, first.getRelationships( typeA, OUTGOING ) ) );
            assertEquals( 0, countTypes( typeA, unrelated.getRelationships( typeA, BOTH ) ) );
            assertEquals( 0, countTypes( typeA, second.getRelationships( typeA, OUTGOING ) ) );
        } );
    }

    @Test
    public void countMatchNotMisdir2()
    {
        check( ( first, unrelated, second ) ->
        {
            assertEquals( 2, countTypes( typeA, first.getRelationships( typeA, OUTGOING ) ) );
            assertEquals( 0, countTypes( typeA, unrelated.getRelationships( typeA, BOTH ) ) );
            assertEquals( 0, countTypes( typeA, first.getRelationships( typeA, INCOMING ) ) );
        } );
    }

    @Test
    public void countMisdirNotMatch1()
    {
        check( ( first, unrelated, second ) ->
        {
            assertEquals( 0, countTypes( typeA, first.getRelationships( typeA, INCOMING ) ) );
            assertEquals( 0, countTypes( typeA, unrelated.getRelationships( typeA, BOTH ) ) );
            assertEquals( 2, countTypes( typeA, second.getRelationships( typeA, INCOMING ) ) );
        } );
    }

    @Test
    public void countMisdirNotMatch2()
    {
        check( ( first, unrelated, second ) ->
        {
            assertEquals( 0, countTypes( typeA, second.getRelationships( typeA, OUTGOING ) ) );
            assertEquals( 0, countTypes( typeA, unrelated.getRelationships( typeA, BOTH ) ) );
            assertEquals( 2, countTypes( typeA, first.getRelationships( typeA, OUTGOING ) ) );
        } );
    }

    @Test
    public void countNotMatch1()
    {
        check( ( first, unrelated, second ) ->
        {
            assertEquals( 0, countTypes( typeA, unrelated.getRelationships( typeA ) ) );
            assertEquals( 2, countTypes( typeA, second.getRelationships( typeA ) ) );
        } );
    }

    @Test
    public void countNotMatch2()
    {
        check( ( first, unrelated, second ) ->
        {
            assertEquals( 0, countTypes( typeA, unrelated.getRelationships( typeA ) ) );
            assertEquals( 2, countTypes( typeA, first.getRelationships( typeA ) ) );
        } );
    }

    @Test
    public void countMisdirMatch1()
    {
        check( ( first, unrelated, second ) ->
        {
            assertEquals( 0, countTypes( typeA, first.getRelationships( typeA, INCOMING ) ) );
            assertEquals( 2, countTypes( typeA, second.getRelationships( typeA, INCOMING ) ) );
        } );
    }

    @Test
    public void countMisdirMatch2()
    {
        check( ( first, unrelated, second ) ->
        {
            assertEquals( 0, countTypes( typeA, second.getRelationships( typeA, OUTGOING ) ) );
            assertEquals( 2, countTypes( typeA, first.getRelationships( typeA, OUTGOING ) ) );
        } );
    }

    @Test
    public void countMatchMisdirMatch1()
    {
        check( ( first, unrelated, second ) ->
        {
            assertEquals( 2, countTypes( typeA, first.getRelationships( typeA, OUTGOING ) ) );
            assertEquals( 0, countTypes( typeA, first.getRelationships( typeA, INCOMING ) ) );
            assertEquals( 2, countTypes( typeA, second.getRelationships( typeA, INCOMING ) ) );
        } );
    }

    @Test
    public void countMatchMisdirMatch2()
    {
        check( ( first, unrelated, second ) ->
        {
            assertEquals( 2, countTypes( typeA, second.getRelationships( typeA, INCOMING ) ) );
            assertEquals( 0, countTypes( typeA, first.getRelationships( typeA, INCOMING ) ) );
            assertEquals( 2, countTypes( typeA, second.getRelationships( typeA, INCOMING ) ) );
        } );
    }

    @Test
    public void countMatchMisdirMatch3()
    {
        check( ( first, unrelated, second ) ->
        {
            assertEquals( 2, countTypes( typeA, second.getRelationships( typeA, INCOMING ) ) );
            assertEquals( 0, countTypes( typeA, first.getRelationships( typeA, INCOMING ) ) );
            assertEquals( 2, countTypes( typeA, first.getRelationships( typeA, OUTGOING ) ) );
        } );
    }

    @Test
    public void countMatchMisdirMatch4()
    {
        check( ( first, unrelated, second ) ->
        {
            assertEquals( 2, countTypes( typeA, first.getRelationships( typeA, OUTGOING ) ) );
            assertEquals( 0, countTypes( typeA, first.getRelationships( typeA, INCOMING ) ) );
            assertEquals( 2, countTypes( typeA, first.getRelationships( typeA, OUTGOING ) ) );
        } );
    }

    @Test
    public void countMatchMisdirMatch5()
    {
        check( ( first, unrelated, second ) ->
        {
            assertEquals( 2, countTypes( typeA, second.getRelationships( typeA, INCOMING ) ) );
            assertEquals( 0, countTypes( typeA, second.getRelationships( typeA, OUTGOING ) ) );
            assertEquals( 2, countTypes( typeA, second.getRelationships( typeA, INCOMING ) ) );
        } );
    }

    @Test
    public void countNotMisdir1()
    {
        check( ( first, unrelated, second ) ->
        {
            assertEquals( 0, countTypes( typeA, unrelated.getRelationships( typeA, BOTH ) ) );
            assertEquals( 0, countTypes( typeA, second.getRelationships( typeA, OUTGOING ) ) );
        } );
    }

    @Test
    public void countNotMisdir2()
    {
        check( ( first, unrelated, second ) ->
        {
            assertEquals( 0, countTypes( typeA, unrelated.getRelationships( typeA, BOTH ) ) );
            assertEquals( 0, countTypes( typeA, first.getRelationships( typeA, INCOMING ) ) );
        } );
    }

    @Test
    public void countNotMatchNot1()
    {
        check( ( first, unrelated, second ) ->
        {
            assertEquals( 0, countTypes( typeA, unrelated.getRelationships( typeA ) ) );
            assertEquals( 2, countTypes( typeA, first.getRelationships( typeA ) ) );
            assertEquals( 0, countTypes( typeA, unrelated.getRelationships( typeA ) ) );
        } );
    }

    @Test
    public void countNotMatchNot2()
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
            Node first = db.getNodeById( matchingFirst );
            Node unrelated = db.getNodeById( notMatching );
            Node second = db.getNodeById( matchingSecond );
            check.check( first, unrelated, second );
        }
    }

    private int countTypes( RelationshipType type, Iterable<Relationship> iterable )
    {
        int count = 0;
        for ( Relationship relationship : iterable )
        {
            assertEquals( type.name(), relationship.getType().name() );
            count++;
        }
        return count;
    }
}
