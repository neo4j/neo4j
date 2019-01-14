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
package org.neo4j.internal.kernel.api;

import org.neo4j.collection.primitive.PrimitiveLongSet;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class IndexReadAsserts
{
    static void assertNodes( NodeIndexCursor node, PrimitiveLongSet uniqueIds, long... expected )
    {
        uniqueIds.clear();
        for ( long count : expected )
        {
            assertTrue( "at least " + expected.length + " nodes", node.next() );
            assertTrue( uniqueIds.add( node.nodeReference() ) );
        }
        assertFalse( "no more than " + expected.length + " nodes", node.next() );
        assertEquals( "all nodes are unique", expected.length, uniqueIds.size() );
        for ( long expectedNode : expected )
        {
            assertTrue( "expected node " + expectedNode, uniqueIds.contains( expectedNode ) );
        }
    }

    static void assertNodeCount( NodeIndexCursor node, int expectedCount, PrimitiveLongSet uniqueIds )
    {
        uniqueIds.clear();
        for ( int i = 0; i < expectedCount; i++ )
        {
            assertTrue( "at least " + expectedCount + " nodes", node.next() );
            assertTrue( uniqueIds.add( node.nodeReference() ) );
        }
        assertFalse( "no more than " + expectedCount + " nodes", node.next() );
    }

    static void assertFoundRelationships( RelationshipIndexCursor edge, int edges, PrimitiveLongSet uniqueIds )
    {
        for ( int i = 0; i < edges; i++ )
        {
            assertTrue( "at least " + edges + " relationships", edge.next() );
            assertTrue( uniqueIds.add( edge.relationshipReference() ) );
        }
        assertFalse( "no more than " + edges + " relationships", edge.next() );
    }
}
