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
package org.neo4j.internal.kernel.api.helpers;

import org.neo4j.graphdb.ResourceIterator;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

class RelationshipSelectionTestBase
{

    final int typeA = 100;
    final int typeB = 101;
    final int typeC = 102;

    void assertOutgoing( ResourceIterator<R> iterator, int targetNode, int type )
    {
        assertTrue( "has next", iterator.hasNext() );
        R r = iterator.next();
        assertEquals( "expected type", type, r.type );
        assertEquals( "expected target", targetNode, r.targetNode );
    }

    void assertIncoming( ResourceIterator<R> iterator, int sourceNode, int type )
    {
        assertTrue( "has next", iterator.hasNext() );
        R r = iterator.next();
        assertEquals( "expected type", type, r.type );
        assertEquals( "expected source", sourceNode, r.sourceNode );
    }

    void assertLoop( ResourceIterator<R> iterator, int type )
    {
        assertTrue( "has next", iterator.hasNext() );
        R r = iterator.next();
        assertEquals( "expected type", type, r.type );
        assertEquals( "expected loop", r.sourceNode, r.targetNode );
    }

    void assertEmpty( ResourceIterator<R> iterator )
    {
        assertFalse( "no more", iterator.hasNext() );
    }

    void assertOutgoing( RelationshipSelectionCursor cursor, int targetNode, int type )
    {
        assertTrue( "has next", cursor.next() );
        assertEquals( "expected type", type, cursor.type() );
        assertEquals( "expected target", targetNode, cursor.targetNodeReference() );
    }

    void assertIncoming( RelationshipSelectionCursor cursor, int sourceNode, int type )
    {
        assertTrue( "has next", cursor.next() );
        assertEquals( "expected type", type, cursor.type() );
        assertEquals( "expected source", sourceNode, cursor.sourceNodeReference() );
    }

    void assertLoop( RelationshipSelectionCursor cursor, int type )
    {
        assertTrue( "has next", cursor.next() );
        assertEquals( "expected type", type, cursor.type() );
        assertEquals( "expected loop", cursor.sourceNodeReference(), cursor.targetNodeReference() );
    }

    void assertEmpty( RelationshipSelectionCursor cursor )
    {
        assertFalse( "no more", cursor.next() );
    }

    int[] types( int... types )
    {
        return types;
    }

    static class R
    {
        final long relationship;
        final long sourceNode;
        final int type;
        final long targetNode;

        R( long relationship, long sourceNode, int type, long targetNode )
        {
            this.relationship = relationship;
            this.sourceNode = sourceNode;
            this.type = type;
            this.targetNode = targetNode;
        }
    }
}
