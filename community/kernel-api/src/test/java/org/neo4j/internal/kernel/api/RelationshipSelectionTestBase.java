/*
 * Copyright (c) 2002-2018 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
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

import static junit.framework.TestCase.assertEquals;
import static junit.framework.TestCase.assertTrue;
import static org.junit.Assert.assertFalse;

class RelationshipSelectionTestBase
{

    final int typeA = 100;
    final int typeB = 101;
    final int typeC = 102;

    void assertOutgoing( RelationshipSelectionCursor cursor, int targetNode, int type )
    {
        assertTrue( "has next", cursor.next() );
        assertEquals( "expected type", type, cursor.label() );
        assertEquals( "expected target", targetNode, cursor.targetNodeReference() );
    }

    void assertIncoming( RelationshipSelectionCursor cursor, int sourceNode, int type )
    {
        assertTrue( "has next", cursor.next() );
        assertEquals( "expected type", type, cursor.label() );
        assertEquals( "expected source", sourceNode, cursor.sourceNodeReference() );
    }

    void assertLoop( RelationshipSelectionCursor cursor, int type )
    {
        assertTrue( "has next", cursor.next() );
        assertEquals( "expected type", type, cursor.label() );
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
}
