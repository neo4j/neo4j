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

import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.fail;

public class ManagedTestCursors implements CursorFactory
{
    private List<Cursor> allCursors = new ArrayList<>();

    private CursorFactory cursors;

    ManagedTestCursors( CursorFactory c )
    {
        this.cursors = c;
    }

    void assertAllClosedAndReset()
    {
        for ( Cursor n : allCursors )
        {
            if ( !n.isClosed() )
            {
                fail( "The Cursor " + n.toString() + " was not closed properly." );
            }
        }

        allCursors.clear();
    }

    @Override
    public NodeCursor allocateNodeCursor()
    {
        NodeCursor n = cursors.allocateNodeCursor();
        allCursors.add( n );
        return n;
    }

    @Override
    public RelationshipScanCursor allocateRelationshipScanCursor()
    {
        RelationshipScanCursor n = cursors.allocateRelationshipScanCursor();
        allCursors.add( n );
        return n;
    }

    @Override
    public RelationshipTraversalCursor allocateRelationshipTraversalCursor()
    {
        RelationshipTraversalCursor n = cursors.allocateRelationshipTraversalCursor();
        allCursors.add( n );
        return n;
    }

    @Override
    public PropertyCursor allocatePropertyCursor()
    {
        PropertyCursor n = cursors.allocatePropertyCursor();
        allCursors.add( n );
        return n;
    }

    @Override
    public RelationshipGroupCursor allocateRelationshipGroupCursor()
    {
        RelationshipGroupCursor n = cursors.allocateRelationshipGroupCursor();
        allCursors.add( n );
        return n;
    }

    @Override
    public NodeValueIndexCursor allocateNodeValueIndexCursor()
    {
        NodeValueIndexCursor n = cursors.allocateNodeValueIndexCursor();
        allCursors.add( n );
        return n;
    }

    @Override
    public NodeLabelIndexCursor allocateNodeLabelIndexCursor()
    {
        NodeLabelIndexCursor n = cursors.allocateNodeLabelIndexCursor();
        allCursors.add( n );
        return n;
    }

    @Override
    public NodeExplicitIndexCursor allocateNodeExplicitIndexCursor()
    {
        NodeExplicitIndexCursor n = cursors.allocateNodeExplicitIndexCursor();
        allCursors.add( n );
        return n;
    }

    @Override
    public RelationshipExplicitIndexCursor allocateRelationshipExplicitIndexCursor()
    {
        RelationshipExplicitIndexCursor n = cursors.allocateRelationshipExplicitIndexCursor();
        allCursors.add( n );
        return n;
    }
}
