/*
 * Copyright (c) 2002-2017 "Neo Technology,"
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

import org.neo4j.internal.kernel.api.exceptions.KernelException;

public class StubRead implements Read
{
    @Override
    public void nodeIndexSeek( IndexReference index, NodeValueIndexCursor cursor, IndexOrder indexOrder,
            IndexQuery... query ) throws KernelException
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public void nodeIndexScan( IndexReference index, NodeValueIndexCursor cursor, IndexOrder indexOrder )
            throws KernelException
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public void nodeLabelScan( int label, NodeLabelIndexCursor cursor )
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public void nodeLabelUnionScan( NodeLabelIndexCursor cursor, int... labels )
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public void nodeLabelIntersectionScan( NodeLabelIndexCursor cursor, int... labels )
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public Scan<NodeLabelIndexCursor> nodeLabelScan( int label )
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public void allNodesScan( NodeCursor cursor )
    {
        ((StubNodeCursor) cursor).scan();
    }

    @Override
    public Scan<NodeCursor> allNodesScan()
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public void singleNode( long reference, NodeCursor cursor )
    {
        ((StubNodeCursor) cursor).single( reference );
    }

    @Override
    public boolean nodeExists( long id )
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public void singleRelationship( long reference, RelationshipScanCursor cursor )
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public void allRelationshipsScan( RelationshipScanCursor cursor )
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public Scan<RelationshipScanCursor> allRelationshipsScan()
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public void relationshipLabelScan( int label, RelationshipScanCursor cursor )
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public Scan<RelationshipScanCursor> relationshipLabelScan( int label )
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public void relationshipGroups( long nodeReference, long reference, RelationshipGroupCursor cursor )
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public void relationships( long nodeReference, long reference, RelationshipTraversalCursor cursor )
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public void nodeProperties( long reference, PropertyCursor cursor )
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public void relationshipProperties( long reference, PropertyCursor cursor )
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public void graphProperties( PropertyCursor cursor )
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public void futureNodeReferenceRead( long reference )
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public void futureRelationshipsReferenceRead( long reference )
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public void futureNodePropertyReferenceRead( long reference )
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public void futureRelationshipPropertyReferenceRead( long reference )
    {
        throw new UnsupportedOperationException();
    }
}
