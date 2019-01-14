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

import org.neo4j.internal.kernel.api.IndexOrder;
import org.neo4j.internal.kernel.api.IndexQuery;
import org.neo4j.internal.kernel.api.IndexReference;
import org.neo4j.internal.kernel.api.NodeCursor;
import org.neo4j.internal.kernel.api.NodeLabelIndexCursor;
import org.neo4j.internal.kernel.api.NodeValueIndexCursor;
import org.neo4j.internal.kernel.api.PropertyCursor;
import org.neo4j.internal.kernel.api.Read;
import org.neo4j.internal.kernel.api.RelationshipGroupCursor;
import org.neo4j.internal.kernel.api.RelationshipScanCursor;
import org.neo4j.internal.kernel.api.RelationshipTraversalCursor;
import org.neo4j.internal.kernel.api.Scan;
import org.neo4j.internal.kernel.api.exceptions.KernelException;

public class StubRead implements Read
{
    @Override
    public void nodeIndexSeek( IndexReference index, NodeValueIndexCursor cursor, IndexOrder indexOrder,
            IndexQuery... query )
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public void nodeIndexDistinctValues( IndexReference index, NodeValueIndexCursor cursor )
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public long lockingNodeUniqueIndexSeek( IndexReference index,
            IndexQuery.ExactPredicate... predicates ) throws KernelException
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public void nodeIndexScan( IndexReference index, NodeValueIndexCursor cursor, IndexOrder indexOrder )
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public void nodeLabelScan( int label, NodeLabelIndexCursor cursor )
    {
        ((StubNodeLabelIndexCursor) cursor).initalize( label );
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
    public long countsForNode( int labelId )
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public long countsForNodeWithoutTxState( int labelId )
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public long countsForRelationship( int startLabelId, int typeId, int endLabelId )
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public long countsForRelationshipWithoutTxState( int startLabelId, int typeId, int endLabelId )
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public long nodesGetCount()
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public long relationshipsGetCount()
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public void singleRelationship( long reference, RelationshipScanCursor cursor )
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean relationshipExists( long reference )
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
    public void relationshipTypeScan( int type, RelationshipScanCursor cursor )
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public Scan<RelationshipScanCursor> relationshipTypeScan( int type )
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
    public void nodeProperties( long nodeReference, long reference, PropertyCursor cursor )
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public void relationshipProperties( long relationshipReference, long reference, PropertyCursor cursor )
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
