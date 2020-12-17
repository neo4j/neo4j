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
package org.neo4j.internal.kernel.api.helpers;

import org.neo4j.internal.kernel.api.IndexQuery;
import org.neo4j.internal.kernel.api.IndexQueryConstraints;
import org.neo4j.internal.kernel.api.IndexReadSession;
import org.neo4j.internal.kernel.api.NodeCursor;
import org.neo4j.internal.kernel.api.NodeLabelIndexCursor;
import org.neo4j.internal.kernel.api.NodeValueIndexCursor;
import org.neo4j.internal.kernel.api.PropertyCursor;
import org.neo4j.internal.kernel.api.Read;
import org.neo4j.internal.kernel.api.RelationshipScanCursor;
import org.neo4j.internal.kernel.api.RelationshipTraversalCursor;
import org.neo4j.internal.kernel.api.RelationshipTypeIndexCursor;
import org.neo4j.internal.kernel.api.RelationshipValueIndexCursor;
import org.neo4j.internal.kernel.api.Scan;
import org.neo4j.internal.schema.IndexDescriptor;
import org.neo4j.internal.schema.IndexOrder;
import org.neo4j.storageengine.api.RelationshipSelection;
import org.neo4j.values.storable.Value;

public class StubRead implements Read
{
    @Override
    public IndexReadSession indexReadSession( IndexDescriptor index )
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public void prepareForLabelScans()
    {
    }

    @Override
    public void nodeIndexSeek( IndexReadSession index, NodeValueIndexCursor cursor, IndexQueryConstraints constraints, IndexQuery... query )
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public void relationshipIndexSeek( IndexReadSession index, RelationshipValueIndexCursor cursor, IndexQueryConstraints constraints, IndexQuery... query )
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public long lockingNodeUniqueIndexSeek( IndexDescriptor index,
                                            NodeValueIndexCursor cursor,
                                            IndexQuery.ExactPredicate... predicates )
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public void nodeIndexScan( IndexReadSession index, NodeValueIndexCursor cursor, IndexQueryConstraints constraints )
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public void relationshipIndexScan( IndexReadSession index, RelationshipValueIndexCursor cursor, IndexQueryConstraints constraints )
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public void nodeLabelScan( int label, NodeLabelIndexCursor cursor, IndexOrder order )
    {
        ((StubNodeLabelIndexCursor) cursor).initialize( label );
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
    public void relationshipTypeScan( int type, RelationshipTypeIndexCursor relationshipTypeIndexCursor, IndexOrder order )
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public void relationships( long nodeReference, long reference, RelationshipSelection selection, RelationshipTraversalCursor cursor )
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public void nodeProperties( long nodeReference, long reference, PropertyCursor cursor )
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public void relationshipProperties( long nodeReference, long reference, PropertyCursor cursor )
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean nodeDeletedInTransaction( long node )
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean relationshipDeletedInTransaction( long relationship )
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public Value nodePropertyChangeInTransactionOrNull( long node, int propertyKeyId )
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public Value relationshipPropertyChangeInTransactionOrNull( long relationship, int propertyKeyId )
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean transactionStateHasChanges()
    {
        throw new UnsupportedOperationException();
    }
}
