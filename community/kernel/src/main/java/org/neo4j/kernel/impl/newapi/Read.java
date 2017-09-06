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
package org.neo4j.kernel.impl.newapi;

import org.neo4j.internal.kernel.api.IndexPredicate;
import org.neo4j.internal.kernel.api.IndexReference;
import org.neo4j.internal.kernel.api.Scan;
import org.neo4j.kernel.impl.store.NeoStores;
import org.neo4j.kernel.impl.store.RecordCursor;
import org.neo4j.kernel.impl.store.RecordStore;
import org.neo4j.kernel.impl.store.record.AbstractBaseRecord;
import org.neo4j.kernel.impl.store.record.DynamicRecord;
import org.neo4j.kernel.impl.store.record.NodeRecord;
import org.neo4j.kernel.impl.store.record.RelationshipRecord;

import static org.neo4j.kernel.impl.store.record.RecordLoad.NORMAL;

public class Read implements org.neo4j.internal.kernel.api.Read
{
    private final NeoStores stores;

    public Read( NeoStores stores )
    {
        this.stores = stores;
    }

    @Override
    public void nodeIndexSeek(
            IndexReference index,
            org.neo4j.internal.kernel.api.NodeValueIndexCursor cursor,
            IndexPredicate... predicates )
    {
        throw new UnsupportedOperationException( "not implemented" );
    }

    @Override
    public void nodeIndexScan( IndexReference index, org.neo4j.internal.kernel.api.NodeValueIndexCursor cursor )
    {
        throw new UnsupportedOperationException( "not implemented" );
    }

    @Override
    public void nodeLabelScan( int label, org.neo4j.internal.kernel.api.NodeLabelIndexCursor cursor )
    {
        throw new UnsupportedOperationException( "not implemented" );
    }

    @Override
    public Scan<org.neo4j.internal.kernel.api.NodeLabelIndexCursor> nodeLabelScan( int label )
    {
        throw new UnsupportedOperationException( "not implemented" );
    }

    @Override
    public void allNodesScan( org.neo4j.internal.kernel.api.NodeCursor cursor )
    {
        ((NodeCursor) cursor).scan();
    }

    @Override
    public Scan<org.neo4j.internal.kernel.api.NodeCursor> allNodesScan()
    {
        throw new UnsupportedOperationException( "not implemented" );
    }

    @Override
    public void singleNode( long reference, org.neo4j.internal.kernel.api.NodeCursor cursor )
    {
        ((NodeCursor) cursor).single( reference );
    }

    @Override
    public void singleRelationship( long reference, org.neo4j.internal.kernel.api.RelationshipScanCursor cursor )
    {
        ((RelationshipScanCursor) cursor).single( reference );
    }

    @Override
    public void allRelationshipsScan( org.neo4j.internal.kernel.api.RelationshipScanCursor cursor )
    {
        ((RelationshipScanCursor) cursor).scan( -1/*include all labels*/ );
    }

    @Override
    public Scan<org.neo4j.internal.kernel.api.RelationshipScanCursor> allRelationshipsScan()
    {
        throw new UnsupportedOperationException( "not implemented" );
    }

    @Override
    public void relationshipLabelScan( int label, org.neo4j.internal.kernel.api.RelationshipScanCursor cursor )
    {
        ((RelationshipScanCursor) cursor).scan( label );
    }

    @Override
    public Scan<org.neo4j.internal.kernel.api.RelationshipScanCursor> relationshipLabelScan( int label )
    {
        throw new UnsupportedOperationException( "not implemented" );
    }

    @Override
    public void relationshipGroups(
            long nodeReference, long reference, org.neo4j.internal.kernel.api.RelationshipGroupCursor cursor )
    {
        ((RelationshipGroupCursor) cursor).init( stores, nodeReference, reference );
    }

    @Override
    public void relationships(
            long nodeReference, long reference, org.neo4j.internal.kernel.api.RelationshipTraversalCursor cursor )
    {
        ((RelationshipTraversalCursor) cursor).init( nodeReference, reference );
    }

    @Override
    public void nodeProperties( long reference, org.neo4j.internal.kernel.api.PropertyCursor cursor )
    {
        ((PropertyCursor) cursor).init( stores, reference );
    }

    @Override
    public void relationshipProperties( long reference, org.neo4j.internal.kernel.api.PropertyCursor cursor )
    {
        ((PropertyCursor) cursor).init( stores, reference );
    }

    @Override
    public void futureNodeReferenceRead( long reference )
    {
    }

    @Override
    public void futureRelationshipsReferenceRead( long reference )
    {
    }

    @Override
    public void futureNodePropertyReferenceRead( long reference )
    {
    }

    @Override
    public void futureRelationshipPropertyReferenceRead( long reference )
    {
    }

    RecordCursor<DynamicRecord> labelCursor()
    {
        return newCursor( stores.getNodeStore().getDynamicLabelStore() );
    }

    private static <R extends AbstractBaseRecord> RecordCursor<R> newCursor( RecordStore<R> store )
    {
        return store.newRecordCursor( store.newRecord() ).acquire( store.getNumberOfReservedLowIds(), NORMAL );
    }

    void node( NodeRecord record, long reference )
    {
        throw new UnsupportedOperationException( "not implemented" );
    }

    void relationship( RelationshipRecord record, long reference )
    {
        throw new UnsupportedOperationException( "not implemented" );
    }

    long nodeHighMark()
    {
        throw new UnsupportedOperationException( "not implemented" );
    }

    long relationshipHighMark()
    {
        throw new UnsupportedOperationException( "not implemented" );
    }
}
