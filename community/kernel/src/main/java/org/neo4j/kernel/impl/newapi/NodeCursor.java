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

import org.neo4j.collection.primitive.Primitive;
import org.neo4j.collection.primitive.PrimitiveIntSet;
import org.neo4j.internal.kernel.api.LabelSet;
import org.neo4j.internal.kernel.api.PropertyCursor;
import org.neo4j.internal.kernel.api.RelationshipGroupCursor;
import org.neo4j.internal.kernel.api.RelationshipTraversalCursor;
import org.neo4j.io.pagecache.PageCursor;
import org.neo4j.kernel.api.txstate.TransactionState;
import org.neo4j.kernel.impl.store.NodeLabelsField;
import org.neo4j.kernel.impl.store.RecordCursor;
import org.neo4j.kernel.impl.store.record.DynamicRecord;
import org.neo4j.kernel.impl.store.record.NodeRecord;
import org.neo4j.storageengine.api.txstate.NodeState;

import static org.neo4j.kernel.impl.newapi.References.setDirectFlag;
import static org.neo4j.kernel.impl.newapi.References.setGroupFlag;

class NodeCursor extends NodeRecord implements org.neo4j.internal.kernel.api.NodeCursor
{
    private Read read;
    private RecordCursor<DynamicRecord> labelCursor;
    private PageCursor pageCursor;
    private long next;
    private long highMark;

    NodeCursor()
    {
        super( NO_ID );
    }

    void scan( Read read )
    {
        if ( getId() != NO_ID )
        {
            reset();
        }
        if ( pageCursor == null )
        {
            pageCursor = read.nodePage( 0 );
        }
        this.next = 0;
        this.highMark = read.nodeHighMark();
        this.read = read;
        this.labelCursor = read.labelCursor();
    }

    void single( long reference, Read read )
    {
        if ( getId() != NO_ID )
        {
            reset();
        }
        if ( pageCursor == null )
        {
            pageCursor = read.nodePage( reference );
        }
        this.next = reference;
        this.highMark = NO_ID;
        this.read = read;
        this.labelCursor = read.labelCursor();
    }

    @Override
    public long nodeReference()
    {
        return getId();
    }

    @Override
    public LabelSet labels()
    {
        if ( read.hasTxStateWithChanges() )
        {
            TransactionState txState = read.txState();
            if (txState.nodeIsAddedInThisTx( nodeReference() ) )
            {
                //Node just added, no reason to go down to store and check
                return Labels.from( txState.nodeStateLabelDiffSets( nodeReference() ).getAdded() );
            }
            else
            {
                //Get labels from store and put in intSet, unfortunately we get longs back
                long[] longs = NodeLabelsField.get( this, labelCursor );
                PrimitiveIntSet labels = Primitive.intSet();
                for ( long labelToken : longs )
                {
                    labels.add( (int) labelToken );
                }

                //Augment what was found in store with what we have in tx state
                return Labels.from( txState.augmentLabels( labels, txState.getNodeState( nodeReference() ) ) );
            }
        }
        else
        {
            //Nothing in tx state, just read the data.
            return new Labels( NodeLabelsField.get( this, labelCursor ) );
        }
    }

    @Override
    public boolean hasProperties()
    {
        return nextProp != (long) NO_ID;
    }

    @Override
    public void relationships( RelationshipGroupCursor cursor )
    {
        read.relationshipGroups( nodeReference(), relationshipGroupReference(), cursor );
    }

    @Override
    public void allRelationships( RelationshipTraversalCursor cursor )
    {
        read.relationships( nodeReference(), allRelationshipsReference(), cursor );
    }

    @Override
    public void properties( PropertyCursor cursor )
    {
        read.nodeProperties( propertiesReference(), cursor );
    }

    @Override
    public long relationshipGroupReference()
    {
        return isDense() ? getNextRel() : setDirectFlag( getNextRel() );
    }

    @Override
    public long allRelationshipsReference()
    {
        return isDense() ? setGroupFlag( getNextRel() ) : getNextRel();
    }

    @Override
    public long propertiesReference()
    {
        //In the case where there hasn't been any changes in the transaction state this method simply returns the
        // property reference.
        //
        //However if there has been changes we can have two cases:
        //
        //   i: The node had no prior properties, in this case we simply encode the node id in the reference
        //      which allows the property cursor to probe tx state for properties.
        //   ii: The node has properties, in this case we mark the actual property reference as having tx state
        //
        //in both cases we need to store a mapping from the computed reference to the state in the transaction state
        //so we can retrieve it later in the property cursor.
        long propertiesReference = getNextProp();

        if ( read.hasTxStateWithChanges() )
        {
            TransactionState txState = read.txState();
            NodeState nodeState = txState.getNodeState( nodeReference() );
            if ( nodeState.hasPropertyChanges() )
            {
                long ref;
                if ( propertiesReference == NO_ID )
                {
                    //Current node has no properties before the start of this transaction,
                    //store the node id in the reference.
                    ref = References.setNodeFlag( nodeReference() );
                }
                else
                {
                    //Mark the reference so that property cursor checks both
                    //tx state as well as disk.
                    ref = References.setTxStateFlag( propertiesReference );
                }
                //stores the node state mapped to the current property
                //reference so that property cursor is able to retrieve the state later.
                txState.registerProperties( ref, nodeState );
                return ref;
            }
        }
        return propertiesReference;
    }

    @Override
    public boolean next()
    {
        if ( next == NO_ID )
        {
            reset();
            return false;
        }
        // Check tx state

        boolean hasChanges = read.hasTxStateWithChanges();
        TransactionState txs = hasChanges ? read.txState() : null;
        do
        {
            if ( hasChanges && txs.nodeIsAddedInThisTx( next ) )
            {
                setId( next++ );
                setInUse( true );
            }
            else if ( hasChanges && txs.nodeIsDeletedInThisTx( next ) )
            {
                next++;
                setInUse( false );
            }
            else
            {
                read.node( this, next++, pageCursor );
            }
            if ( next > highMark )
            {
                if ( highMark == NO_ID )
                {
                    //we are using single
                    next = NO_ID;
                    return inUse();
                }
                else
                {
                    //Check if there is a new high mark
                    highMark = read.nodeHighMark();
                    if ( next > highMark )
                    {
                        next = NO_ID;
                        return inUse();
                    }
                }
            }
        }
        while ( !inUse() );
        return true;
    }

    @Override
    public boolean shouldRetry()
    {
        return false;
    }

    @Override
    public void close()
    {
        if ( pageCursor != null )
        {
            pageCursor.close();
            pageCursor = null;
        }
        reset();
    }

    @Override
    public boolean isClosed()
    {
        return pageCursor == null;
    }

    private void reset()
    {
        next = NO_ID;
        setId( NO_ID );
    }
}
