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
package org.neo4j.kernel.impl.newapi;

import org.eclipse.collections.api.iterator.LongIterator;
import org.eclipse.collections.api.set.primitive.LongSet;
import org.eclipse.collections.api.set.primitive.MutableLongSet;
import org.eclipse.collections.impl.factory.primitive.LongSets;
import org.eclipse.collections.impl.iterator.ImmutableEmptyLongIterator;
import org.eclipse.collections.impl.set.mutable.primitive.LongHashSet;

import java.util.Arrays;
import java.util.stream.LongStream;

import org.neo4j.internal.kernel.api.LabelSet;
import org.neo4j.internal.kernel.api.NodeCursor;
import org.neo4j.internal.kernel.api.PropertyCursor;
import org.neo4j.internal.kernel.api.RelationshipGroupCursor;
import org.neo4j.internal.kernel.api.RelationshipTraversalCursor;
import org.neo4j.internal.kernel.api.security.AccessMode;
import org.neo4j.kernel.api.txstate.TransactionState;
import org.neo4j.storageengine.api.AllNodeScan;
import org.neo4j.storageengine.api.StorageNodeCursor;
import org.neo4j.storageengine.api.txstate.LongDiffSets;

import static org.neo4j.kernel.impl.newapi.Read.NO_ID;
import static org.neo4j.kernel.impl.newapi.RelationshipReferenceEncoding.encodeDense;

class DefaultNodeCursor implements NodeCursor
{
    Read read;
    HasChanges hasChanges = HasChanges.MAYBE;
    private LongIterator addedNodes;
    StorageNodeCursor storeCursor;
    long currentAddedInTx;
    private long single;
    private AccessMode accessMode;

    private final CursorPool<DefaultNodeCursor> pool;

    DefaultNodeCursor( CursorPool<DefaultNodeCursor> pool, StorageNodeCursor storeCursor )
    {
        this.pool = pool;
        this.storeCursor = storeCursor;
    }

    void scan( Read read )
    {
        storeCursor.scan();
        this.read = read;
        this.single = NO_ID;
        this.currentAddedInTx = NO_ID;
        this.hasChanges = HasChanges.MAYBE;
        this.addedNodes = ImmutableEmptyLongIterator.INSTANCE;
        this.accessMode = read.ktx.securityContext().mode();
    }

    boolean scanBatch( Read read, AllNodeScan scan, int sizeHint, LongIterator addedNodes, boolean hasChanges )
    {
        this.read = read;
        this.single = NO_ID;
        this.currentAddedInTx = NO_ID;
        this.hasChanges = hasChanges ? HasChanges.YES : HasChanges.NO;
        this.addedNodes = addedNodes;
        this.accessMode = read.ktx.securityContext().mode();
        boolean scanBatch = storeCursor.scanBatch( scan, sizeHint );
        return addedNodes.hasNext() || scanBatch;
    }

    void single( long reference, Read read )
    {
        storeCursor.single( reference );
        this.read = read;
        this.single = reference;
        this.currentAddedInTx = NO_ID;
        this.hasChanges = HasChanges.MAYBE;
        this.addedNodes = ImmutableEmptyLongIterator.INSTANCE;
        this.accessMode = read.ktx.securityContext().mode();
    }

    @Override
    public long nodeReference()
    {
        if ( currentAddedInTx != NO_ID )
        {
            // Special case where the most recent next() call selected a node that exists only in tx-state.
            // Internal methods getting data about this node will also check tx-state and get the data from there.
            return currentAddedInTx;
        }
        return storeCursor.entityReference();
    }

    @Override
    public LabelSet labels()
    {
        if ( currentAddedInTx != NO_ID )
        {
            //Node added in tx-state, no reason to go down to store and check
            TransactionState txState = read.txState();
            // Select only allowed labels
            final LongSet labels = txState.nodeStateLabelDiffSets( currentAddedInTx ).getAdded();
            return Labels.from( labels.select( this::allowedLabels ) );
        }
        else if ( hasChanges() )
        {
            //Get labels from store and put in intSet, unfortunately we get longs back
            TransactionState txState = read.txState();
            long[] longs = storeCursor.labels();
            final MutableLongSet labels = new LongHashSet();
            for ( long labelToken : longs )
            {
                labels.add( labelToken );
            }

            //Augment what was found in store with what we have in tx state
            return Labels.from( txState.augmentLabels( labels, txState.getNodeState( storeCursor.entityReference() ) ).select(
                    this::allowedLabels ) );
        }
        else
        {
            //Nothing in tx state, just read the data.
            final long[] labels = storeCursor.labels();
            final LongStream allowedLabels = LongStream.of( labels ).filter( this::allowedLabels );
            return Labels.from( allowedLabels.toArray() );
        }
    }

    @Override
    public boolean hasLabel( int label )
    {
        if ( !allowedLabels( label ) )
        {
            return false;
        }

        if ( hasChanges() )
        {
            TransactionState txState = read.txState();
            LongDiffSets diffSets = txState.nodeStateLabelDiffSets( nodeReference() );
            if ( diffSets.getAdded().contains( label ) )
            {
                return true;
            }
            if ( diffSets.getRemoved().contains( label ) )
            {
                return false;
            }
        }

        //Get labels from store and put in intSet, unfortunately we get longs back
        return storeCursor.hasLabel( label );
    }

    boolean allowedLabels( long... labels )
    {
        return accessMode.allowsReadLabels( Arrays.stream( labels ).mapToInt( l -> (int) l ) );
    }

    @Override
    public void relationships( RelationshipGroupCursor cursor )
    {
        ((DefaultRelationshipGroupCursor) cursor).init( nodeReference(), relationshipGroupReferenceWithoutFlags(), isDense(), read );
    }

    @Override
    public void allRelationships( RelationshipTraversalCursor cursor )
    {
        ((DefaultRelationshipTraversalCursor) cursor).init( nodeReference(), allRelationshipsReferenceWithoutFlags(), isDense(), read );
    }

    @Override
    public void properties( PropertyCursor cursor )
    {
        ((DefaultPropertyCursor) cursor).initNode( nodeReference(), propertiesReference(), read, read );
    }

    @Override
    public long relationshipGroupReference()
    {
        long reference = relationshipGroupReferenceWithoutFlags();
        // Mark reference with special flags since this reference will leave some context behind when returned
        return isDense() ? encodeDense( reference ) : reference;
    }

    private long relationshipGroupReferenceWithoutFlags()
    {
        return currentAddedInTx != NO_ID ? NO_ID : storeCursor.relationshipGroupReference();
    }

    @Override
    public long allRelationshipsReference()
    {
        long reference = allRelationshipsReferenceWithoutFlags();
        // Mark reference with special flags since this reference will leave some context behind when returned
        return isDense() ? encodeDense( reference ) : reference;
    }

    private long allRelationshipsReferenceWithoutFlags()
    {
        return currentAddedInTx != NO_ID ? NO_ID : storeCursor.allRelationshipsReference();
    }

    @Override
    public long propertiesReference()
    {
        return currentAddedInTx != NO_ID ? NO_ID : storeCursor.propertiesReference();
    }

    @Override
    public boolean isDense()
    {
        return currentAddedInTx == NO_ID && storeCursor.isDense();
    }

    @Override
    public boolean next()
    {
        // Check tx state
        boolean hasChanges = hasChanges();

        if ( hasChanges )
        {
            if ( addedNodes.hasNext() )
            {
                currentAddedInTx = addedNodes.next();
                return true;
            }
            else
            {
                currentAddedInTx = NO_ID;
            }
        }

        while ( storeCursor.next() )
        {
            boolean skip = hasChanges && read.txState().nodeIsDeletedInThisTx( storeCursor.entityReference() );
            if ( !skip && allowsTraverse() )
            {
                return true;
            }
        }
        return false;
    }

    boolean allowsTraverse()
    {
        return accessMode.allowsTraverseAllLabels() ||
               accessMode.allowsTraverseLabels( Arrays.stream( storeCursor.labels() ).mapToInt( l -> (int) l ) );
    }

    @Override
    public void close()
    {
        if ( !isClosed() )
        {
            read = null;
            hasChanges = HasChanges.MAYBE;
            addedNodes = ImmutableEmptyLongIterator.INSTANCE;
            storeCursor.reset();
            accessMode = null;

            pool.accept( this );
        }
    }

    @Override
    public boolean isClosed()
    {
        return read == null;
    }

    /**
     * NodeCursor should only see changes that are there from the beginning
     * otherwise it will not be stable.
     */
    boolean hasChanges()
    {
        switch ( hasChanges )
        {
        case MAYBE:
            boolean changes = read.hasTxStateWithChanges();
            if ( changes )
            {
                if ( single != NO_ID )
                {
                    addedNodes = read.txState().nodeIsAddedInThisTx( single ) ?
                                 LongSets.immutable.of( single ).longIterator() : ImmutableEmptyLongIterator.INSTANCE;
                }
                else
                {
                    addedNodes = read.txState().addedAndRemovedNodes().getAdded().freeze().longIterator();
                }
                hasChanges = HasChanges.YES;
            }
            else
            {
                hasChanges = HasChanges.NO;
            }
            return changes;
        case YES:
            return true;
        case NO:
            return false;
        default:
            throw new IllegalStateException( "Style guide, why are you making me do this" );
        }
    }

    @Override
    public String toString()
    {
        if ( isClosed() )
        {
            return "NodeCursor[closed state]";
        }
        else
        {
            return "NodeCursor[id=" + nodeReference() + ", " + storeCursor.toString() + "]";
        }
    }

    void release()
    {
        storeCursor.close();
    }
}
