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
package org.neo4j.kernel.impl.newapi;

import org.eclipse.collections.api.iterator.LongIterator;
import org.eclipse.collections.api.set.primitive.MutableLongSet;
import org.eclipse.collections.impl.factory.primitive.LongSets;
import org.eclipse.collections.impl.iterator.ImmutableEmptyLongIterator;
import org.eclipse.collections.impl.set.mutable.primitive.LongHashSet;

import org.neo4j.internal.kernel.api.LabelSet;
import org.neo4j.internal.kernel.api.NodeCursor;
import org.neo4j.internal.kernel.api.PropertyCursor;
import org.neo4j.internal.kernel.api.RelationshipGroupCursor;
import org.neo4j.internal.kernel.api.RelationshipTraversalCursor;
import org.neo4j.kernel.api.txstate.TransactionState;
import org.neo4j.storageengine.api.StorageNodeCursor;
import org.neo4j.storageengine.api.txstate.LongDiffSets;

import static org.neo4j.kernel.impl.store.record.AbstractBaseRecord.NO_ID;

class DefaultNodeCursor implements NodeCursor
{
    private Read read;
    private HasChanges hasChanges = HasChanges.MAYBE;
    private LongIterator addedNodes;
    private StorageNodeCursor storeCursor;
    private long single;

    private final DefaultCursors pool;

    DefaultNodeCursor( DefaultCursors pool, StorageNodeCursor storeCursor )
    {
        this.pool = pool;
        this.storeCursor = storeCursor;
    }

    void scan( Read read )
    {
        storeCursor.scan();
        this.read = read;
        this.single = NO_ID;
        this.hasChanges = HasChanges.MAYBE;
        this.addedNodes = ImmutableEmptyLongIterator.INSTANCE;
    }

    void single( long reference, Read read )
    {
        storeCursor.single( reference );
        this.read = read;
        this.single = reference;
        this.hasChanges = HasChanges.MAYBE;
        this.addedNodes = ImmutableEmptyLongIterator.INSTANCE;
    }

    @Override
    public long nodeReference()
    {
        return storeCursor.entityReference();
    }

    @Override
    public LabelSet labels()
    {
        if ( hasChanges() )
        {
            TransactionState txState = read.txState();
            if ( txState.nodeIsAddedInThisTx( storeCursor.entityReference() ) )
            {
                //Node just added, no reason to go down to store and check
                return Labels.from( txState.nodeStateLabelDiffSets( storeCursor.entityReference() ).getAdded() );
            }
            else
            {
                //Get labels from store and put in intSet, unfortunately we get longs back
                long[] longs = storeCursor.labels();
                final MutableLongSet labels = new LongHashSet();
                for ( long labelToken : longs )
                {
                    labels.add( labelToken );
                }

                //Augment what was found in store with what we have in tx state
                return Labels.from( txState.augmentLabels( labels, txState.getNodeState( storeCursor.entityReference() ) ) );
            }
        }
        else
        {
            //Nothing in tx state, just read the data.
            return Labels.from( storeCursor.labels() );
        }
    }

    @Override
    public boolean hasLabel( int label )
    {
        if ( hasChanges() )
        {
            TransactionState txState = read.txState();
            LongDiffSets diffSets = txState.nodeStateLabelDiffSets( storeCursor.entityReference() );
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

    @Override
    public void relationships( RelationshipGroupCursor cursor )
    {
        ((DefaultRelationshipGroupCursor) cursor).init( nodeReference(), relationshipGroupReference(), read );
    }

    @Override
    public void allRelationships( RelationshipTraversalCursor cursor )
    {
        ((DefaultRelationshipTraversalCursor) cursor).init( nodeReference(), allRelationshipsReference(), read );
    }

    @Override
    public void properties( PropertyCursor cursor )
    {
        ((DefaultPropertyCursor) cursor).initNode( nodeReference(), propertiesReference(), read, read );
    }

    @Override
    public long relationshipGroupReference()
    {
        return storeCursor.relationshipGroupReference();
    }

    @Override
    public long allRelationshipsReference()
    {
        return storeCursor.allRelationshipsReference();
    }

    @Override
    public long propertiesReference()
    {
        return storeCursor.propertiesReference();
    }

    @Override
    public boolean isDense()
    {
        return storeCursor.isDense();
    }

    @Override
    public boolean next()
    {
        // Check tx state
        boolean hasChanges = hasChanges();

        if ( hasChanges && addedNodes.hasNext() )
        {
            storeCursor.setCurrent( addedNodes.next() );
            return true;
        }

        while ( storeCursor.next() )
        {
            if ( !hasChanges || !read.txState().nodeIsDeletedInThisTx( storeCursor.entityReference() ) )
            {
                return true;
            }
        }
        return false;
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
    private boolean hasChanges()
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
