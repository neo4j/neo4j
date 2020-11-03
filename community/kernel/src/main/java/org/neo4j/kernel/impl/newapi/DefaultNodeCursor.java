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

import org.eclipse.collections.api.iterator.IntIterator;
import org.eclipse.collections.api.iterator.LongIterator;
import org.eclipse.collections.api.set.primitive.MutableIntSet;
import org.eclipse.collections.api.set.primitive.MutableLongSet;
import org.eclipse.collections.impl.factory.primitive.IntSets;
import org.eclipse.collections.impl.iterator.ImmutableEmptyLongIterator;
import org.eclipse.collections.impl.set.mutable.primitive.LongHashSet;

import org.neo4j.collection.PrimitiveLongCollections;
import org.neo4j.graphdb.Direction;
import org.neo4j.internal.kernel.api.NodeCursor;
import org.neo4j.internal.kernel.api.PropertyCursor;
import org.neo4j.internal.kernel.api.RelationshipTraversalCursor;
import org.neo4j.internal.kernel.api.TokenSet;
import org.neo4j.internal.kernel.api.security.AccessMode;
import org.neo4j.io.IOUtils;
import org.neo4j.kernel.api.txstate.TransactionState;
import org.neo4j.storageengine.api.AllNodeScan;
import org.neo4j.storageengine.api.Degrees;
import org.neo4j.storageengine.api.RelationshipDirection;
import org.neo4j.storageengine.api.RelationshipSelection;
import org.neo4j.storageengine.api.StorageNodeCursor;
import org.neo4j.storageengine.api.txstate.LongDiffSets;
import org.neo4j.storageengine.api.txstate.NodeState;
import org.neo4j.storageengine.util.EagerDegrees;
import org.neo4j.storageengine.util.SingleDegree;

import static org.neo4j.kernel.impl.newapi.Read.NO_ID;

class DefaultNodeCursor extends TraceableCursor<DefaultNodeCursor> implements NodeCursor
{
    Read read;
    boolean checkHasChanges;
    boolean hasChanges;
    private LongIterator addedNodes;
    StorageNodeCursor storeCursor;
    private StorageNodeCursor securityStoreCursor;
    private long currentAddedInTx;
    private long single;
    private AccessMode accessMode;

    DefaultNodeCursor( CursorPool<DefaultNodeCursor> pool, StorageNodeCursor storeCursor, StorageNodeCursor securityStoreCursor )
    {
        super( pool );
        this.storeCursor = storeCursor;
        this.securityStoreCursor = securityStoreCursor;
    }

    void scan( Read read )
    {
        storeCursor.scan();
        this.read = read;
        this.single = NO_ID;
        this.currentAddedInTx = NO_ID;
        this.checkHasChanges = true;
        this.addedNodes = ImmutableEmptyLongIterator.INSTANCE;
        if ( tracer != null )
        {
            tracer.onAllNodesScan();
        }
    }

    boolean scanBatch( Read read, AllNodeScan scan, int sizeHint, LongIterator addedNodes, boolean hasChanges )
    {
        this.read = read;
        this.single = NO_ID;
        this.currentAddedInTx = NO_ID;
        this.checkHasChanges = false;
        this.hasChanges = hasChanges;
        this.addedNodes = addedNodes;
        boolean scanBatch = storeCursor.scanBatch( scan, sizeHint );
        return addedNodes.hasNext() || scanBatch;
    }

    void single( long reference, Read read )
    {
        storeCursor.single( reference );
        this.read = read;
        this.single = reference;
        this.currentAddedInTx = NO_ID;
        this.checkHasChanges = true;
        this.addedNodes = ImmutableEmptyLongIterator.INSTANCE;
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
    public TokenSet labels()
    {
        if ( currentAddedInTx != NO_ID )
        {
            //Node added in tx-state, no reason to go down to store and check
            TransactionState txState = read.txState();
            return Labels.from( txState.nodeStateLabelDiffSets( currentAddedInTx ).getAdded() );
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
            return Labels.from( txState.augmentLabels( labels, txState.getNodeState( storeCursor.entityReference() ) ) );
        }
        else
        {
            //Nothing in tx state, just read the data.
            return Labels.from( storeCursor.labels() );
        }
    }

    /**
     * The normal labels() method takes into account TxState for both created nodes and set/remove labels.
     * Some code paths need to consider created, but not changed labels.
     */
    @Override
    public TokenSet labelsIgnoringTxStateSetRemove()
    {
        if ( currentAddedInTx != NO_ID )
        {
            //Node added in tx-state, no reason to go down to store and check
            TransactionState txState = read.txState();
            return Labels.from( txState.nodeStateLabelDiffSets( currentAddedInTx ).getAdded() );
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

    @Override
    public void relationships( RelationshipTraversalCursor cursor, RelationshipSelection selection )
    {
        ((DefaultRelationshipTraversalCursor) cursor).init( this, selection, read );
    }

    @Override
    public void properties( PropertyCursor cursor )
    {
        ((DefaultPropertyCursor) cursor).initNode( nodeReference(), propertiesReference(), read, read );
    }

    @Override
    public long relationshipsReference()
    {
        return currentAddedInTx != NO_ID ? NO_ID : storeCursor.relationshipsReference();
    }

    @Override
    public long propertiesReference()
    {
        return currentAddedInTx != NO_ID ? NO_ID : storeCursor.propertiesReference();
    }

    @Override
    public boolean supportsFastDegreeLookup()
    {
        return currentAddedInTx == NO_ID && storeCursor.supportsFastDegreeLookup();
    }

    @Override
    public int[] relationshipTypes()
    {
        boolean hasChanges = hasChanges();
        NodeState nodeTxState = hasChanges ? read.txState().getNodeState( nodeReference() ) : null;
        int[] storedTypes = currentAddedInTx == NO_ID ? storeCursor.relationshipTypes() : null;
        MutableIntSet types = storedTypes != null ? IntSets.mutable.of( storedTypes ) : IntSets.mutable.empty();
        if ( nodeTxState != null )
        {
            types.addAll( nodeTxState.getAddedRelationshipTypes() );
        }
        return types.toArray();
    }

    @Override
    public Degrees degrees( RelationshipSelection selection )
    {
        EagerDegrees degrees = new EagerDegrees();
        fillDegrees( selection, degrees );
        return degrees;
    }

    @Override
    public int degree( RelationshipSelection selection )
    {
        SingleDegree degrees = new SingleDegree();
        fillDegrees( selection, degrees );
        return degrees.getTotal();
    }

    @Override
    public int degreeWithMax( int maxDegree, RelationshipSelection selection )
    {
        SingleDegree degrees = new SingleDegree( maxDegree );
        fillDegrees( selection, degrees );
        return Math.min(degrees.getTotal(), maxDegree);
    }

    private void fillDegrees( RelationshipSelection selection, Degrees.Mutator degrees )
    {
        boolean hasChanges = hasChanges();
        NodeState nodeTxState = hasChanges ? read.txState().getNodeState( nodeReference() ) : null;
        if ( currentAddedInTx == NO_ID )
        {
            if ( allowsTraverseAll() )
            {
                storeCursor.degrees( selection, degrees, true );
            }
            else
            {
                storeCursor.degrees( new SecureRelationshipSelection( selection ), degrees, false );
            }
        }
        if ( nodeTxState != null )
        {
            // Then add the remaining types that's only present in the tx-state
            IntIterator txTypes = nodeTxState.getAddedAndRemovedRelationshipTypes().intIterator();
            while ( txTypes.hasNext() )
            {
                int type = txTypes.next();
                if ( selection.test( type ) )
                {
                    int outgoing = selection.test( RelationshipDirection.OUTGOING ) ? nodeTxState.augmentDegree( RelationshipDirection.OUTGOING, 0, type ) : 0;
                    int incoming = selection.test( RelationshipDirection.INCOMING ) ? nodeTxState.augmentDegree( RelationshipDirection.INCOMING, 0, type ) : 0;
                    int loop = selection.test( RelationshipDirection.LOOP ) ? nodeTxState.augmentDegree( RelationshipDirection.LOOP, 0, type ) : 0;
                    if ( !degrees.add( type, outgoing, incoming, loop ) )
                    {
                        return;
                    }
                }
            }
        }
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
                if ( tracer != null )
                {
                    tracer.onNode( nodeReference() );
                }
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
                if ( tracer != null )
                {
                    tracer.onNode( nodeReference() );
                }
                return true;
            }
        }
        return false;
    }

    boolean allowsTraverse()
    {
        if ( accessMode == null )
        {
            accessMode = read.ktx.securityContext().mode();
        }
        return accessMode.allowsTraverseAllLabels() || accessMode.allowsTraverseNode( storeCursor.labels() );
    }

    boolean allowsTraverseAll()
    {
        if ( accessMode == null )
        {
            accessMode = read.ktx.securityContext().mode();
        }
        return accessMode.allowsTraverseAllRelTypes() && accessMode.allowsTraverseAllLabels();
    }

    @Override
    public void closeInternal()
    {
        if ( !isClosed() )
        {
            read = null;
            checkHasChanges = true;
            addedNodes = ImmutableEmptyLongIterator.INSTANCE;
            storeCursor.reset();
            if ( securityStoreCursor != null )
            {
                securityStoreCursor.reset();
            }
            accessMode = null;
        }
        super.closeInternal();
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
        if ( checkHasChanges )
        {
            computeHasChanges();
        }
        return hasChanges;
    }

    private void computeHasChanges()
    {
        checkHasChanges = false;
        if ( hasChanges = read.hasTxStateWithChanges() )
        {
            if ( single != NO_ID )
            {
                addedNodes = read.txState().nodeIsAddedInThisTx( single ) ?
                             PrimitiveLongCollections.single( single ) : ImmutableEmptyLongIterator.INSTANCE;
            }
            else
            {
                addedNodes = read.txState().addedAndRemovedNodes().getAdded().freeze().longIterator();
            }
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
            return "NodeCursor[id=" + nodeReference() + ", " + storeCursor + "]";
        }
    }

    void release()
    {
        IOUtils.closeAllUnchecked( storeCursor, securityStoreCursor );
    }

    private class SecureRelationshipSelection extends RelationshipSelection
    {
        private final RelationshipSelection inner;

        SecureRelationshipSelection( RelationshipSelection selection )
        {
            inner = selection;
        }

        @Override
        public boolean test( int type, long sourceReference, long targetReference )
        {
            if ( accessMode.allowsTraverseRelType( type ) )
            {
                if ( sourceReference == targetReference )
                {
                    return inner.test( type );
                }
                long otherReference = sourceReference == nodeReference() ? targetReference : sourceReference;
                securityStoreCursor.single( otherReference );
                return securityStoreCursor.next() && accessMode.allowsTraverseNode( securityStoreCursor.labels() ) && inner.test( type );
            }
            else
            {
                return false;
            }
        }

        @Override
        public boolean test( int type )
        {
            return inner.test( type );
        }

        @Override
        public boolean test( RelationshipDirection direction )
        {
            return inner.test( direction );
        }

        @Override
        public Direction direction()
        {
            return inner.direction();
        }

        @Override
        public int numberOfCriteria()
        {
            return inner.numberOfCriteria();
        }

        @Override
        public Criterion criterion( int index )
        {
            return inner.criterion( index );
        }

        @Override
        public boolean test( int type, RelationshipDirection direction )
        {
            return inner.test( type, direction );
        }

        @Override
        public boolean isTypeLimited()
        {
            return inner.isTypeLimited();
        }

        @Override
        public LongIterator addedRelationship( NodeState transactionState )
        {
            return inner.addedRelationship( transactionState );
        }
    }
}
