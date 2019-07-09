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
import org.eclipse.collections.impl.iterator.ImmutableEmptyLongIterator;

import org.neo4j.internal.kernel.api.LabelSet;
import org.neo4j.internal.kernel.api.NodeCursor;
import org.neo4j.internal.kernel.api.NodeLabelIndexCursor;
import org.neo4j.kernel.api.index.IndexProgressor;
import org.neo4j.kernel.api.index.IndexProgressor.NodeLabelClient;
import org.neo4j.storageengine.api.txstate.LongDiffSets;

import static org.neo4j.collection.PrimitiveLongCollections.mergeToSet;
import static org.neo4j.kernel.impl.newapi.Read.NO_ID;

class DefaultNodeLabelIndexCursor extends IndexCursor<IndexProgressor>
        implements NodeLabelIndexCursor
{
    private Read read;
    private long node;
    private LabelSet labels;
    private LongIterator added;
    private LongSet removed;

    private final CursorPool<DefaultNodeLabelIndexCursor> pool;

    DefaultNodeLabelIndexCursor( CursorPool<DefaultNodeLabelIndexCursor> pool )
    {
        this.pool = pool;
        this.node = NO_ID;
    }

    public void scan( IndexProgressor progressor, int label )
    {
        super.initialize( progressor );
        if ( read.hasTxStateWithChanges() )
        {
            final LongDiffSets changes = read.txState().nodesWithLabelChanged( label );
            added = changes.augment( ImmutableEmptyLongIterator.INSTANCE );
            removed = mergeToSet( read.txState().addedAndRemovedNodes().getRemoved(), changes.getRemoved() );
        }
        getTracer().onLabelScan( label );
    }

    public void scan( IndexProgressor progressor, LongIterator added, LongSet removed )
    {
        super.initialize( progressor );
        this.added = added;
        this.removed = removed;
    }

    public NodeLabelClient nodeLabelClient()
    {
        return ( reference, labels ) ->
        {
            if ( isRemoved( reference ) )
            {
                return false;
            }
            else
            {
                DefaultNodeLabelIndexCursor.this.node = reference;
                DefaultNodeLabelIndexCursor.this.labels = labels;

                return true;
            }
        };
    }

    @Override
    public boolean next()
    {
        if ( added != null && added.hasNext() )
        {
            this.node = added.next();
            getTracer().onNode( this.node );
            return true;
        }
        else
        {
            boolean hasNext = innerNext();
            if ( hasNext )
            {
                getTracer().onNode( this.node );
            }
            return hasNext;
        }
    }

    public void setRead( Read read )
    {
        this.read = read;
    }

    @Override
    public void node( NodeCursor cursor )
    {
        read.singleNode( node, cursor );
    }

    @Override
    public long nodeReference()
    {
        return node;
    }

    @Override
    public float score()
    {
        return Float.NaN;
    }

    @Override
    public LabelSet labels()
    {
        return labels;
    }

    @Override
    public void close()
    {
        if ( !isClosed() )
        {
            closeProgressor();
            node = NO_ID;
            labels = null;
            read = null;
            removed = null;

            pool.accept( this );
        }
    }

    @Override
    public boolean isClosed()
    {
        return isProgressorClosed();
    }

    @Override
    public String toString()
    {
        if ( isClosed() )
        {
            return "NodeLabelIndexCursor[closed state]";
        }
        else
        {
            return "NodeLabelIndexCursor[node=" + node + ", labels= " + labels +
                    ", underlying record=" + super.toString() + "]";
        }
    }

    private boolean isRemoved( long reference )
    {
        return removed != null && removed.contains( reference );
    }

    public void release()
    {
        // nothing to do
    }
}
