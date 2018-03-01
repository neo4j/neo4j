/*
 * Copyright (c) 2002-2018 "Neo Technology,"
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

import org.neo4j.collection.primitive.PrimitiveLongCollections;
import org.neo4j.collection.primitive.PrimitiveLongIterator;
import org.neo4j.collection.primitive.PrimitiveLongResourceCollections;
import org.neo4j.collection.primitive.PrimitiveLongSet;
import org.neo4j.internal.kernel.api.LabelSet;
import org.neo4j.internal.kernel.api.NodeCursor;
import org.neo4j.internal.kernel.api.NodeLabelIndexCursor;
import org.neo4j.kernel.impl.index.labelscan.LabelScanValueIndexProgressor;
import org.neo4j.kernel.impl.util.diffsets.PrimitiveLongDiffSets;
import org.neo4j.storageengine.api.schema.IndexProgressor;
import org.neo4j.storageengine.api.schema.IndexProgressor.NodeLabelClient;

import static org.neo4j.kernel.impl.store.record.AbstractBaseRecord.NO_ID;

class DefaultNodeLabelIndexCursor extends IndexCursor<LabelScanValueIndexProgressor>
        implements NodeLabelIndexCursor, NodeLabelClient
{
    private Read read;
    private long node;
    private LabelSet labels;
    private PrimitiveLongIterator added;
    private PrimitiveLongSet removed;

    DefaultNodeLabelIndexCursor()
    {
        node = NO_ID;
    }

    @Override
    public void scan( LabelScanValueIndexProgressor progressor, boolean providesLabels, int label )
    {
        super.initialize( progressor );
        if ( read.hasTxStateWithChanges() )
        {
            PrimitiveLongDiffSets changes =
                    read.txState().nodesWithLabelChanged( label );
            added = changes.augment( PrimitiveLongResourceCollections.emptyIterator() );
            removed = PrimitiveLongCollections.asSet( read.txState().addedAndRemovedNodes().getRemoved() );
            removed.addAll( changes.getRemoved().iterator() );
        }
    }

    @Override
    public void unionScan( IndexProgressor progressor, boolean providesLabels, int... labels )
    {
        //TODO: Currently we don't have a good way of handling this in the tx state
        //The problem is this case:
        //Given a node with label :A
        //we remove label A in a transaction and follow that by
        //a scan of `:A and :B`. In order to figure this out we need
        //to check both tx state and disk, which we currently don't.
        throw new UnsupportedOperationException(  );
    }

    @Override
    public void intersectionScan( IndexProgressor progressor, boolean providesLabels, int... labels )
    {
        //TODO: Currently we don't have a good way of handling this in the tx state
        //The problem is for the nodes where some - but not all of the labels - are
        //added in the transaction. For these we need to go to disk and check if they
        //have the missing labels and hence return them or if not discard them.
        throw new UnsupportedOperationException(  );
    }

    @Override
    public boolean acceptNode( long reference, LabelSet labels )
    {
        if ( isRemoved( reference ) )
        {
            return false;
        }
        else
        {
            this.node = reference;
            this.labels = labels;

            return true;
        }
    }

    @Override
    public boolean next()
    {
        if ( added != null && added.hasNext() )
        {
            this.node = added.next();
            return true;
        }
        else
        {
            return innerNext();
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
    public LabelSet labels()
    {
        return labels;
    }

    @Override
    public void close()
    {
        super.close();
        node = NO_ID;
        labels = null;
        read = null;
        removed = null;
    }

    @Override
    public boolean isClosed()
    {
        return super.isClosed();
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
                    ", underlying record=" + super.toString() + " ]";
        }
    }

    private boolean isRemoved( long reference )
    {
        return removed != null && removed.contains( reference );
    }
}
