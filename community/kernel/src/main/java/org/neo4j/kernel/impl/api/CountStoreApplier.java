/**
 * Copyright (c) 2002-2014 "Neo Technology,"
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
package org.neo4j.kernel.impl.api;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.neo4j.collection.primitive.PrimitiveLongIterator;
import org.neo4j.collection.primitive.PrimitiveLongSet;
import org.neo4j.kernel.impl.nioneo.store.CountsStore;
import org.neo4j.kernel.impl.nioneo.store.NodeRecord;
import org.neo4j.kernel.impl.nioneo.store.NodeStore;
import org.neo4j.kernel.impl.nioneo.xa.command.Command;
import org.neo4j.kernel.impl.nioneo.xa.command.NeoCommandHandler;
import org.neo4j.kernel.impl.util.statistics.IntCounter;

import static org.neo4j.collection.primitive.Primitive.iterator;
import static org.neo4j.collection.primitive.Primitive.longSet;
import static org.neo4j.collection.primitive.PrimitiveLongCollections.emptyIterator;
import static org.neo4j.kernel.impl.nioneo.store.labels.NodeLabelsField.parseLabelsField;

import static org.neo4j.kernel.api.ReadOperations.ANY_LABEL;

public class CountStoreApplier extends NeoCommandHandler.Adapter
{
    private final CountsStore countsStore;
    private final NodeStore nodeStore;
    private int nodesDelta;
    private final Map<Integer/*labelId*/, IntCounter> labelDelta = new HashMap<>();

    public CountStoreApplier( CountsStore countsStore, NodeStore nodeStore )
    {
        this.countsStore = countsStore;
        this.nodeStore = nodeStore;
    }

    @Override
    public boolean visitNodeCommand( Command.NodeCommand command ) throws IOException
    {
        NodeRecord before = command.getBefore(), after = command.getAfter();
        if ( !before.inUse() && after.inUse() )
        { // node added
            nodesDelta++;
        }
        else if ( before.inUse() && !after.inUse() )
        { // node deleted
            nodesDelta--;
        }
        if ( before.getLabelField() != after.getLabelField() )
        {
            long[] labelsBefore = labels( before );
            long[] labelsAfter = labels( after );
            for ( PrimitiveLongIterator added = diff( labelsBefore, labelsAfter ); added.hasNext(); )
            {
                label( (int) added.next() ).increment();
            }
            for ( PrimitiveLongIterator removed = diff( labelsAfter, labelsBefore ); removed.hasNext(); )
            {
                label( (int) removed.next() ).decrement();
            }
        }
        return true;
    }

    private IntCounter label( int label )
    {
        IntCounter counter = labelDelta.get( label );
        if ( counter == null )
        {
            labelDelta.put( label, counter = new IntCounter() );
        }
        return counter;
    }

    @Override
    public void apply()
    {
        countsStore.updateCountsForNode( ANY_LABEL, nodesDelta );
        for ( Map.Entry<Integer, IntCounter> label : labelDelta.entrySet() )
        {
            countsStore.updateCountsForNode( label.getKey(), label.getValue().value() );
        }
    }

    private long[] labels( NodeRecord node )
    {
        return node.inUse() ? parseLabelsField( node ).get( nodeStore ) : null;
    }

    private static PrimitiveLongIterator diff( long[] remove, long[] add )
    {
        if ( add == null || add.length == 0 )
        {
            return emptyIterator();
        }
        else if ( remove == null || remove.length == 0 )
        {
            return iterator( add );
        }
        else
        {
            return removeAll( addAll( longSet( add.length ), add ), remove ).iterator();
        }
    }

    private static PrimitiveLongSet addAll( PrimitiveLongSet target, long... all )
    {
        for ( long label : all )
        {
            target.add( label );
        }
        return target;
    }

    private static PrimitiveLongSet removeAll( PrimitiveLongSet target, long... all )
    {
        for ( long label : all )
        {
            target.remove( label );
        }
        return target;
    }
}
