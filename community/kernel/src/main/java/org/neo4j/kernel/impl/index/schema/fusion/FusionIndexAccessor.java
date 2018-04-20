/*
 * Copyright (c) 2002-2018 "Neo4j,"
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
package org.neo4j.kernel.impl.index.schema.fusion;

import java.io.File;
import java.io.IOException;
import java.util.Iterator;

import org.neo4j.graphdb.ResourceIterator;
import org.neo4j.helpers.collection.BoundedIterable;
import org.neo4j.helpers.collection.Iterables;
import org.neo4j.io.pagecache.IOLimiter;
import org.neo4j.kernel.api.exceptions.index.IndexEntryConflictException;
import org.neo4j.kernel.api.index.IndexAccessor;
import org.neo4j.kernel.api.index.IndexUpdater;
import org.neo4j.kernel.api.index.PropertyAccessor;
import org.neo4j.kernel.api.schema.index.SchemaIndexDescriptor;
import org.neo4j.kernel.impl.api.index.IndexUpdateMode;
import org.neo4j.kernel.impl.index.schema.fusion.FusionIndexProvider.DropAction;
import org.neo4j.storageengine.api.schema.IndexReader;
import org.neo4j.values.storable.Value;

import static org.neo4j.helpers.collection.Iterators.concatResourceIterators;
import static org.neo4j.helpers.collection.Iterators.iterator;
import static org.neo4j.kernel.impl.index.schema.fusion.SlotSelector.INSTANCE_COUNT;

class FusionIndexAccessor extends FusionIndexBase<IndexAccessor> implements IndexAccessor
{
    private final long indexId;
    private final SchemaIndexDescriptor descriptor;
    private final DropAction dropAction;

    FusionIndexAccessor( SlotSelector slotSelector,
            Selector<IndexAccessor> selector,
            long indexId,
            SchemaIndexDescriptor descriptor,
            DropAction dropAction )
    {
        super( slotSelector, selector );
        this.indexId = indexId;
        this.descriptor = descriptor;
        this.dropAction = dropAction;
    }

    @Override
    public void drop() throws IOException
    {
        forAll( IndexAccessor::drop, selector );
        dropAction.drop( indexId );
    }

    @Override
    public IndexUpdater newUpdater( IndexUpdateMode mode )
    {
        Selector<IndexUpdater> updaterSelector = new Selector<>( new IndexUpdater[INSTANCE_COUNT], slot -> selector.select( slot ).newUpdater( mode ) );
        return new FusionIndexUpdater( slotSelector, updaterSelector );
    }

    @Override
    public void force( IOLimiter ioLimiter ) throws IOException
    {
        forAll( accessor -> accessor.force( ioLimiter ), selector );
    }

    @Override
    public void refresh() throws IOException
    {
        forAll( IndexAccessor::refresh, selector );
    }

    @Override
    public void close() throws IOException
    {
        forAll( IndexAccessor::close, selector );
    }

    @Override
    public IndexReader newReader()
    {
        Selector<IndexReader> readerSelector = new Selector<>( new IndexReader[INSTANCE_COUNT], slot -> selector.select( slot ).newReader() );
        return new FusionIndexReader( slotSelector, readerSelector, descriptor );
    }

    @Override
    public BoundedIterable<Long> newAllEntriesReader()
    {
        BoundedIterable<Long>[] entries = instancesAs( new BoundedIterable[INSTANCE_COUNT], IndexAccessor::newAllEntriesReader );
        return new BoundedIterable<Long>()
        {
            @Override
            public long maxCount()
            {
                long[] maxCounts = new long[entries.length];
                long sum = 0;
                for ( int i = 0; i < entries.length; i++ )
                {
                    maxCounts[i] = entries[i].maxCount();
                    sum += maxCounts[i];
                }
                return existsUnknownMaxCount( maxCounts ) ? UNKNOWN_MAX_COUNT : sum;
            }

            private boolean existsUnknownMaxCount( long... maxCounts )
            {
                for ( long maxCount : maxCounts )
                {
                    if ( maxCount == UNKNOWN_MAX_COUNT )
                    {
                        return true;
                    }
                }
                return false;
            }

            @SuppressWarnings( "unchecked" )
            @Override
            public void close() throws Exception
            {
                forAll( BoundedIterable::close, entries );
            }

            @Override
            public Iterator<Long> iterator()
            {
                return Iterables.concat( entries ).iterator();
            }
        };
    }

    @Override
    public ResourceIterator<File> snapshotFiles() throws IOException
    {
        return concatResourceIterators( iterator( instancesAs( new ResourceIterator[INSTANCE_COUNT], accessor -> accessor.snapshotFiles() ) ) );
    }

    @Override
    public void verifyDeferredConstraints( PropertyAccessor propertyAccessor )
            throws IndexEntryConflictException, IOException
    {
        for ( int slot = 0; slot < INSTANCE_COUNT; slot++ )
        {
            selector.select( slot ).verifyDeferredConstraints( propertyAccessor );
        }
    }

    @Override
    public boolean isDirty()
    {
        boolean isDirty = false;
        for ( int slot = 0; slot < INSTANCE_COUNT; slot++ )
        {
            isDirty |= selector.select( slot ).isDirty();
        }
        return isDirty;
    }

    @Override
    public void validateBeforeCommit( Value[] tuple )
    {
        selector.select( slotSelector.selectSlot( tuple, GROUP_OF ) ).validateBeforeCommit( tuple );
    }
}
