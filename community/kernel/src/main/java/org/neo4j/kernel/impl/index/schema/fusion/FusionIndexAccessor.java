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
            InstanceSelector<IndexAccessor> instanceSelector,
            long indexId,
            SchemaIndexDescriptor descriptor,
            DropAction dropAction )
    {
        super( slotSelector, instanceSelector );
        this.indexId = indexId;
        this.descriptor = descriptor;
        this.dropAction = dropAction;
    }

    @Override
    public void drop() throws IOException
    {
        instanceSelector.forAll( IndexAccessor::drop );
        dropAction.drop( indexId );
    }

    @Override
    public IndexUpdater newUpdater( IndexUpdateMode mode )
    {
        LazyInstanceSelector<IndexUpdater> updaterSelector = new LazyInstanceSelector<>( new IndexUpdater[INSTANCE_COUNT],
                slot -> instanceSelector.select( slot ).newUpdater( mode ) );
        return new FusionIndexUpdater( slotSelector, updaterSelector );
    }

    @Override
    public void force( IOLimiter ioLimiter ) throws IOException
    {
        instanceSelector.forAll( accessor -> accessor.force( ioLimiter ) );
    }

    @Override
    public void refresh() throws IOException
    {
        instanceSelector.forAll( IndexAccessor::refresh );
    }

    @Override
    public void close() throws IOException
    {
        instanceSelector.close( IndexAccessor::close );
    }

    @Override
    public IndexReader newReader()
    {
        LazyInstanceSelector<IndexReader> readerSelector = new LazyInstanceSelector<>( new IndexReader[INSTANCE_COUNT],
                slot -> instanceSelector.select( slot ).newReader() );
        return new FusionIndexReader( slotSelector, readerSelector, descriptor );
    }

    @Override
    public BoundedIterable<Long> newAllEntriesReader()
    {
        BoundedIterable<Long>[] entries = instanceSelector.instancesAs( new BoundedIterable[INSTANCE_COUNT], IndexAccessor::newAllEntriesReader );
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
        return concatResourceIterators(
                iterator( instanceSelector.instancesAs( new ResourceIterator[INSTANCE_COUNT], accessor -> accessor.snapshotFiles() ) ) );
    }

    @Override
    public void verifyDeferredConstraints( PropertyAccessor propertyAccessor )
            throws IndexEntryConflictException, IOException
    {
        for ( int slot = 0; slot < INSTANCE_COUNT; slot++ )
        {
            instanceSelector.select( slot ).verifyDeferredConstraints( propertyAccessor );
        }
    }

    @Override
    public boolean isDirty()
    {
        boolean isDirty = false;
        for ( int slot = 0; slot < INSTANCE_COUNT; slot++ )
        {
            isDirty |= instanceSelector.select( slot ).isDirty();
        }
        return isDirty;
    }

    @Override
    public void validateBeforeCommit( Value[] tuple )
    {
        instanceSelector.select( slotSelector.selectSlot( tuple, GROUP_OF ) ).validateBeforeCommit( tuple );
    }
}
