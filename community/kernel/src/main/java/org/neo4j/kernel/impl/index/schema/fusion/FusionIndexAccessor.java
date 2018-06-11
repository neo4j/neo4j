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
import org.neo4j.kernel.api.schema.index.StoreIndexDescriptor;
import org.neo4j.kernel.impl.api.index.IndexUpdateMode;
import org.neo4j.kernel.impl.index.schema.fusion.FusionIndexProvider.DropAction;
import org.neo4j.storageengine.api.schema.IndexReader;
import org.neo4j.values.storable.Value;

import static org.neo4j.helpers.collection.Iterators.concatResourceIterators;

class FusionIndexAccessor extends FusionIndexBase<IndexAccessor> implements IndexAccessor
{
    private final StoreIndexDescriptor descriptor;
    private final DropAction dropAction;

    FusionIndexAccessor( SlotSelector slotSelector,
            InstanceSelector<IndexAccessor> instanceSelector,
            StoreIndexDescriptor descriptor,
            DropAction dropAction )
    {
        super( slotSelector, instanceSelector );
        this.descriptor = descriptor;
        this.dropAction = dropAction;
    }

    @Override
    public void drop() throws IOException
    {
        instanceSelector.forAll( IndexAccessor::drop );
        dropAction.drop( descriptor.getId() );
    }

    @Override
    public IndexUpdater newUpdater( IndexUpdateMode mode )
    {
        LazyInstanceSelector<IndexUpdater> updaterSelector = new LazyInstanceSelector<>( slot -> instanceSelector.select( slot ).newUpdater( mode ) );
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
        LazyInstanceSelector<IndexReader> readerSelector = new LazyInstanceSelector<>( slot -> instanceSelector.select( slot ).newReader() );
        return new FusionIndexReader( slotSelector, readerSelector, descriptor );
    }

    @Override
    public BoundedIterable<Long> newAllEntriesReader()
    {
        Iterable<BoundedIterable<Long>> entries = instanceSelector.transform( IndexAccessor::newAllEntriesReader );
        return new BoundedIterable<Long>()
        {
            @Override
            public long maxCount()
            {
                long sum = 0;
                for ( BoundedIterable entry : entries )
                {
                    long maxCount = entry.maxCount();
                    if ( maxCount == UNKNOWN_MAX_COUNT )
                    {
                        return UNKNOWN_MAX_COUNT;
                    }
                    sum += maxCount;
                }
                return sum;
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
        return concatResourceIterators( instanceSelector.transform( IndexAccessor::snapshotFiles ).iterator() );
    }

    @Override
    public void verifyDeferredConstraints( PropertyAccessor propertyAccessor )
            throws IndexEntryConflictException, IOException
    {
        for ( IndexSlot slot : IndexSlot.values() )
        {
            instanceSelector.select( slot ).verifyDeferredConstraints( propertyAccessor );
        }
    }

    @Override
    public boolean isDirty()
    {
        return Iterables.stream( instanceSelector.transform( IndexAccessor::isDirty ) ).anyMatch( Boolean::booleanValue );
    }

    @Override
    public void validateBeforeCommit( Value[] tuple )
    {
        instanceSelector.select( slotSelector.selectSlot( tuple, GROUP_OF ) ).validateBeforeCommit( tuple );
    }
}
