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
package org.neo4j.kernel.impl.index.schema.tracking;

import java.io.File;
import java.io.IOException;
import java.util.concurrent.atomic.AtomicLong;

import org.neo4j.graphdb.ResourceIterator;
import org.neo4j.helpers.collection.BoundedIterable;
import org.neo4j.io.pagecache.IOLimiter;
import org.neo4j.kernel.api.exceptions.index.IndexEntryConflictException;
import org.neo4j.kernel.api.index.IndexAccessor;
import org.neo4j.kernel.api.index.IndexUpdater;
import org.neo4j.kernel.api.index.PropertyAccessor;
import org.neo4j.kernel.impl.api.index.IndexUpdateMode;
import org.neo4j.storageengine.api.schema.IndexReader;
import org.neo4j.values.storable.Value;

public class TrackingReadersIndexAccessor implements IndexAccessor
{
    private final IndexAccessor accessor;
    private static final AtomicLong openReaders = new AtomicLong();
    private static final AtomicLong closedReaders = new AtomicLong();

    public static long numberOfOpenReaders()
    {
        return openReaders.get();
    }

    public static long numberOfClosedReaders()
    {
        return closedReaders.get();
    }

    TrackingReadersIndexAccessor( IndexAccessor accessor )
    {
        this.accessor = accessor;
    }

    @Override
    public void drop() throws IOException
    {
        accessor.drop();
    }

    @Override
    public IndexUpdater newUpdater( IndexUpdateMode mode )
    {
        return accessor.newUpdater( mode );
    }

    @Override
    public void force( IOLimiter ioLimiter ) throws IOException
    {
        accessor.force( ioLimiter );
    }

    @Override
    public void refresh() throws IOException
    {
        accessor.refresh();
    }

    @Override
    public void close() throws IOException
    {
        accessor.close();
    }

    @Override
    public IndexReader newReader()
    {
        openReaders.incrementAndGet();
        return new TrackingIndexReader( accessor.newReader(), closedReaders );
    }

    @Override
    public BoundedIterable<Long> newAllEntriesReader()
    {
        return accessor.newAllEntriesReader();
    }

    @Override
    public ResourceIterator<File> snapshotFiles() throws IOException
    {
        return accessor.snapshotFiles();
    }

    @Override
    public void verifyDeferredConstraints( PropertyAccessor propertyAccessor ) throws IndexEntryConflictException, IOException
    {
        accessor.verifyDeferredConstraints( propertyAccessor );
    }

    @Override
    public boolean isDirty()
    {
        return accessor.isDirty();
    }

    @Override
    public void validateBeforeCommit( Value[] tuple )
    {
        accessor.validateBeforeCommit( tuple );
    }
}
