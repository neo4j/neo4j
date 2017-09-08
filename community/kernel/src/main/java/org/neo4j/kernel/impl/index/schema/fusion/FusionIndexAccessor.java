/*
 * Copyright (c) 2002-2017 "Neo Technology,"
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
package org.neo4j.kernel.impl.index.schema.fusion;

import java.io.File;
import java.io.IOException;
import java.util.Iterator;

import org.neo4j.graphdb.ResourceIterator;
import org.neo4j.helpers.collection.BoundedIterable;
import org.neo4j.helpers.collection.Iterables;
import org.neo4j.kernel.api.exceptions.index.IndexEntryConflictException;
import org.neo4j.kernel.api.index.IndexAccessor;
import org.neo4j.kernel.api.index.IndexUpdater;
import org.neo4j.kernel.api.index.PropertyAccessor;
import org.neo4j.kernel.impl.api.index.IndexUpdateMode;
import org.neo4j.kernel.impl.index.schema.fusion.FusionSchemaIndexProvider.DropAction;
import org.neo4j.kernel.impl.index.schema.fusion.FusionSchemaIndexProvider.Selector;
import org.neo4j.storageengine.api.schema.IndexReader;

import static java.util.Arrays.asList;
import static org.neo4j.helpers.collection.Iterators.concatResourceIterators;

class FusionIndexAccessor implements IndexAccessor
{
    private final IndexAccessor nativeAccessor;
    private final IndexAccessor luceneAccessor;
    private final Selector selector;
    private final long indexId;
    private final DropAction dropAction;

    FusionIndexAccessor( IndexAccessor nativeAccessor, IndexAccessor luceneAccessor, Selector selector,
            long indexId, DropAction dropAction )
    {
        this.nativeAccessor = nativeAccessor;
        this.luceneAccessor = luceneAccessor;
        this.selector = selector;
        this.indexId = indexId;
        this.dropAction = dropAction;
    }

    @Override
    public void drop() throws IOException
    {
        try
        {
            nativeAccessor.drop();
        }
        finally
        {
            luceneAccessor.drop();
        }
        dropAction.drop( indexId );
    }

    @Override
    public IndexUpdater newUpdater( IndexUpdateMode mode )
    {
        return new FusionIndexUpdater( nativeAccessor.newUpdater( mode ), luceneAccessor.newUpdater( mode ), selector );
    }

    @Override
    public void force() throws IOException
    {
        nativeAccessor.force();
        luceneAccessor.force();
    }

    @Override
    public void close() throws IOException
    {
        try
        {
            nativeAccessor.close();
        }
        finally
        {
            luceneAccessor.close();
        }
    }

    @Override
    public IndexReader newReader()
    {
        return new FusionIndexReader( nativeAccessor.newReader(), luceneAccessor.newReader(), selector );
    }

    @Override
    public BoundedIterable<Long> newAllEntriesReader()
    {
        BoundedIterable<Long> nativeAllEntries = nativeAccessor.newAllEntriesReader();
        BoundedIterable<Long> luceneAllEntries = luceneAccessor.newAllEntriesReader();
        return new BoundedIterable<Long>()
        {
            @Override
            public long maxCount()
            {
                long nativeMaxCount = nativeAllEntries.maxCount();
                long luceneMaxCount = luceneAllEntries.maxCount();
                return nativeMaxCount == UNKNOWN_MAX_COUNT || luceneMaxCount == UNKNOWN_MAX_COUNT ?
                       UNKNOWN_MAX_COUNT : nativeMaxCount + luceneMaxCount;
            }

            @Override
            public void close() throws Exception
            {
                try
                {
                    nativeAllEntries.close();
                }
                finally
                {
                    luceneAllEntries.close();
                }
            }

            @Override
            public Iterator<Long> iterator()
            {
                return Iterables.concat( nativeAllEntries, luceneAllEntries ).iterator();
            }
        };
    }

    @Override
    public ResourceIterator<File> snapshotFiles() throws IOException
    {
        return concatResourceIterators(
                asList( nativeAccessor.snapshotFiles(), luceneAccessor.snapshotFiles() ).iterator() );
    }

    @Override
    public void verifyDeferredConstraints( PropertyAccessor propertyAccessor )
            throws IndexEntryConflictException, IOException
    {
        nativeAccessor.verifyDeferredConstraints( propertyAccessor );
        luceneAccessor.verifyDeferredConstraints( propertyAccessor );
    }
}
