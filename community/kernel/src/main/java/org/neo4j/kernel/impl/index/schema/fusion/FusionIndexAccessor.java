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
import org.neo4j.kernel.api.schema.index.IndexDescriptor;
import org.neo4j.kernel.impl.api.index.IndexUpdateMode;
import org.neo4j.kernel.impl.index.schema.fusion.FusionSchemaIndexProvider.DropAction;
import org.neo4j.kernel.impl.index.schema.fusion.FusionSchemaIndexProvider.Selector;
import org.neo4j.storageengine.api.schema.IndexReader;

import static java.util.Arrays.asList;
import static org.neo4j.helpers.collection.Iterators.concatResourceIterators;
import static org.neo4j.kernel.impl.index.schema.fusion.FusionIndexUtils.forAll;

class FusionIndexAccessor implements IndexAccessor
{
    private final IndexAccessor nativeAccessor;
    private final IndexAccessor spatialAccessor;
    private final IndexAccessor luceneAccessor;
    private final Selector selector;
    private final long indexId;
    private final IndexDescriptor descriptor;
    private final DropAction dropAction;

    FusionIndexAccessor( IndexAccessor nativeAccessor, IndexAccessor spatialAccessor, IndexAccessor luceneAccessor,
            Selector selector, long indexId, IndexDescriptor descriptor, DropAction dropAction )
    {
        this.nativeAccessor = nativeAccessor;
        this.spatialAccessor = spatialAccessor;
        this.luceneAccessor = luceneAccessor;
        this.selector = selector;
        this.indexId = indexId;
        this.descriptor = descriptor;
        this.dropAction = dropAction;
    }

    @Override
    public void drop() throws IOException
    {
        forAll( accessor -> ((IndexAccessor) accessor).drop(), nativeAccessor, spatialAccessor, luceneAccessor );
        dropAction.drop( indexId );
    }

    @Override
    public IndexUpdater newUpdater( IndexUpdateMode mode )
    {
        return new FusionIndexUpdater( nativeAccessor.newUpdater( mode ), spatialAccessor.newUpdater( mode ), luceneAccessor.newUpdater( mode ), selector );
    }

    @Override
    public void force() throws IOException
    {
        nativeAccessor.force();
        spatialAccessor.force();
        luceneAccessor.force();
    }

    @Override
    public void refresh() throws IOException
    {
        nativeAccessor.refresh();
        luceneAccessor.refresh();
    }

    @Override
    public void close() throws IOException
    {
        forAll( accessor -> ((IndexAccessor) accessor).close(), nativeAccessor, spatialAccessor, luceneAccessor );
    }

    @Override
    public IndexReader newReader()
    {
        return new FusionIndexReader( nativeAccessor.newReader(), spatialAccessor.newReader(), luceneAccessor.newReader(), selector, descriptor );
    }

    @Override
    public BoundedIterable<Long> newAllEntriesReader()
    {
        BoundedIterable<Long> nativeAllEntries = nativeAccessor.newAllEntriesReader();
        BoundedIterable<Long> spatialAllEntries = spatialAccessor.newAllEntriesReader();
        BoundedIterable<Long> luceneAllEntries = luceneAccessor.newAllEntriesReader();
        return new BoundedIterable<Long>()
        {
            @Override
            public long maxCount()
            {
                long nativeMaxCount = nativeAllEntries.maxCount();
                long spatialMaxCount = spatialAllEntries.maxCount();
                long luceneMaxCount = luceneAllEntries.maxCount();
                return nativeMaxCount == UNKNOWN_MAX_COUNT || spatialMaxCount == UNKNOWN_MAX_COUNT || luceneMaxCount == UNKNOWN_MAX_COUNT ?
                       UNKNOWN_MAX_COUNT : nativeMaxCount + spatialMaxCount + luceneMaxCount;
            }

            @Override
            public void close() throws Exception
            {
                forAll( entries -> ((BoundedIterable) entries).close(), nativeAllEntries, spatialAllEntries, luceneAllEntries );
            }

            @Override
            public Iterator<Long> iterator()
            {
                return Iterables.concat( nativeAllEntries, spatialAllEntries, luceneAllEntries ).iterator();
            }
        };
    }

    @Override
    public ResourceIterator<File> snapshotFiles() throws IOException
    {
        return concatResourceIterators(
                asList( nativeAccessor.snapshotFiles(), spatialAccessor.snapshotFiles(), luceneAccessor.snapshotFiles() ).iterator() );
    }

    @Override
    public void verifyDeferredConstraints( PropertyAccessor propertyAccessor )
            throws IndexEntryConflictException, IOException
    {
        nativeAccessor.verifyDeferredConstraints( propertyAccessor );
        spatialAccessor.verifyDeferredConstraints( propertyAccessor );
        luceneAccessor.verifyDeferredConstraints( propertyAccessor );
    }
}
