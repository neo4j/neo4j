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
import org.neo4j.io.pagecache.IOLimiter;
import org.neo4j.kernel.api.exceptions.index.IndexEntryConflictException;
import org.neo4j.kernel.api.index.IndexAccessor;
import org.neo4j.kernel.api.index.IndexUpdater;
import org.neo4j.kernel.api.index.PropertyAccessor;
import org.neo4j.kernel.api.schema.index.SchemaIndexDescriptor;
import org.neo4j.kernel.impl.api.index.IndexUpdateMode;
import org.neo4j.kernel.impl.index.schema.fusion.FusionSchemaIndexProvider.DropAction;
import org.neo4j.kernel.impl.index.schema.fusion.FusionSchemaIndexProvider.Selector;
import org.neo4j.storageengine.api.schema.IndexReader;

import static java.util.Arrays.asList;
import static org.neo4j.helpers.collection.Iterators.concatResourceIterators;
import static org.neo4j.kernel.impl.index.schema.fusion.FusionIndexUtils.forAll;

class FusionIndexAccessor implements IndexAccessor
{
    private final IndexAccessor numberAccessor;
    private final IndexAccessor spatialAccessor;
    private final IndexAccessor temporalAccessor;
    private final IndexAccessor luceneAccessor;
    private final Selector selector;
    private final long indexId;
    private final SchemaIndexDescriptor descriptor;
    private final DropAction dropAction;

    FusionIndexAccessor( IndexAccessor numberAccessor,
            IndexAccessor spatialAccessor,
            IndexAccessor temporalAccessor, IndexAccessor luceneAccessor, Selector selector,
            long indexId, SchemaIndexDescriptor descriptor, DropAction dropAction )
    {
        this.numberAccessor = numberAccessor;
        this.spatialAccessor = spatialAccessor;
        this.temporalAccessor = temporalAccessor;
        this.luceneAccessor = luceneAccessor;
        this.selector = selector;
        this.indexId = indexId;
        this.descriptor = descriptor;
        this.dropAction = dropAction;
    }

    @Override
    public void drop() throws IOException
    {
        forAll( IndexAccessor::drop, numberAccessor, spatialAccessor, temporalAccessor, luceneAccessor );
        dropAction.drop( indexId );
    }

    @Override
    public IndexUpdater newUpdater( IndexUpdateMode mode )
    {
        return new FusionIndexUpdater(
                numberAccessor.newUpdater( mode ),
                spatialAccessor.newUpdater( mode ),
                temporalAccessor.newUpdater( mode ),
                luceneAccessor.newUpdater( mode ), selector );
    }

    @Override
    public void force( IOLimiter ioLimiter ) throws IOException
    {
        numberAccessor.force( ioLimiter );
        spatialAccessor.force( ioLimiter );
        temporalAccessor.force( ioLimiter );
        luceneAccessor.force( ioLimiter );
    }

    @Override
    public void refresh() throws IOException
    {
        numberAccessor.refresh();
        spatialAccessor.refresh();
        temporalAccessor.refresh();
        luceneAccessor.refresh();
    }

    @Override
    public void close() throws IOException
    {
        forAll( IndexAccessor::close, numberAccessor, spatialAccessor, temporalAccessor, luceneAccessor );
    }

    @Override
    public IndexReader newReader()
    {
        return new FusionIndexReader(
                numberAccessor.newReader(),
                spatialAccessor.newReader(),
                temporalAccessor.newReader(),
                luceneAccessor.newReader(),
                selector,
                descriptor );
    }

    @Override
    public BoundedIterable<Long> newAllEntriesReader()
    {
        BoundedIterable<Long> numberAllEntries = numberAccessor.newAllEntriesReader();
        BoundedIterable<Long> spatialAllEntries = spatialAccessor.newAllEntriesReader();
        BoundedIterable<Long> temporalAllEntries = temporalAccessor.newAllEntriesReader();
        BoundedIterable<Long> luceneAllEntries = luceneAccessor.newAllEntriesReader();
        return new BoundedIterable<Long>()
        {
            @Override
            public long maxCount()
            {
                long numberMaxCount = numberAllEntries.maxCount();
                long spatialMaxCount = spatialAllEntries.maxCount();
                long temporalMaxCount = temporalAllEntries.maxCount();
                long luceneMaxCount = luceneAllEntries.maxCount();
                return existsUnknownMaxCount( numberMaxCount, spatialMaxCount, temporalMaxCount, luceneMaxCount ) ?
                       UNKNOWN_MAX_COUNT : numberMaxCount + spatialMaxCount + temporalMaxCount + luceneMaxCount;
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
                forAll( BoundedIterable::close, numberAllEntries, spatialAllEntries, temporalAllEntries, luceneAllEntries );
            }

            @Override
            public Iterator<Long> iterator()
            {
                return Iterables.concat( numberAllEntries, spatialAllEntries, temporalAllEntries, luceneAllEntries ).iterator();
            }
        };
    }

    @Override
    public ResourceIterator<File> snapshotFiles() throws IOException
    {
        return concatResourceIterators(
                asList( numberAccessor.snapshotFiles(),
                        spatialAccessor.snapshotFiles(),
                        temporalAccessor.snapshotFiles(),
                        luceneAccessor.snapshotFiles() ).iterator() );
    }

    @Override
    public void verifyDeferredConstraints( PropertyAccessor propertyAccessor )
            throws IndexEntryConflictException, IOException
    {
        numberAccessor.verifyDeferredConstraints( propertyAccessor );
        spatialAccessor.verifyDeferredConstraints( propertyAccessor );
        temporalAccessor.verifyDeferredConstraints( propertyAccessor );
        luceneAccessor.verifyDeferredConstraints( propertyAccessor );
    }

    @Override
    public boolean isDirty()
    {
        return numberAccessor.isDirty() || spatialAccessor.isDirty() || temporalAccessor.isDirty() || luceneAccessor.isDirty();
    }
}
