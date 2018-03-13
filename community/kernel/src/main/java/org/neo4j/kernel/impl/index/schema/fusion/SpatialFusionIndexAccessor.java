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
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.neo4j.graphdb.ResourceIterator;
import org.neo4j.helpers.collection.BoundedIterable;
import org.neo4j.helpers.collection.CombiningIterable;
import org.neo4j.io.pagecache.IOLimiter;
import org.neo4j.kernel.api.exceptions.index.IndexEntryConflictException;
import org.neo4j.kernel.api.index.IndexAccessor;
import org.neo4j.kernel.api.index.IndexUpdater;
import org.neo4j.kernel.api.index.PropertyAccessor;
import org.neo4j.kernel.api.schema.index.SchemaIndexDescriptor;
import org.neo4j.kernel.impl.api.index.IndexUpdateMode;
import org.neo4j.kernel.impl.api.index.sampling.IndexSamplingConfig;
import org.neo4j.kernel.impl.index.schema.SpatialCRSSchemaIndex;
import org.neo4j.storageengine.api.schema.IndexReader;
import org.neo4j.values.storable.CoordinateReferenceSystem;

import static org.neo4j.helpers.collection.Iterators.concatResourceIterators;
import static org.neo4j.kernel.impl.index.schema.fusion.FusionIndexBase.forAll;

class SpatialFusionIndexAccessor implements IndexAccessor
{
    private final Map<CoordinateReferenceSystem,SpatialCRSSchemaIndex> indexMap;
    private final long indexId;
    private final SchemaIndexDescriptor descriptor;
    private final IndexSamplingConfig samplingConfig;
    private final SpatialCRSSchemaIndex.Supplier indexFactory;

    SpatialFusionIndexAccessor( Map<CoordinateReferenceSystem,SpatialCRSSchemaIndex> indexMap, long indexId,
                                SchemaIndexDescriptor descriptor, IndexSamplingConfig samplingConfig,
                                SpatialCRSSchemaIndex.Supplier indexFactory ) throws IOException
    {
        this.indexMap = indexMap;
        this.indexId = indexId;
        this.descriptor = descriptor;
        this.samplingConfig = samplingConfig;
        this.indexFactory = indexFactory;
        for ( SpatialCRSSchemaIndex index : indexMap.values() )
        {
            index.takeOnline();
        }
    }

    @Override
    public void drop() throws IOException
    {
        forAll( SpatialCRSSchemaIndex::drop, indexMap.values() );
        indexMap.clear();
    }

    @Override
    public IndexUpdater newUpdater( IndexUpdateMode mode )
    {
        return SpatialFusionIndexUpdater.updaterForAccessor( indexMap, indexId, indexFactory, descriptor );
    }

    @Override
    public void force( IOLimiter ioLimiter ) throws IOException
    {
        forAll( spatialCRSSchemaIndex -> spatialCRSSchemaIndex.force( ioLimiter ), indexMap.values() );
    }

    @Override
    public void refresh()
    {
        // not required in this implementation
    }

    @Override
    public void close() throws IOException
    {
        forAll( SpatialCRSSchemaIndex::close, indexMap.values() );
    }

    @Override
    public IndexReader newReader()
    {
        Map<CoordinateReferenceSystem,IndexReader> indexReaders = new HashMap<>();
        for ( Map.Entry<CoordinateReferenceSystem,SpatialCRSSchemaIndex> index : indexMap.entrySet() )
        {
            // TODO should this be populated here, or delegate to SpatialFusionIndexReader?
            indexReaders.put( index.getKey(), index.getValue().newReader( samplingConfig, descriptor ) );
        }
        return new SpatialFusionIndexReader( indexReaders, descriptor );
    }

    @Override
    public BoundedIterable<Long> newAllEntriesReader()
    {
        ArrayList<BoundedIterable<Long>> allEntriesReader = new ArrayList<>();
        for ( SpatialCRSSchemaIndex index : indexMap.values() )
        {
            allEntriesReader.add( index.newAllEntriesReader() );
        }
        return new BoundedIterable<Long>()
        {
            @Override
            public long maxCount()
            {
                return allEntriesReader.stream().map( BoundedIterable::maxCount ).reduce( 0L, ( acc, e ) ->
                {
                    if ( acc == UNKNOWN_MAX_COUNT || e == UNKNOWN_MAX_COUNT )
                    {
                        return UNKNOWN_MAX_COUNT;
                    }
                    return acc + e;
                } );
            }

            @Override
            public void close() throws Exception
            {
                forAll( BoundedIterable::close, allEntriesReader );
            }

            @Override
            public Iterator<Long> iterator()
            {
                return new CombiningIterable<>( allEntriesReader ).iterator();
            }
        };
    }

    @Override
    public ResourceIterator<File> snapshotFiles() throws IOException
    {
        List<ResourceIterator<File>> snapshotFiles = new ArrayList<>();
        for ( SpatialCRSSchemaIndex index : indexMap.values() )
        {
            snapshotFiles.add( index.snapshotFiles() );
        }
        return concatResourceIterators( snapshotFiles.iterator() );
    }

    @Override
    public void verifyDeferredConstraints( PropertyAccessor propertyAccessor )
            throws IndexEntryConflictException, IOException
    {
        // Not needed since uniqueness is verified automatically w/o cost for every update.
    }

    @Override
    public boolean isDirty()
    {
        return indexMap.values().stream().anyMatch( SpatialCRSSchemaIndex::wasDirtyOnStartup );
    }
}
