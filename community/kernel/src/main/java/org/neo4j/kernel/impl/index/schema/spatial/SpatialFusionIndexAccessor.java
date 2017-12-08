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
package org.neo4j.kernel.impl.index.schema.spatial;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import org.neo4j.graphdb.ResourceIterator;
import org.neo4j.helpers.collection.BoundedIterable;
import org.neo4j.helpers.collection.CombiningIterable;
import org.neo4j.kernel.api.exceptions.index.IndexEntryConflictException;
import org.neo4j.kernel.api.index.IndexAccessor;
import org.neo4j.kernel.api.index.IndexUpdater;
import org.neo4j.kernel.api.index.PropertyAccessor;
import org.neo4j.kernel.api.schema.index.IndexDescriptor;
import org.neo4j.kernel.impl.api.index.IndexUpdateMode;
import org.neo4j.kernel.impl.api.index.sampling.IndexSamplingConfig;
import org.neo4j.kernel.impl.index.schema.spatial.SpatialSchemaIndexProvider.KnownSpatialIndex;
import org.neo4j.kernel.impl.index.schema.spatial.SpatialSchemaIndexProvider.KnownSpatialIndexFactory;
import org.neo4j.storageengine.api.schema.IndexReader;
import org.neo4j.values.storable.CoordinateReferenceSystem;

import static org.neo4j.helpers.collection.Iterators.concatResourceIterators;
import static org.neo4j.kernel.impl.index.schema.fusion.FusionIndexUtils.forAll;

class SpatialFusionIndexAccessor implements IndexAccessor
{
    private final Map<CoordinateReferenceSystem,KnownSpatialIndex> indexMap;
    private final long indexId;
    private final IndexDescriptor descriptor;
    private final IndexSamplingConfig samplingConfig;
    private final KnownSpatialIndexFactory indexFactory;

    SpatialFusionIndexAccessor( Map<CoordinateReferenceSystem,KnownSpatialIndex> indexMap, long indexId, IndexDescriptor descriptor,
            IndexSamplingConfig samplingConfig, KnownSpatialIndexFactory indexFactory ) {
        this.indexMap = indexMap;
        this.indexId = indexId;
        this.descriptor = descriptor;
        this.samplingConfig = samplingConfig;
        this.indexFactory = indexFactory;
    }

    @Override
    public void drop() throws IOException
    {
        forAll( ( index ) -> ((KnownSpatialIndex) index).getOnlineAccessor( descriptor, samplingConfig ).drop(), indexMap.values().toArray() );
    }

    @Override
    public IndexUpdater newUpdater( IndexUpdateMode mode )
    {
        return new SpatialFusionIndexUpdater( indexMap, indexId, indexFactory, descriptor, samplingConfig, mode );
    }

    @Override
    public void force() throws IOException
    {
        for(KnownSpatialIndex index : indexMap.values()) {
            index.getOnlineAccessor( descriptor, samplingConfig ).force();
        }
    }

    @Override
    public void refresh() throws IOException
    {
        forAll( ( entry ) -> ((KnownSpatialIndex) entry).getOnlineAccessor( descriptor, samplingConfig ).refresh(), indexMap.values().toArray() );
    }

    @Override
    public void close() throws IOException
    {
        forAll( ( entry ) -> ((KnownSpatialIndex) entry).getOnlineAccessor( descriptor, samplingConfig ).close(), indexMap.values().toArray() );
    }

    @Override
    public IndexReader newReader()
    {
        Map<CoordinateReferenceSystem,IndexReader> indexReaders = new HashMap<>();
        for ( Map.Entry<CoordinateReferenceSystem,IndexAccessor> accessor : accessorMap.entrySet() )
        {
            indexReaders.put( accessor.getKey(), accessor.getValue().newReader() );
        }
        return new SpatialFusionIndexReader( indexReaders, selector, descriptor.schema().getPropertyIds() );
    }

    @Override
    public BoundedIterable<Long> newAllEntriesReader()
    {
        List<BoundedIterable<Long>> allEntriesReader = accessorMap.entrySet().stream().map( e -> e.getValue().newAllEntriesReader() ).collect( Collectors.toList() );
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
                forAll( ( entries ) -> ((BoundedIterable) entries).close(), allEntriesReader );
            }

            @Override
            public Iterator<Long> iterator()
            {
                return new CombiningIterable( allEntriesReader ).iterator();
            }
        };
    }

    @Override
    public ResourceIterator<File> snapshotFiles() throws IOException
    {
        List<ResourceIterator<File>> snapshotFiles = new ArrayList<>();
        for ( Map.Entry<CoordinateReferenceSystem,IndexAccessor> accessor : accessorMap.entrySet() )
        {
            snapshotFiles.add( accessor.getValue().snapshotFiles() );
        }
        return concatResourceIterators( snapshotFiles.iterator() );
    }

    @Override
    public void verifyDeferredConstraints( PropertyAccessor propertyAccessor )
            throws IndexEntryConflictException, IOException
    {
        for(IndexAccessor accessor : accessorMap.values()) {
            accessor.verifyDeferredConstraints(propertyAccessor);
        }
    }
}
