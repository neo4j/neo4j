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
package org.neo4j.kernel.impl.transaction.state;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.function.Function;

import org.neo4j.collection.primitive.PrimitiveLongSet;
import org.neo4j.graphdb.Resource;
import org.neo4j.graphdb.ResourceIterator;
import org.neo4j.helpers.collection.Iterators;
import org.neo4j.kernel.api.exceptions.index.IndexNotFoundKernelException;
import org.neo4j.kernel.api.labelscan.LabelScanStore;
import org.neo4j.kernel.impl.api.ExplicitIndexProviderLookup;
import org.neo4j.kernel.impl.api.index.IndexingService;
import org.neo4j.kernel.impl.store.format.RecordFormat;
import org.neo4j.kernel.impl.util.MultiResource;
import org.neo4j.kernel.spi.explicitindex.IndexImplementation;
import org.neo4j.storageengine.api.StoreFileMetadata;

import static org.neo4j.helpers.collection.Iterators.resourceIterator;

public class NeoStoreFileIndexListing
{
    private final LabelScanStore labelScanStore;
    private final IndexingService indexingService;
    private final ExplicitIndexProviderLookup explicitIndexProviders;

    private static final Function<File,StoreFileMetadata> toStoreFileMetatadata = file -> new StoreFileMetadata( file, RecordFormat.NO_RECORD_SIZE );

    NeoStoreFileIndexListing( LabelScanStore labelScanStore, IndexingService indexingService, ExplicitIndexProviderLookup explicitIndexProviders )
    {
        this.labelScanStore = labelScanStore;
        this.indexingService = indexingService;
        this.explicitIndexProviders = explicitIndexProviders;
    }

    public PrimitiveLongSet getIndexIds()
    {
        return indexingService.getIndexIds();
    }

    Resource gatherSchemaIndexFiles( Collection<StoreFileMetadata> targetFiles ) throws IOException
    {
        ResourceIterator<File> snapshot = indexingService.snapshotIndexFiles();
        getSnapshotFilesMetadata( snapshot, targetFiles );
        // Intentionally don't close the snapshot here, return it for closing by the consumer of
        // the targetFiles list.
        return snapshot;
    }

    Resource gatherLabelScanStoreFiles( Collection<StoreFileMetadata> targetFiles )
    {
        ResourceIterator<File> snapshot = labelScanStore.snapshotStoreFiles();
        getSnapshotFilesMetadata( snapshot, targetFiles );
        // Intentionally don't close the snapshot here, return it for closing by the consumer of
        // the targetFiles list.
        return snapshot;
    }

    Resource gatherExplicitIndexFiles( Collection<StoreFileMetadata> files ) throws IOException
    {
        final Collection<ResourceIterator<File>> snapshots = new ArrayList<>();
        for ( IndexImplementation indexProvider : explicitIndexProviders.all() )
        {
            ResourceIterator<File> snapshot = indexProvider.listStoreFiles();
            snapshots.add( snapshot );
            getSnapshotFilesMetadata( snapshot, files );
        }
        // Intentionally don't close the snapshot here, return it for closing by the consumer of
        // the targetFiles list.
        return new MultiResource( snapshots );
    }

    private void getSnapshotFilesMetadata( ResourceIterator<File> snapshot, Collection<StoreFileMetadata> targetFiles )
    {
        snapshot.stream().map( toStoreFileMetatadata ).forEach( targetFiles::add );
    }

    public ResourceIterator<StoreFileMetadata> getSnapshot( long indexId ) throws IOException
    {
        try
        {
            ResourceIterator<File> snapshot = indexingService.getIndexProxy( indexId ).snapshotFiles();
            ArrayList<StoreFileMetadata> files = new ArrayList<>();
            getSnapshotFilesMetadata( snapshot, files );
            return resourceIterator( files.iterator(), snapshot );
        }
        catch ( IndexNotFoundKernelException e )
        {
            // Perhaps it got dropped after getIndexIds() was called.
            return Iterators.emptyResourceIterator();
        }
    }
}
