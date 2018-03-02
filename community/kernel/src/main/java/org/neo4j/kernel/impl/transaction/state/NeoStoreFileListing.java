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
package org.neo4j.kernel.impl.transaction.state;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.function.Function;
import java.util.stream.Collectors;

import org.neo4j.graphdb.Resource;
import org.neo4j.graphdb.ResourceIterator;
import org.neo4j.kernel.api.labelscan.LabelScanStore;
import org.neo4j.kernel.impl.api.ExplicitIndexProviderLookup;
import org.neo4j.kernel.impl.api.index.IndexingService;
import org.neo4j.kernel.impl.index.IndexConfigStore;
import org.neo4j.kernel.impl.store.StoreType;
import org.neo4j.kernel.impl.store.format.RecordFormat;
import org.neo4j.kernel.impl.transaction.log.files.LogFiles;
import org.neo4j.kernel.spi.explicitindex.IndexImplementation;
import org.neo4j.storageengine.api.StorageEngine;
import org.neo4j.storageengine.api.StoreFileMetadata;

import static org.neo4j.helpers.collection.Iterators.resourceIterator;

public class NeoStoreFileListing
{
    private final File storeDir;
    private final LogFiles logFiles;
    private final LabelScanStore labelScanStore;
    private final IndexingService indexingService;
    private final ExplicitIndexProviderLookup explicitIndexProviders;
    private final StorageEngine storageEngine;
    private static final Function<File,StoreFileMetadata> toNotAStoreTypeFile =
            file -> new StoreFileMetadata( file, RecordFormat.NO_RECORD_SIZE );
    private static final Function<File, StoreFileMetadata> logFileMapper =
            file -> new StoreFileMetadata( file, RecordFormat.NO_RECORD_SIZE, true );
    private final Collection<StoreFileProvider> additionalProviders;

    public NeoStoreFileListing( File storeDir, LogFiles logFiles,
            LabelScanStore labelScanStore, IndexingService indexingService,
            ExplicitIndexProviderLookup explicitIndexProviders, StorageEngine storageEngine )
    {
        this.storeDir = storeDir;
        this.logFiles = logFiles;
        this.labelScanStore = labelScanStore;
        this.indexingService = indexingService;
        this.explicitIndexProviders = explicitIndexProviders;
        this.storageEngine = storageEngine;
        this.additionalProviders = new CopyOnWriteArrayList<>();
    }

    public ResourceIterator<StoreFileMetadata> listStoreFiles( boolean includeLogs ) throws IOException
    {
        List<StoreFileMetadata> files = new ArrayList<>();
        gatherNonRecordStores( files, includeLogs );
        gatherNeoStoreFiles( files );
        List<Resource> additionalResources = new ArrayList<>();
        additionalResources.add( gatherLabelScanStoreFiles( files ) );
        additionalResources.add( gatherSchemaIndexFiles( files ) );
        additionalResources.add( gatherExplicitIndexFiles( files ) );
        for ( StoreFileProvider additionalProvider : additionalProviders )
        {
            additionalResources.add( additionalProvider.addFilesTo( files ) );
        }

        placeMetaDataStoreLast( files );

        return resourceIterator( files.iterator(), new MultiResource( additionalResources ) );
    }

    public void registerStoreFileProvider( StoreFileProvider provider )
    {
        additionalProviders.add( provider );
    }

    public interface StoreFileProvider
    {
        /**
         * @param fileMetadataCollection the collection to add the files to
         * @return A {@link Resource} that should be closed when we are done working with the files added to the collection
         * @throws IOException if the provider is unable to prepare the file listing
         */
        Resource addFilesTo( Collection<StoreFileMetadata> fileMetadataCollection ) throws IOException;
    }

    private void placeMetaDataStoreLast( List<StoreFileMetadata> files )
    {
        int index = 0;
        for ( StoreFileMetadata file : files )
        {
            Optional<StoreType> storeType = StoreType.typeOf( file.file().getName() );
            if ( storeType.isPresent() && storeType.get().equals( StoreType.META_DATA ) )
            {
                break;
            }
            index++;
        }
        if ( index < files.size() - 1 )
        {
            StoreFileMetadata metaDataStoreFile = files.remove( index );
            files.add( metaDataStoreFile );
        }
    }

    private void gatherNonRecordStores( Collection<StoreFileMetadata> files, boolean includeLogs )
    {
        File[] indexFiles = storeDir.listFiles( ( dir, name ) -> name.equals( IndexConfigStore.INDEX_DB_FILE_NAME ) );
        if ( indexFiles != null )
        {
            for ( File file : indexFiles )
            {
                files.add( toNotAStoreTypeFile.apply( file ) );
            }
        }
        if ( includeLogs )
        {
            File[] logFiles = this.logFiles.logFiles();
            for ( File logFile : logFiles )
            {
                files.add( logFileMapper.apply( logFile ) );
            }
        }
    }

    private Resource gatherExplicitIndexFiles( Collection<StoreFileMetadata> files ) throws IOException
    {
        final Collection<ResourceIterator<File>> snapshots = new ArrayList<>();
        for ( IndexImplementation indexProvider : explicitIndexProviders.all() )
        {
            ResourceIterator<File> snapshot = indexProvider.listStoreFiles();
            snapshots.add( snapshot );
            files.addAll( getSnapshotFilesMetadata( snapshot ) );
        }
        // Intentionally don't close the snapshot here, return it for closing by the consumer of
        // the targetFiles list.
        return new MultiResource( snapshots );
    }

    private Resource gatherSchemaIndexFiles( Collection<StoreFileMetadata> targetFiles ) throws IOException
    {
        ResourceIterator<File> snapshot = indexingService.snapshotStoreFiles();
        targetFiles.addAll( getSnapshotFilesMetadata( snapshot ) );
        // Intentionally don't close the snapshot here, return it for closing by the consumer of
        // the targetFiles list.
        return snapshot;
    }

    private Resource gatherLabelScanStoreFiles( Collection<StoreFileMetadata> targetFiles )
    {
        ResourceIterator<File> snapshot = labelScanStore.snapshotStoreFiles();
        targetFiles.addAll( getSnapshotFilesMetadata( snapshot ) );
        // Intentionally don't close the snapshot here, return it for closing by the consumer of
        // the targetFiles list.
        return snapshot;
    }

    public static List<StoreFileMetadata> getSnapshotFilesMetadata( ResourceIterator<File> snapshot )
    {
        return snapshot.stream().map( toNotAStoreTypeFile ).collect( Collectors.toList() );
    }

    private void gatherNeoStoreFiles( final Collection<StoreFileMetadata> targetFiles )
    {
        targetFiles.addAll( storageEngine.listStorageFiles() );
    }

    public static final class MultiResource implements Resource
    {
        private final Collection<? extends Resource> snapshots;

        public MultiResource( Collection<? extends Resource> resources )
        {
            this.snapshots = resources;
        }

        @Override
        public void close()
        {
            RuntimeException exception = null;
            for ( Resource snapshot : snapshots )
            {
                try
                {
                    snapshot.close();
                }
                catch ( RuntimeException e )
                {
                    exception = e;
                }
            }
            if ( exception != null )
            {
                throw exception;
            }
        }
    }
}
