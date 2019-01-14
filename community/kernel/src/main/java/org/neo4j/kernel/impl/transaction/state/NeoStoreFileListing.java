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
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CopyOnWriteArraySet;
import java.util.function.Function;
import java.util.stream.Collectors;

import org.neo4j.graphdb.Resource;
import org.neo4j.graphdb.ResourceIterator;
import org.neo4j.helpers.Exceptions;
import org.neo4j.io.IOUtils;
import org.neo4j.kernel.api.labelscan.LabelScanStore;
import org.neo4j.kernel.impl.api.ExplicitIndexProviderLookup;
import org.neo4j.kernel.impl.api.index.IndexingService;
import org.neo4j.kernel.impl.index.IndexConfigStore;
import org.neo4j.kernel.impl.store.StoreType;
import org.neo4j.kernel.impl.store.format.RecordFormat;
import org.neo4j.kernel.impl.transaction.log.files.LogFiles;
import org.neo4j.kernel.impl.util.MultiResource;
import org.neo4j.storageengine.api.StorageEngine;
import org.neo4j.storageengine.api.StoreFileMetadata;

import static org.neo4j.helpers.collection.Iterators.resourceIterator;

public class NeoStoreFileListing
{
    private final File storeDir;
    private final LogFiles logFiles;
    private final StorageEngine storageEngine;
    private static final Function<File,StoreFileMetadata> toNotAStoreTypeFile =
            file -> new StoreFileMetadata( file, RecordFormat.NO_RECORD_SIZE );
    private static final Function<File, StoreFileMetadata> logFileMapper =
            file -> new StoreFileMetadata( file, RecordFormat.NO_RECORD_SIZE, true );
    private final NeoStoreFileIndexListing neoStoreFileIndexListing;
    private final Collection<StoreFileProvider> additionalProviders;

    public NeoStoreFileListing( File storeDir, LogFiles logFiles,
            LabelScanStore labelScanStore, IndexingService indexingService,
            ExplicitIndexProviderLookup explicitIndexProviders, StorageEngine storageEngine )
    {
        this.storeDir = storeDir;
        this.logFiles = logFiles;
        this.storageEngine = storageEngine;
        this.neoStoreFileIndexListing = new NeoStoreFileIndexListing( labelScanStore, indexingService, explicitIndexProviders );
        this.additionalProviders = new CopyOnWriteArraySet<>();
    }

    public StoreFileListingBuilder builder()
    {
        return new StoreFileListingBuilder();
    }

    public NeoStoreFileIndexListing getNeoStoreFileIndexListing()
    {
        return neoStoreFileIndexListing;
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

    public class StoreFileListingBuilder
    {
        private boolean excludeLogFiles;
        private boolean excludeNonRecordStoreFiles;
        private boolean excludeNeoStoreFiles;
        private boolean excludeLabelScanStoreFiles;
        private boolean excludeSchemaIndexStoreFiles;
        private boolean excludeExplicitIndexStoreFiles;
        private boolean excludeAdditionalProviders;

        private StoreFileListingBuilder()
        {
        }

        private void excludeAll( boolean initiateInclusive )
        {
            this.excludeLogFiles =
            this.excludeNonRecordStoreFiles =
            this.excludeNeoStoreFiles =
            this.excludeLabelScanStoreFiles =
                    this.excludeSchemaIndexStoreFiles = this.excludeAdditionalProviders = this.excludeExplicitIndexStoreFiles = initiateInclusive;
        }

        public StoreFileListingBuilder excludeAll()
        {
            excludeAll( true );
            return this;
        }

        public StoreFileListingBuilder includeAll()
        {
            excludeAll( false );
            return this;
        }

        public StoreFileListingBuilder excludeLogFiles()
        {
            excludeLogFiles = true;
            return this;
        }

        public StoreFileListingBuilder excludeNonRecordStoreFiles()
        {
            excludeNonRecordStoreFiles = true;
            return this;
        }

        public StoreFileListingBuilder excludeNeoStoreFiles()
        {
            excludeNeoStoreFiles = true;
            return this;
        }

        public StoreFileListingBuilder excludeLabelScanStoreFiles()
        {
            excludeLabelScanStoreFiles = true;
            return this;
        }

        public StoreFileListingBuilder excludeSchemaIndexStoreFiles()
        {
            excludeSchemaIndexStoreFiles = true;
            return this;
        }

        public StoreFileListingBuilder excludeExplicitIndexStoreFiles()
        {
            excludeExplicitIndexStoreFiles = true;
            return this;
        }

        public StoreFileListingBuilder excludeAdditionalProviders()
        {
            excludeAdditionalProviders = true;
            return this;
        }

        public StoreFileListingBuilder includeLogFiles()
        {
            excludeLogFiles = false;
            return this;
        }

        public StoreFileListingBuilder includeNonRecordStoreFiles()
        {
            excludeNonRecordStoreFiles = false;
            return this;
        }

        public StoreFileListingBuilder includeNeoStoreFiles()
        {
            excludeNeoStoreFiles = false;
            return this;
        }

        public StoreFileListingBuilder includeLabelScanStoreFiles()
        {
            excludeLabelScanStoreFiles = false;
            return this;
        }

        public StoreFileListingBuilder includeSchemaIndexStoreFiles()
        {
            excludeSchemaIndexStoreFiles = false;
            return this;
        }

        public StoreFileListingBuilder includeExplicitIndexStoreStoreFiles()
        {
            excludeExplicitIndexStoreFiles = false;
            return this;
        }

        public StoreFileListingBuilder includeAdditionalProviders()
        {
            excludeAdditionalProviders = false;
            return this;
        }

        public ResourceIterator<StoreFileMetadata> build() throws IOException
        {
            List<StoreFileMetadata> files = new ArrayList<>();
            List<Resource> resources = new ArrayList<>();
            try
            {
                if ( !excludeNonRecordStoreFiles )
                {
                    gatherNonRecordStores( files, !excludeLogFiles );
                }
                if ( !excludeNeoStoreFiles )
                {
                    gatherNeoStoreFiles( files );
                }
                if ( !excludeLabelScanStoreFiles )
                {
                    resources.add( neoStoreFileIndexListing.gatherLabelScanStoreFiles( files ) );
                }
                if ( !excludeSchemaIndexStoreFiles )
                {
                    resources.add( neoStoreFileIndexListing.gatherSchemaIndexFiles( files ) );
                }
                if ( !excludeExplicitIndexStoreFiles )
                {
                    resources.add( neoStoreFileIndexListing.gatherExplicitIndexFiles( files ) );
                }
                if ( !excludeAdditionalProviders )
                {
                    for ( StoreFileProvider additionalProvider : additionalProviders )
                    {
                        resources.add( additionalProvider.addFilesTo( files ) );
                    }
                }

                placeMetaDataStoreLast( files );
            }
            catch ( IOException e )
            {
                try
                {
                    IOUtils.closeAll( resources );
                }
                catch ( IOException e1 )
                {
                    e = Exceptions.chain( e, e1 );
                }
                throw e;
            }

            return resourceIterator( files.iterator(), new MultiResource( resources ) );
        }
    }

    public static List<StoreFileMetadata> getSnapshotFilesMetadata( ResourceIterator<File> snapshot )
    {
        return snapshot.stream().map( toNotAStoreTypeFile ).collect( Collectors.toList() );
    }

    private void gatherNeoStoreFiles( final Collection<StoreFileMetadata> targetFiles )
    {
        targetFiles.addAll( storageEngine.listStorageFiles() );
    }
}
