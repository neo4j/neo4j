/*
 * Copyright (c) 2002-2020 "Neo4j,"
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

import java.io.IOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.CopyOnWriteArraySet;
import java.util.function.Function;
import java.util.stream.Collectors;

import org.neo4j.graphdb.Resource;
import org.neo4j.graphdb.ResourceIterator;
import org.neo4j.internal.helpers.Exceptions;
import org.neo4j.internal.id.IdGeneratorFactory;
import org.neo4j.io.IOUtils;
import org.neo4j.io.layout.DatabaseLayout;
import org.neo4j.kernel.impl.api.index.IndexingService;
import org.neo4j.kernel.impl.index.schema.LabelScanStore;
import org.neo4j.kernel.impl.index.schema.RelationshipTypeScanStore;
import org.neo4j.kernel.impl.transaction.log.files.LogFiles;
import org.neo4j.kernel.impl.util.MultiResource;
import org.neo4j.storageengine.api.StorageEngine;
import org.neo4j.storageengine.api.StoreFileMetadata;

import static org.neo4j.internal.helpers.collection.Iterators.resourceIterator;

public class DatabaseFileListing
{
    private final DatabaseLayout databaseLayout;
    private final LogFiles logFiles;
    private final StorageEngine storageEngine;
    private final IdGeneratorFactory idGeneratorFactory;
    private static final Function<Path,StoreFileMetadata> logFileMapper = path -> new StoreFileMetadata( path, 1, true );
    private final SchemaAndIndexingFileIndexListing fileIndexListing;
    private final Collection<StoreFileProvider> additionalProviders;

    public DatabaseFileListing( DatabaseLayout databaseLayout, LogFiles logFiles,
            LabelScanStore labelScanStore, RelationshipTypeScanStore relationshipTypeScanStore, IndexingService indexingService,
            StorageEngine storageEngine,
            IdGeneratorFactory idGeneratorFactory )
    {
        this.databaseLayout = databaseLayout;
        this.logFiles = logFiles;
        this.storageEngine = storageEngine;
        this.idGeneratorFactory = idGeneratorFactory;
        this.fileIndexListing = new SchemaAndIndexingFileIndexListing( labelScanStore, relationshipTypeScanStore, indexingService );
        this.additionalProviders = new CopyOnWriteArraySet<>();
    }

    public StoreFileListingBuilder builder()
    {
        return new StoreFileListingBuilder();
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
            if ( databaseLayout.metadataStore().equals( file.path() ) )
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

    private void gatherLogFiles( Collection<StoreFileMetadata> files )
    {
        Path[] list = this.logFiles.logFiles();
        for ( Path logFile : list )
        {
            files.add( logFileMapper.apply( logFile ) );
        }
    }

    public class StoreFileListingBuilder
    {
        private boolean excludeLogFiles;
        private boolean excludeNeoStoreFiles;
        private boolean excludeLabelScanStoreFiles;
        private boolean excludeRelationshipTypeScanStoreFiles;
        private boolean excludeSchemaIndexStoreFiles;
        private boolean excludeAdditionalProviders;
        private boolean excludeIdFiles;

        private StoreFileListingBuilder()
        {
        }

        private void excludeAll( boolean initiateInclusive )
        {
            this.excludeLogFiles = initiateInclusive;
            this.excludeNeoStoreFiles = initiateInclusive;
            this.excludeLabelScanStoreFiles = initiateInclusive;
            this.excludeRelationshipTypeScanStoreFiles = initiateInclusive;
            this.excludeSchemaIndexStoreFiles = initiateInclusive;
            this.excludeAdditionalProviders = initiateInclusive;
            this.excludeIdFiles = initiateInclusive;
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

        public StoreFileListingBuilder excludeRelationshipTypeScanStoreFiles()
        {
            excludeRelationshipTypeScanStoreFiles = true;
            return this;
        }

        public StoreFileListingBuilder excludeSchemaIndexStoreFiles()
        {
            excludeSchemaIndexStoreFiles = true;
            return this;
        }

        public StoreFileListingBuilder excludeAdditionalProviders()
        {
            excludeAdditionalProviders = true;
            return this;
        }

        public StoreFileListingBuilder excludeIdFiles()
        {
            excludeIdFiles = true;
            return this;
        }

        public StoreFileListingBuilder includeLogFiles()
        {
            excludeLogFiles = false;
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

        public StoreFileListingBuilder includeRelationshipTypeScanStoreFiles()
        {
            excludeRelationshipTypeScanStoreFiles = false;
            return this;
        }

        public StoreFileListingBuilder includeSchemaIndexStoreFiles()
        {
            excludeSchemaIndexStoreFiles = false;
            return this;
        }

        public StoreFileListingBuilder includeAdditionalProviders()
        {
            excludeAdditionalProviders = false;
            return this;
        }

        public StoreFileListingBuilder includeIdFiles()
        {
            excludeIdFiles = false;
            return this;
        }

        public ResourceIterator<StoreFileMetadata> build() throws IOException
        {
            List<StoreFileMetadata> files = new ArrayList<>();
            List<Resource> resources = new ArrayList<>();
            try
            {
                if ( !excludeLogFiles )
                {
                    gatherLogFiles( files );
                }
                if ( !excludeNeoStoreFiles )
                {
                    gatherNeoStoreFiles( files );
                }
                if ( !excludeIdFiles )
                {
                    gatherIdFiles( files );
                }
                if ( !excludeLabelScanStoreFiles )
                {
                    resources.add( fileIndexListing.gatherLabelScanStoreFiles( files ) );
                }
                if ( !excludeRelationshipTypeScanStoreFiles )
                {
                    resources.add( fileIndexListing.gatherRelationshipTypeScanStoreFiles( files ) );
                }
                if ( !excludeSchemaIndexStoreFiles )
                {
                    resources.add( fileIndexListing.gatherSchemaIndexFiles( files ) );
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

    private void gatherIdFiles( List<StoreFileMetadata> targetFiles )
    {
        targetFiles.addAll( idGeneratorFactory.listIdFiles().stream().map( file -> new StoreFileMetadata( file, 0 ) ).collect( Collectors.toList() ) );
    }

    private void gatherNeoStoreFiles( final Collection<StoreFileMetadata> targetFiles )
    {
        targetFiles.addAll( storageEngine.listStorageFiles() );
    }
}
