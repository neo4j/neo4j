/*
 * Copyright (c) "Neo4j"
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
package org.neo4j.kernel.api.impl.schema;

import org.apache.commons.lang3.StringUtils;

import java.io.IOException;

import org.neo4j.common.TokenNameLookup;
import org.neo4j.configuration.Config;
import org.neo4j.configuration.helpers.DatabaseReadOnlyChecker;
import org.neo4j.internal.kernel.api.InternalIndexState;
import org.neo4j.internal.schema.IndexDescriptor;
import org.neo4j.internal.schema.IndexPrototype;
import org.neo4j.internal.schema.IndexProviderDescriptor;
import org.neo4j.internal.schema.IndexType;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.memory.ByteBufferFactory;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.io.pagecache.context.CursorContext;
import org.neo4j.kernel.api.impl.index.DroppableIndex;
import org.neo4j.kernel.api.impl.index.DroppableLuceneIndex;
import org.neo4j.kernel.api.impl.index.IndexWriterConfigs;
import org.neo4j.kernel.api.impl.index.LuceneMinimalIndexAccessor;
import org.neo4j.kernel.api.impl.index.partition.ReadOnlyIndexPartitionFactory;
import org.neo4j.kernel.api.impl.index.storage.DirectoryFactory;
import org.neo4j.kernel.api.impl.index.storage.IndexStorageFactory;
import org.neo4j.kernel.api.impl.index.storage.PartitionedIndexStorage;
import org.neo4j.kernel.api.impl.schema.populator.NonUniqueLuceneIndexPopulator;
import org.neo4j.kernel.api.impl.schema.populator.UniqueLuceneIndexPopulator;
import org.neo4j.kernel.api.index.IndexAccessor;
import org.neo4j.kernel.api.index.IndexDirectoryStructure;
import org.neo4j.kernel.api.index.IndexPopulator;
import org.neo4j.kernel.api.index.IndexProvider;
import org.neo4j.kernel.api.index.MinimalIndexAccessor;
import org.neo4j.kernel.api.index.ValueIndexReader;
import org.neo4j.kernel.impl.api.index.IndexSamplingConfig;
import org.neo4j.memory.MemoryTracker;
import org.neo4j.monitoring.Monitors;
import org.neo4j.storageengine.api.StorageEngineFactory;
import org.neo4j.storageengine.migration.SchemaIndexMigrator;
import org.neo4j.storageengine.migration.StoreMigrationParticipant;
import org.neo4j.util.VisibleForTesting;

import static org.apache.commons.lang3.StringUtils.defaultIfEmpty;

public abstract class AbstractLuceneIndexProvider extends IndexProvider
{
    private final IndexStorageFactory indexStorageFactory;
    private final Config config;
    private final DatabaseReadOnlyChecker readOnlyChecker;
    private final FileSystemAbstraction fileSystem;
    private final Monitor monitor;
    private final IndexType supportedIndexType;

    public AbstractLuceneIndexProvider(
            IndexType supportedIndexType, IndexProviderDescriptor descriptor,
            FileSystemAbstraction fileSystem, DirectoryFactory directoryFactory,
            IndexDirectoryStructure.Factory directoryStructureFactory, Monitors monitors, Config config, DatabaseReadOnlyChecker readOnlyChecker )
    {
        super( descriptor, directoryStructureFactory );
        this.supportedIndexType = supportedIndexType;
        this.monitor = monitors.newMonitor( Monitor.class, descriptor.toString() );
        this.indexStorageFactory = buildIndexStorageFactory( fileSystem, directoryFactory );
        this.fileSystem = fileSystem;
        this.config = config;
        this.readOnlyChecker = readOnlyChecker;
    }

    @VisibleForTesting
    protected IndexStorageFactory buildIndexStorageFactory( FileSystemAbstraction fileSystem, DirectoryFactory directoryFactory )
    {
        return new IndexStorageFactory( directoryFactory, fileSystem, directoryStructure() );
    }

    @Override
    public void validatePrototype( IndexPrototype prototype )
    {
        IndexType indexType = prototype.getIndexType();
        if ( indexType != supportedIndexType )
        {
            String providerName = getProviderDescriptor().name();
            throw new IllegalArgumentException( "The '" + providerName + "' index provider does not support " + indexType + " indexes: " + prototype );
        }
    }

    @Override
    public MinimalIndexAccessor getMinimalIndexAccessor( IndexDescriptor descriptor )
    {
        PartitionedIndexStorage indexStorage = indexStorageFactory.indexStorageOf( descriptor.getId() );
        DroppableIndex<ValueIndexReader> index =
                new DroppableIndex<>( new DroppableLuceneIndex<>( indexStorage, new ReadOnlyIndexPartitionFactory(), descriptor ) );
        return new LuceneMinimalIndexAccessor<>( descriptor, index, true );
    }

    @Override
    public IndexPopulator getPopulator( IndexDescriptor descriptor, IndexSamplingConfig samplingConfig, ByteBufferFactory bufferFactory,
                                        MemoryTracker memoryTracker, TokenNameLookup tokenNameLookup )
    {
        SchemaIndex luceneIndex = LuceneSchemaIndexBuilder
                .create( descriptor, readOnlyChecker, config )
                .withFileSystem( fileSystem )
                .withSamplingConfig( samplingConfig )
                .withIndexStorage( getIndexStorage( descriptor.getId() ) )
                .withWriterConfig( () -> IndexWriterConfigs.population( config ) )
                .build();

        if ( luceneIndex.isReadOnly() )
        {
            throw new UnsupportedOperationException( "Can't create populator for read only index" );
        }
        return descriptor.isUnique()
               ? new UniqueLuceneIndexPopulator( luceneIndex, descriptor )
               : new NonUniqueLuceneIndexPopulator( luceneIndex, samplingConfig );
    }

    @Override
    public IndexAccessor getOnlineAccessor( IndexDescriptor descriptor, IndexSamplingConfig samplingConfig, TokenNameLookup tokenNameLookup ) throws IOException
    {
        SchemaIndex luceneIndex = LuceneSchemaIndexBuilder.create( descriptor, readOnlyChecker, config )
                                                          .withSamplingConfig( samplingConfig )
                                                          .withIndexStorage( getIndexStorage( descriptor.getId() ) )
                                                          .build();
        luceneIndex.open();
        return new LuceneIndexAccessor( luceneIndex, descriptor, tokenNameLookup );
    }

    @Override
    public InternalIndexState getInitialState( IndexDescriptor descriptor, CursorContext cursorContext )
    {
        PartitionedIndexStorage indexStorage = getIndexStorage( descriptor.getId() );
        String failure = indexStorage.getStoredIndexFailure();
        if ( failure != null )
        {
            return InternalIndexState.FAILED;
        }
        try
        {
            return indexIsOnline( indexStorage, descriptor ) ? InternalIndexState.ONLINE : InternalIndexState.POPULATING;
        }
        catch ( IOException e )
        {
            monitor.failedToOpenIndex( descriptor, "Requesting re-population.", e );
            return InternalIndexState.POPULATING;
        }
    }

    @Override
    public StoreMigrationParticipant storeMigrationParticipant( final FileSystemAbstraction fs, PageCache pageCache, StorageEngineFactory storageEngineFactory )
    {
        return new SchemaIndexMigrator(
                getProviderDescriptor().name(), fs, pageCache, this.directoryStructure(), storageEngineFactory, true );
    }

    @Override
    public String getPopulationFailure( IndexDescriptor descriptor, CursorContext cursorContext ) throws IllegalStateException
    {
        return defaultIfEmpty( getIndexStorage( descriptor.getId() ).getStoredIndexFailure(), StringUtils.EMPTY );
    }

    private PartitionedIndexStorage getIndexStorage( long indexId )
    {
        return indexStorageFactory.indexStorageOf( indexId );
    }

    private boolean indexIsOnline( PartitionedIndexStorage indexStorage, IndexDescriptor descriptor ) throws IOException
    {
        try ( SchemaIndex index = LuceneSchemaIndexBuilder.create( descriptor, readOnlyChecker, config ).withIndexStorage( indexStorage ).build() )
        {
            if ( index.exists() )
            {
                index.open();
                return index.isOnline();
            }
            return false;
        }
    }

    @Override
    public IndexDescriptor completeConfiguration( IndexDescriptor index )
    {
        return index;
    }

}
