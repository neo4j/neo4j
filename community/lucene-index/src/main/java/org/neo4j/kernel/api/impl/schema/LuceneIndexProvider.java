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
package org.neo4j.kernel.api.impl.schema;

import org.apache.commons.lang3.StringUtils;

import java.io.IOException;

import org.neo4j.common.TokenNameLookup;
import org.neo4j.configuration.Config;
import org.neo4j.internal.kernel.api.InternalIndexState;
import org.neo4j.internal.schema.IndexDescriptor;
import org.neo4j.internal.schema.IndexProviderDescriptor;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.memory.ByteBufferFactory;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.io.pagecache.tracing.cursor.PageCursorTracer;
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
import org.neo4j.kernel.api.index.IndexReader;
import org.neo4j.kernel.api.index.MinimalIndexAccessor;
import org.neo4j.kernel.impl.api.index.IndexSamplingConfig;
import org.neo4j.memory.MemoryTracker;
import org.neo4j.monitoring.Monitors;
import org.neo4j.storageengine.api.StorageEngineFactory;
import org.neo4j.storageengine.migration.SchemaIndexMigrator;
import org.neo4j.storageengine.migration.StoreMigrationParticipant;

import static org.apache.commons.lang3.StringUtils.defaultIfEmpty;

public class LuceneIndexProvider extends IndexProvider
{
    public static final IndexProviderDescriptor DESCRIPTOR = new IndexProviderDescriptor( "lucene", "2.0" );
    private final IndexStorageFactory indexStorageFactory;
    private final Config config;
    private final boolean isSingleInstance;
    private final FileSystemAbstraction fileSystem;
    private final Monitor monitor;

    public LuceneIndexProvider( FileSystemAbstraction fileSystem, DirectoryFactory directoryFactory,
            IndexDirectoryStructure.Factory directoryStructureFactory, Monitors monitors, Config config,
            boolean isSingleInstance )
    {
        this( fileSystem, directoryFactory, directoryStructureFactory, monitors, DESCRIPTOR.toString(), config, isSingleInstance );
    }

    public LuceneIndexProvider( FileSystemAbstraction fileSystem, DirectoryFactory directoryFactory,
            IndexDirectoryStructure.Factory directoryStructureFactory, Monitors monitors, String monitorTag, Config config,
            boolean isSingleInstance )
    {
        super( DESCRIPTOR, directoryStructureFactory );
        this.monitor = monitors.newMonitor( Monitor.class, monitorTag );
        this.indexStorageFactory = buildIndexStorageFactory( fileSystem, directoryFactory );
        this.fileSystem = fileSystem;
        this.config = config;
        this.isSingleInstance = isSingleInstance;
    }

    /**
     * Visible <b>only</b> for testing.
     */
    protected IndexStorageFactory buildIndexStorageFactory( FileSystemAbstraction fileSystem, DirectoryFactory directoryFactory )
    {
        return new IndexStorageFactory( directoryFactory, fileSystem, directoryStructure() );
    }

    @Override
    public MinimalIndexAccessor getMinimalIndexAccessor( IndexDescriptor descriptor )
    {
        PartitionedIndexStorage indexStorage = indexStorageFactory.indexStorageOf( descriptor.getId() );
        DroppableIndex<IndexReader> index =
                new DroppableIndex<>( new DroppableLuceneIndex<>( indexStorage, new ReadOnlyIndexPartitionFactory(), descriptor ) );
        return new LuceneMinimalIndexAccessor<>( descriptor, index, true );
    }

    @Override
    public IndexPopulator getPopulator( IndexDescriptor descriptor, IndexSamplingConfig samplingConfig, ByteBufferFactory bufferFactory,
            MemoryTracker memoryTracker, TokenNameLookup tokenNameLookup )
    {
        SchemaIndex luceneIndex = LuceneSchemaIndexBuilder.create( descriptor, config )
                                        .withFileSystem( fileSystem )
                                        .withOperationalMode( isSingleInstance )
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
        SchemaIndex luceneIndex = LuceneSchemaIndexBuilder.create( descriptor, config )
                                            .withOperationalMode( isSingleInstance )
                                            .withSamplingConfig( samplingConfig )
                                            .withIndexStorage( getIndexStorage( descriptor.getId() ) )
                                            .build();
        luceneIndex.open();
        return new LuceneIndexAccessor( luceneIndex, descriptor, tokenNameLookup );
    }

    @Override
    public InternalIndexState getInitialState( IndexDescriptor descriptor, PageCursorTracer cursorTracer )
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
        return new SchemaIndexMigrator( "Lucene indexes", fs, this.directoryStructure(), storageEngineFactory );
    }

    @Override
    public String getPopulationFailure( IndexDescriptor descriptor, PageCursorTracer cursorTracer ) throws IllegalStateException
    {
        return defaultIfEmpty( getIndexStorage( descriptor.getId() ).getStoredIndexFailure(), StringUtils.EMPTY );
    }

    private PartitionedIndexStorage getIndexStorage( long indexId )
    {
        return indexStorageFactory.indexStorageOf( indexId );
    }

    private boolean indexIsOnline( PartitionedIndexStorage indexStorage, IndexDescriptor descriptor ) throws IOException
    {
        try ( SchemaIndex index = LuceneSchemaIndexBuilder.create( descriptor, config ).withIndexStorage( indexStorage ).build() )
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
