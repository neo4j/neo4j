/*
 * Copyright (c) 2002-2016 "Neo Technology,"
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
package org.neo4j.unsafe.impl.batchimport.store;

import java.io.File;
import java.io.IOException;
import java.nio.file.OpenOption;

import org.neo4j.graphdb.factory.GraphDatabaseSettings;
import org.neo4j.helpers.Service;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.io.pagecache.tracing.DefaultPageCacheTracer;
import org.neo4j.io.pagecache.tracing.PageCacheTracer;
import org.neo4j.kernel.api.labelscan.LabelScanStore;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.extension.KernelExtensionFactory;
import org.neo4j.kernel.extension.KernelExtensions;
import org.neo4j.kernel.extension.UnsatisfiedDependencyStrategies;
import org.neo4j.kernel.extension.dependency.HighestSelectionStrategy;
import org.neo4j.kernel.impl.api.index.IndexStoreView;
import org.neo4j.kernel.impl.api.scan.LabelScanStoreProvider;
import org.neo4j.kernel.impl.factory.DatabaseInfo;
import org.neo4j.kernel.impl.logging.LogService;
import org.neo4j.kernel.impl.pagecache.ConfiguringPageCacheFactory;
import org.neo4j.kernel.impl.spi.KernelContext;
import org.neo4j.kernel.impl.spi.SimpleKernelContext;
import org.neo4j.kernel.impl.store.NeoStores;
import org.neo4j.kernel.impl.store.NodeStore;
import org.neo4j.kernel.impl.store.PropertyStore;
import org.neo4j.kernel.impl.store.RecordStore;
import org.neo4j.kernel.impl.store.RelationshipStore;
import org.neo4j.kernel.impl.store.StoreFactory;
import org.neo4j.kernel.impl.store.UnderlyingStorageException;
import org.neo4j.kernel.impl.store.counts.CountsTracker;
import org.neo4j.kernel.impl.store.format.RecordFormats;
import org.neo4j.kernel.impl.store.record.RelationshipGroupRecord;
import org.neo4j.kernel.impl.util.Dependencies;
import org.neo4j.kernel.lifecycle.LifeSupport;
import org.neo4j.logging.LogProvider;
import org.neo4j.logging.NullLogProvider;
import org.neo4j.unsafe.impl.batchimport.AdditionalInitialIds;
import org.neo4j.unsafe.impl.batchimport.Configuration;
import org.neo4j.unsafe.impl.batchimport.ParallelBatchImporter;
import org.neo4j.unsafe.impl.batchimport.store.BatchingTokenRepository.BatchingLabelTokenRepository;
import org.neo4j.unsafe.impl.batchimport.store.BatchingTokenRepository.BatchingPropertyKeyTokenRepository;
import org.neo4j.unsafe.impl.batchimport.store.BatchingTokenRepository.BatchingRelationshipTypeTokenRepository;
import org.neo4j.unsafe.impl.batchimport.store.io.IoTracer;

import static java.lang.String.valueOf;
import static java.nio.file.StandardOpenOption.DELETE_ON_CLOSE;
import static org.neo4j.graphdb.factory.GraphDatabaseSettings.dense_node_threshold;
import static org.neo4j.graphdb.factory.GraphDatabaseSettings.mapped_memory_page_size;
import static org.neo4j.graphdb.factory.GraphDatabaseSettings.pagecache_memory;
import static org.neo4j.helpers.collection.MapUtil.stringMap;
import static org.neo4j.kernel.impl.store.MetaDataStore.DEFAULT_NAME;
import static org.neo4j.kernel.impl.store.StoreType.RELATIONSHIP_GROUP;
import static org.neo4j.kernel.impl.transaction.log.TransactionIdStore.BASE_TX_COMMIT_TIMESTAMP;

/**
 * Creator and accessor of {@link NeoStores} with some logic to provide very batch friendly services to the
 * {@link NeoStores} when instantiating it. Different services for specific purposes.
 */
public class BatchingNeoStores implements AutoCloseable
{
    private final FileSystemAbstraction fileSystem;
    private final BatchingPropertyKeyTokenRepository propertyKeyRepository;
    private final BatchingLabelTokenRepository labelRepository;
    private final BatchingRelationshipTypeTokenRepository relationshipTypeRepository;
    private final LogProvider logProvider;
    private final File storeDir;
    private final Config neo4jConfig;
    private final PageCache pageCache;
    private final NeoStores neoStores;
    private final LifeSupport life = new LifeSupport();
    private final LabelScanStore labelScanStore;
    private final IoTracer ioTracer;
    private final RecordFormats recordFormats;

    // Some stores are considered temporary during the import and will be reordered/restructured
    // into the main store. These temporary stores will live here
    private final NeoStores temporaryNeoStores;

    public BatchingNeoStores( FileSystemAbstraction fileSystem, File storeDir, RecordFormats recordFormats,
            Configuration config, LogService logService, AdditionalInitialIds initialIds, Config dbConfig )
    {
        this.fileSystem = fileSystem;
        this.recordFormats = recordFormats;
        this.logProvider = logService.getInternalLogProvider();
        this.storeDir = storeDir;
        long mappedMemory = config.pageCacheMemory();
        // 30 is the minimum number of pages the page cache wants to keep free at all times.
        // Having less than that might result in an evicted page will reading, which would mean
        // unnecessary re-reading. Having slightly more leaves some leg room.
        int pageSize = config.pageSize();
        this.neo4jConfig = new Config( stringMap( dbConfig.getParams(),
                dense_node_threshold.name(), valueOf( config.denseNodeThreshold() ),
                pagecache_memory.name(), valueOf( mappedMemory ),
                mapped_memory_page_size.name(), valueOf( pageSize ) ),
                GraphDatabaseSettings.class );
        final PageCacheTracer tracer = new DefaultPageCacheTracer();
        this.pageCache = createPageCache( fileSystem, neo4jConfig, logProvider, tracer );
        this.ioTracer = tracer::bytesWritten;
        this.neoStores = newStoreFactory( DEFAULT_NAME ).openAllNeoStores( true );
        if ( alreadyContainsData( neoStores ) )
        {
            neoStores.close();
            IllegalStateException ise =
                    new IllegalStateException( storeDir + " already contains data, cannot do import here" );
            try
            {
                pageCache.close();
            }
            catch ( Exception e )
            {
                // Oddly enough we can't close the page cache, how to communicate this? Here we add as suppressed
                ise.addSuppressed( e );
            }
            throw ise;
        }
        try
        {
            neoStores.rebuildCountStoreIfNeeded();
        }
        catch ( IOException e )
        {
            throw new UnderlyingStorageException( e );
        }
        neoStores.getMetaDataStore().setLastCommittedAndClosedTransactionId(
                initialIds.lastCommittedTransactionId(), initialIds.lastCommittedTransactionChecksum(),
                BASE_TX_COMMIT_TIMESTAMP, initialIds.lastCommittedTransactionLogByteOffset(),
                initialIds.lastCommittedTransactionLogVersion() );
        this.propertyKeyRepository = new BatchingPropertyKeyTokenRepository(
                neoStores.getPropertyKeyTokenStore() );
        this.labelRepository = new BatchingLabelTokenRepository(
                neoStores.getLabelTokenStore() );
        this.relationshipTypeRepository = new BatchingRelationshipTypeTokenRepository(
                neoStores.getRelationshipTypeTokenStore() );

        // Instantiate the temporary stores
        temporaryNeoStores = newStoreFactory(
                "temp." + DEFAULT_NAME, DELETE_ON_CLOSE ).openNeoStores( true, RELATIONSHIP_GROUP );

        // Initialize kernel extensions
        Dependencies dependencies = new Dependencies();
        dependencies.satisfyDependency( neo4jConfig );
        dependencies.satisfyDependency( fileSystem );
        dependencies.satisfyDependency( this );
        dependencies.satisfyDependency( logService );
        dependencies.satisfyDependency( IndexStoreView.EMPTY );
        KernelContext kernelContext = new SimpleKernelContext( fileSystem, storeDir, DatabaseInfo.UNKNOWN,
                dependencies );
        @SuppressWarnings( { "unchecked", "rawtypes" } )
        KernelExtensions extensions = life.add( new KernelExtensions(
                kernelContext, (Iterable) Service.load( KernelExtensionFactory.class ),
                dependencies, UnsatisfiedDependencyStrategies.ignore() ) );
        life.start();
        labelScanStore = life.add( extensions.resolveDependency( LabelScanStoreProvider.class,
                HighestSelectionStrategy.getInstance() ).getLabelScanStore() );
    }

    private static PageCache createPageCache( FileSystemAbstraction fileSystem, Config config, LogProvider log,
            PageCacheTracer tracer )
    {
        return new ConfiguringPageCacheFactory( fileSystem, config, tracer,
                log.getLog( BatchingNeoStores.class ) ).getOrCreatePageCache();
    }

    private boolean alreadyContainsData( NeoStores neoStores )
    {
        return neoStores.getNodeStore().getHighId() > 0 || neoStores.getRelationshipStore().getHighId() > 0;
    }

    /**
     * A way to create the underlying {@link NeoStores} files in the {@link FileSystemAbstraction file system}
     * before instantiating the real one. This allows some store contents to be populated before an import.
     * Useful for store migration where the {@link ParallelBatchImporter} is used as migrator and some of
     * its data need to be communicated by copying a store file.
     */
    public static void createStore( FileSystemAbstraction fileSystem, String storeDir, Config dbConfig,
            RecordFormats newFormat )
            throws IOException
    {
        try ( PageCache pageCache = createPageCache( fileSystem, dbConfig, NullLogProvider.getInstance(),
                PageCacheTracer.NULL ) )
        {
            StoreFactory storeFactory = new StoreFactory( new File( storeDir ), pageCache, fileSystem, newFormat,
                            NullLogProvider.getInstance() );
            try ( NeoStores neoStores = storeFactory.openAllNeoStores( true ) )
            {
                neoStores.getMetaDataStore();
                neoStores.getLabelTokenStore();
                neoStores.getNodeStore();
                neoStores.getPropertyStore();
                neoStores.getRelationshipGroupStore();
                neoStores.getRelationshipStore();
                neoStores.getSchemaStore();
            }
        }
    }

    private StoreFactory newStoreFactory( String name, OpenOption... openOptions )
    {
        return new StoreFactory( storeDir, name, neo4jConfig,
                new BatchingIdGeneratorFactory( fileSystem ), pageCache, fileSystem, recordFormats, logProvider,
                openOptions );
    }

    /**
     * @return temporary relationship group store which will be deleted in {@link #close()}.
     */
    public RecordStore<RelationshipGroupRecord> getTemporaryRelationshipGroupStore()
    {
        return temporaryNeoStores.getRelationshipGroupStore();
    }

    public IoTracer getIoTracer()
    {
        return ioTracer;
    }

    public NodeStore getNodeStore()
    {
        return neoStores.getNodeStore();
    }

    public PropertyStore getPropertyStore()
    {
        return neoStores.getPropertyStore();
    }

    public BatchingPropertyKeyTokenRepository getPropertyKeyRepository()
    {
        return propertyKeyRepository;
    }

    public BatchingLabelTokenRepository getLabelRepository()
    {
        return labelRepository;
    }

    public BatchingRelationshipTypeTokenRepository getRelationshipTypeRepository()
    {
        return relationshipTypeRepository;
    }

    public RelationshipStore getRelationshipStore()
    {
        return neoStores.getRelationshipStore();
    }

    public RecordStore<RelationshipGroupRecord> getRelationshipGroupStore()
    {
        return neoStores.getRelationshipGroupStore();
    }

    public CountsTracker getCountsStore()
    {
        return neoStores.getCounts();
    }

    @Override
    public void close() throws IOException
    {
        // Flush out all pending changes
        propertyKeyRepository.close();
        labelRepository.close();
        relationshipTypeRepository.close();

        // Close the neo store
        life.shutdown();
        neoStores.close();
        // These temporary stores are configured to be deleted when closed
        temporaryNeoStores.close();
        pageCache.close();
    }

    public long getLastCommittedTransactionId()
    {
        return neoStores.getMetaDataStore().getLastCommittedTransactionId();
    }

    public LabelScanStore getLabelScanStore()
    {
        return labelScanStore;
    }

    public NeoStores getNeoStores()
    {
        return neoStores;
    }
}
