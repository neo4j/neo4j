/*
 * Copyright (c) 2002-2015 "Neo Technology,"
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
import org.neo4j.kernel.impl.api.scan.LabelScanStoreProvider;
import org.neo4j.kernel.impl.logging.LogService;
import org.neo4j.kernel.impl.pagecache.ConfiguringPageCacheFactory;
import org.neo4j.kernel.impl.spi.KernelContext;
import org.neo4j.kernel.impl.store.NeoStore;
import org.neo4j.kernel.impl.store.NodeStore;
import org.neo4j.kernel.impl.store.PropertyStore;
import org.neo4j.kernel.impl.store.RelationshipGroupStore;
import org.neo4j.kernel.impl.store.RelationshipStore;
import org.neo4j.kernel.impl.store.StoreFactory;
import org.neo4j.kernel.impl.store.UnderlyingStorageException;
import org.neo4j.kernel.impl.store.counts.CountsTracker;
import org.neo4j.kernel.impl.transaction.state.NeoStoreSupplier;
import org.neo4j.kernel.impl.util.Dependencies;
import org.neo4j.kernel.lifecycle.LifeSupport;
import org.neo4j.kernel.monitoring.Monitors;
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

import static org.neo4j.graphdb.factory.GraphDatabaseSettings.dense_node_threshold;
import static org.neo4j.graphdb.factory.GraphDatabaseSettings.pagecache_memory;
import static org.neo4j.helpers.collection.MapUtil.stringMap;

/**
 * Creator and accessor of {@link NeoStore} with some logic to provide very batch friendly services to the
 * {@link NeoStore} when instantiating it. Different services for specific purposes.
 */
public class BatchingNeoStore implements AutoCloseable, NeoStoreSupplier
{
    private final FileSystemAbstraction fileSystem;
    private final Monitors monitors;
    private final BatchingPropertyKeyTokenRepository propertyKeyRepository;
    private final BatchingLabelTokenRepository labelRepository;
    private final BatchingRelationshipTypeTokenRepository relationshipTypeRepository;
    private final LogProvider logProvider;
    private final File storeDir;
    private final Config neo4jConfig;
    private final PageCache pageCache;
    private final NeoStore neoStore;
    private final LifeSupport life = new LifeSupport();
    private final LabelScanStore labelScanStore;
    private final IoTracer ioTracer;

    public BatchingNeoStore( FileSystemAbstraction fileSystem, File storeDir,
                             Configuration config, LogService logService,
                             Monitors monitors, AdditionalInitialIds initialIds )
    {
        this.fileSystem = fileSystem;
        this.monitors = monitors;
        this.logProvider = logService.getInternalLogProvider();
        this.storeDir = storeDir;
        this.neo4jConfig = new Config( stringMap(
                dense_node_threshold.name(), valueOf( config.denseNodeThreshold() ),
                pagecache_memory.name(), valueOf( config.writeBufferSize() ) ),
                GraphDatabaseSettings.class );
        final PageCacheTracer tracer = new DefaultPageCacheTracer();
        this.pageCache = createPageCache( fileSystem, neo4jConfig, logProvider, tracer );
        this.ioTracer = new IoTracer()
        {
            @Override
            public long countBytesWritten()
            {
                return tracer.countBytesWritten();
            }
        };
        this.neoStore = newNeoStore( pageCache );
        if ( alreadyContainsData( neoStore ) )
        {
            neoStore.close();
            throw new IllegalStateException( storeDir + " already contains data, cannot do import here" );
        }
        try
        {
            neoStore.rebuildCountStoreIfNeeded();
        }
        catch ( IOException e )
        {
            throw new UnderlyingStorageException( e );
        }
        neoStore.setLastCommittedAndClosedTransactionId(
                initialIds.lastCommittedTransactionId(), initialIds.lastCommittedTransactionChecksum(),
                initialIds.lastCommittedTransactionLogVersion(), initialIds.lastCommittedTransactionLogByteOffset() );
        this.propertyKeyRepository = new BatchingPropertyKeyTokenRepository(
                neoStore.getPropertyKeyTokenStore(), initialIds.highPropertyKeyTokenId() );
        this.labelRepository = new BatchingLabelTokenRepository(
                neoStore.getLabelTokenStore(), initialIds.highLabelTokenId() );
        this.relationshipTypeRepository = new BatchingRelationshipTypeTokenRepository(
                neoStore.getRelationshipTypeTokenStore(), initialIds.highRelationshipTypeTokenId() );

        // Initialze kernel extensions
        Dependencies dependencies = new Dependencies();
        dependencies.satisfyDependency( neo4jConfig );
        dependencies.satisfyDependency( fileSystem );
        dependencies.satisfyDependency( this );
        dependencies.satisfyDependency( logService );
        KernelContext kernelContext = new KernelContext()
        {
            @Override
            public FileSystemAbstraction fileSystem()
            {
                return BatchingNeoStore.this.fileSystem;
            }

            @Override
            public File storeDir()
            {
                return BatchingNeoStore.this.storeDir;
            }
        };
        @SuppressWarnings( { "unchecked", "rawtypes" } )
        KernelExtensions extensions = life.add( new KernelExtensions(
                kernelContext, (Iterable) Service.load( KernelExtensionFactory.class ),
                dependencies, UnsatisfiedDependencyStrategies.ignore() ) );
        life.start();
        labelScanStore = life.add( extensions.resolveDependency( LabelScanStoreProvider.class,
                LabelScanStoreProvider.HIGHEST_PRIORITIZED ).getLabelScanStore() );
    }

    private static PageCache createPageCache( FileSystemAbstraction fileSystem, Config config, LogProvider log,
            PageCacheTracer tracer )
    {
        return new ConfiguringPageCacheFactory( fileSystem, config, tracer,
                log.getLog( BatchingNeoStore.class ) ).getOrCreatePageCache();
    }

    private boolean alreadyContainsData( NeoStore neoStore )
    {
        return neoStore.getNodeStore().getHighId() > 0 || neoStore.getRelationshipStore().getHighId() > 0;
    }

    /**
     * A way to create the underlying {@link NeoStore} files in the {@link FileSystemAbstraction file system}
     * before instantiating the real one. This allows some store contents to be populated before an import.
     * Useful for store migration where the {@link ParallelBatchImporter} is used as migrator and some of
     * its data need to be communicated by copying a store file.
     */
    public static void createStore( FileSystemAbstraction fileSystem, String storeDir ) throws IOException
    {
        try ( PageCache pageCache = createPageCache( fileSystem, new Config(), NullLogProvider.getInstance(),
                PageCacheTracer.NULL ) )
        {
            StoreFactory storeFactory = new StoreFactory(
                    fileSystem, new File( storeDir ), pageCache, NullLogProvider.getInstance(), new Monitors() );
            storeFactory.createNeoStore().close();
        }
    }

    private NeoStore newNeoStore( PageCache pageCache )
    {
        BatchingIdGeneratorFactory idGeneratorFactory = new BatchingIdGeneratorFactory( fileSystem );
        StoreFactory storeFactory = new StoreFactory(
                storeDir, neo4jConfig, idGeneratorFactory, pageCache, fileSystem, logProvider, monitors );
        return storeFactory.newNeoStore( true );
    }

    public IoTracer getIoTracer()
    {
        return ioTracer;
    }

    public NodeStore getNodeStore()
    {
        return neoStore.getNodeStore();
    }

    public PropertyStore getPropertyStore()
    {
        return neoStore.getPropertyStore();
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
        return neoStore.getRelationshipStore();
    }

    public RelationshipGroupStore getRelationshipGroupStore()
    {
        return neoStore.getRelationshipGroupStore();
    }

    public CountsTracker getCountsStore()
    {
        return neoStore.getCounts();
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
        neoStore.close();
        pageCache.close();
    }

    public long getLastCommittedTransactionId()
    {
        return neoStore.getLastCommittedTransactionId();
    }

    public LabelScanStore getLabelScanStore()
    {
        return labelScanStore;
    }

    @Override
    public NeoStore get()
    {
        return neoStore;
    }
}
