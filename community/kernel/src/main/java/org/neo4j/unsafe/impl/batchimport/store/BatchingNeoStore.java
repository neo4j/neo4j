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
import org.neo4j.kernel.api.labelscan.LabelScanStore;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.extension.KernelExtensionFactory;
import org.neo4j.kernel.extension.KernelExtensions;
import org.neo4j.kernel.extension.UnsatisfiedDependencyStrategies;
import org.neo4j.kernel.impl.api.scan.LabelScanStoreProvider;
import org.neo4j.kernel.impl.store.NeoStore;
import org.neo4j.kernel.impl.store.NodeStore;
import org.neo4j.kernel.impl.store.PropertyStore;
import org.neo4j.kernel.impl.store.RelationshipGroupStore;
import org.neo4j.kernel.impl.store.RelationshipStore;
import org.neo4j.kernel.impl.store.StoreFactory;
import org.neo4j.kernel.impl.store.UnderlyingStorageException;
import org.neo4j.kernel.impl.store.counts.CountsTracker;
import org.neo4j.kernel.impl.transaction.state.NeoStoreProvider;
import org.neo4j.kernel.impl.util.Dependencies;
import org.neo4j.kernel.impl.util.StringLogger;
import org.neo4j.kernel.lifecycle.LifeSupport;
import org.neo4j.kernel.logging.Logging;
import org.neo4j.kernel.monitoring.Monitors;
import org.neo4j.unsafe.impl.batchimport.AdditionalInitialIds;
import org.neo4j.unsafe.impl.batchimport.Configuration;
import org.neo4j.unsafe.impl.batchimport.ParallelBatchImporter;
import org.neo4j.unsafe.impl.batchimport.store.BatchingPageCache.WriterFactory;
import org.neo4j.unsafe.impl.batchimport.store.BatchingTokenRepository.BatchingLabelTokenRepository;
import org.neo4j.unsafe.impl.batchimport.store.BatchingTokenRepository.BatchingPropertyKeyTokenRepository;
import org.neo4j.unsafe.impl.batchimport.store.BatchingTokenRepository.BatchingRelationshipTypeTokenRepository;
import org.neo4j.unsafe.impl.batchimport.store.io.Monitor;

import static java.lang.String.valueOf;

import static org.neo4j.graphdb.factory.GraphDatabaseSettings.dense_node_threshold;
import static org.neo4j.helpers.collection.MapUtil.stringMap;
import static org.neo4j.kernel.impl.store.StoreFactory.configForStoreDir;

/**
 * Creator and accessor of {@link NeoStore} with some logic to provide very batch friendly services to the
 * {@link NeoStore} when instantiating it. Different services for specific purposes.
 */
public class BatchingNeoStore implements AutoCloseable, NeoStoreProvider
{
    private final FileSystemAbstraction fileSystem;
    private final Monitors monitors;
    private final BatchingPropertyKeyTokenRepository propertyKeyRepository;
    private final BatchingLabelTokenRepository labelRepository;
    private final BatchingRelationshipTypeTokenRepository relationshipTypeRepository;
    private final StringLogger logger;
    private final Config neo4jConfig;
    private final BatchingPageCache pageCache;
    private final NeoStore neoStore;
    private final WriterFactory writerFactory;
    private final LifeSupport life = new LifeSupport();
    private final LabelScanStore labelScanStore;

    public BatchingNeoStore( FileSystemAbstraction fileSystem, File storeDir,
                             Configuration config, Monitor writeMonitor, Logging logging,
                             Monitors monitors, WriterFactory writerFactory, AdditionalInitialIds initialIds )
    {
        this.fileSystem = fileSystem;
        this.monitors = monitors;
        this.writerFactory = writerFactory;
        this.logger = logging.getMessagesLog( getClass() );
        this.neo4jConfig = configForStoreDir(
                new Config( stringMap( dense_node_threshold.name(), valueOf( config.denseNodeThreshold() ) ),
                        GraphDatabaseSettings.class ),
                storeDir );

        this.pageCache = new BatchingPageCache( fileSystem, config.fileChannelBufferSize(),
                config.bigFileChannelBufferSizeMultiplier(), writerFactory, writeMonitor );
        this.neoStore = newNeoStore( pageCache );
        flushNeoStoreAndAwaitEverythingWritten();
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
                initialIds.lastCommittedTransactionId(), initialIds.lastCommittedTransactionChecksum() );
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
        dependencies.satisfyDependency( logging );
        @SuppressWarnings( { "unchecked", "rawtypes" } )
        KernelExtensions extensions = life.add( new KernelExtensions(
                (Iterable) Service.load( KernelExtensionFactory.class ),
                dependencies, UnsatisfiedDependencyStrategies.ignore() ) );
        life.start();
        labelScanStore = life.add( extensions.resolveDependency( LabelScanStoreProvider.class,
                LabelScanStoreProvider.HIGHEST_PRIORITIZED ).getLabelScanStore() );
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
        PageCache pageCache = new BatchingPageCache( fileSystem, Configuration.DEFAULT.fileChannelBufferSize(),
                Configuration.DEFAULT.bigFileChannelBufferSizeMultiplier(),
                BatchingPageCache.SYNCHRONOUS, Monitor.NO_MONITOR );
        StoreFactory storeFactory = new StoreFactory(
                fileSystem, new File( storeDir ), pageCache, StringLogger.DEV_NULL, new Monitors() );
        storeFactory.createNeoStore().close();
        pageCache.close();
    }

    private NeoStore newNeoStore( PageCache pageCache )
    {
        StoreFactory storeFactory = new StoreFactory( neo4jConfig, new BatchingIdGeneratorFactory(),
                pageCache, fileSystem, logger, monitors );
        return storeFactory.newNeoStore( true );
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
    public void close()
    {
        // Flush out all pending changes
        propertyKeyRepository.close();
        labelRepository.close();
        relationshipTypeRepository.close();
        flushNeoStoreAndAwaitEverythingWritten();

        // Close the neo store
        life.shutdown();
        neoStore.close();
    }

    private void flushNeoStoreAndAwaitEverythingWritten()
    {
        neoStore.flush();
        // Issuing a "flush" might queue up I/O jobs to the WriterFactory given to this batching neo store.
        // That's why we have to wait for any pending I/O jobs to be written after the flush.
        writerFactory.awaitEverythingWritten();
    }

    public void flush() throws IOException
    {
        pageCache.flushAndForce();
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
    public NeoStore evaluate()
    {
        return neoStore;
    }
}
