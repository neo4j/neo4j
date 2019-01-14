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
package org.neo4j.unsafe.impl.batchimport.store;

import java.io.File;
import java.io.IOException;
import java.nio.file.OpenOption;
import java.nio.file.StandardOpenOption;
import java.util.function.Predicate;

import org.neo4j.function.Predicates;
import org.neo4j.index.internal.gbptree.RecoveryCleanupWorkCollector;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.io.pagecache.PagedFile;
import org.neo4j.io.pagecache.tracing.DefaultPageCacheTracer;
import org.neo4j.io.pagecache.tracing.PageCacheTracer;
import org.neo4j.io.pagecache.tracing.cursor.DefaultPageCursorTracerSupplier;
import org.neo4j.io.pagecache.tracing.cursor.PageCursorTracerSupplier;
import org.neo4j.io.pagecache.tracing.cursor.context.EmptyVersionContextSupplier;
import org.neo4j.io.pagecache.tracing.cursor.context.VersionContextSupplier;
import org.neo4j.kernel.api.labelscan.LabelScanStore;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.impl.api.scan.FullStoreChangeStream;
import org.neo4j.kernel.impl.index.labelscan.NativeLabelScanStore;
import org.neo4j.kernel.impl.logging.LogService;
import org.neo4j.kernel.impl.pagecache.ConfiguringPageCacheFactory;
import org.neo4j.kernel.impl.store.NeoStores;
import org.neo4j.kernel.impl.store.NodeStore;
import org.neo4j.kernel.impl.store.PropertyStore;
import org.neo4j.kernel.impl.store.RecordStore;
import org.neo4j.kernel.impl.store.RelationshipStore;
import org.neo4j.kernel.impl.store.StoreFactory;
import org.neo4j.kernel.impl.store.StoreType;
import org.neo4j.kernel.impl.store.counts.CountsTracker;
import org.neo4j.kernel.impl.store.format.Capability;
import org.neo4j.kernel.impl.store.format.RecordFormats;
import org.neo4j.kernel.impl.store.id.DefaultIdGeneratorFactory;
import org.neo4j.kernel.impl.store.id.IdGeneratorFactory;
import org.neo4j.kernel.impl.store.record.AbstractBaseRecord;
import org.neo4j.kernel.impl.store.record.RelationshipGroupRecord;
import org.neo4j.kernel.impl.storemigration.StoreFileType;
import org.neo4j.kernel.lifecycle.LifeSupport;
import org.neo4j.kernel.monitoring.Monitors;
import org.neo4j.logging.LogProvider;
import org.neo4j.unsafe.impl.batchimport.AdditionalInitialIds;
import org.neo4j.unsafe.impl.batchimport.Configuration;
import org.neo4j.unsafe.impl.batchimport.cache.MemoryStatsVisitor;
import org.neo4j.unsafe.impl.batchimport.input.Input.Estimates;
import org.neo4j.unsafe.impl.batchimport.store.BatchingTokenRepository.BatchingLabelTokenRepository;
import org.neo4j.unsafe.impl.batchimport.store.BatchingTokenRepository.BatchingPropertyKeyTokenRepository;
import org.neo4j.unsafe.impl.batchimport.store.BatchingTokenRepository.BatchingRelationshipTypeTokenRepository;
import org.neo4j.unsafe.impl.batchimport.store.io.IoTracer;

import static java.lang.String.valueOf;
import static org.neo4j.graphdb.factory.GraphDatabaseSettings.dense_node_threshold;
import static org.neo4j.graphdb.factory.GraphDatabaseSettings.pagecache_memory;
import static org.neo4j.helpers.collection.MapUtil.stringMap;
import static org.neo4j.io.IOUtils.closeAll;
import static org.neo4j.io.pagecache.IOLimiter.unlimited;
import static org.neo4j.kernel.impl.index.labelscan.NativeLabelScanStore.getLabelScanStoreFile;
import static org.neo4j.kernel.impl.store.MetaDataStore.DEFAULT_NAME;
import static org.neo4j.kernel.impl.store.StoreType.PROPERTY;
import static org.neo4j.kernel.impl.store.StoreType.PROPERTY_ARRAY;
import static org.neo4j.kernel.impl.store.StoreType.PROPERTY_STRING;
import static org.neo4j.kernel.impl.store.StoreType.RELATIONSHIP_GROUP;
import static org.neo4j.kernel.impl.transaction.log.TransactionIdStore.BASE_TX_COMMIT_TIMESTAMP;

/**
 * Creator and accessor of {@link NeoStores} with some logic to provide very batch friendly services to the
 * {@link NeoStores} when instantiating it. Different services for specific purposes.
 */
public class BatchingNeoStores implements AutoCloseable, MemoryStatsVisitor.Visitable
{
    // Empirical and slightly defensive threshold where relationship records seem to start requiring double record units.
    // Basically decided by picking a maxId of pointer (as well as node ids) in the relationship record and randomizing its data,
    // seeing which is a maxId where records starts to require a secondary unit.
    static final long DOUBLE_RELATIONSHIP_RECORD_UNIT_THRESHOLD = 1L << 33;
    private static final String TEMP_NEOSTORE_NAME = "temp." + DEFAULT_NAME;
    private static final StoreType[] TEMP_STORE_TYPES = {RELATIONSHIP_GROUP, PROPERTY, PROPERTY_ARRAY, PROPERTY_STRING};

    private final FileSystemAbstraction fileSystem;
    private final LogProvider logProvider;
    private final File storeDir;
    private final Config neo4jConfig;
    private final Configuration importConfiguration;
    private final PageCache pageCache;
    private final IoTracer ioTracer;
    private final RecordFormats recordFormats;
    private final AdditionalInitialIds initialIds;
    private final boolean externalPageCache;
    private final IdGeneratorFactory idGeneratorFactory;

    // Some stores are considered temporary during the import and will be reordered/restructured
    // into the main store. These temporary stores will live here
    private NeoStores neoStores;
    private NeoStores temporaryNeoStores;
    private BatchingPropertyKeyTokenRepository propertyKeyRepository;
    private BatchingLabelTokenRepository labelRepository;
    private BatchingRelationshipTypeTokenRepository relationshipTypeRepository;
    private LifeSupport life = new LifeSupport();
    private LabelScanStore labelScanStore;
    private PageCacheFlusher flusher;
    private boolean doubleRelationshipRecordUnits;

    private boolean successful;

    private BatchingNeoStores( FileSystemAbstraction fileSystem, PageCache pageCache, File storeDir,
            RecordFormats recordFormats, Config neo4jConfig, Configuration importConfiguration, LogService logService,
            AdditionalInitialIds initialIds, boolean externalPageCache, IoTracer ioTracer )
    {
        this.fileSystem = fileSystem;
        this.recordFormats = recordFormats;
        this.importConfiguration = importConfiguration;
        this.initialIds = initialIds;
        this.logProvider = logService.getInternalLogProvider();
        this.storeDir = storeDir;
        this.neo4jConfig = neo4jConfig;
        this.pageCache = pageCache;
        this.ioTracer = ioTracer;
        this.externalPageCache = externalPageCache;
        this.idGeneratorFactory = new DefaultIdGeneratorFactory( fileSystem );
    }

    private boolean databaseExistsAndContainsData()
    {
        File metaDataFile = new File( storeDir, StoreType.META_DATA.getStoreFile().fileName( StoreFileType.STORE ) );
        try ( PagedFile pagedFile = pageCache.map( metaDataFile, pageCache.pageSize(), StandardOpenOption.READ ) )
        {
            // OK so the db probably exists
        }
        catch ( IOException e )
        {
            // It's OK
            return false;
        }

        try ( NeoStores stores = newStoreFactory( DEFAULT_NAME ).openNeoStores( StoreType.NODE, StoreType.RELATIONSHIP ) )
        {
            return stores.getNodeStore().getHighId() > 0 || stores.getRelationshipStore().getHighId() > 0;
        }
    }

    /**
     * Called when expecting a clean {@code storeDir} folder and where a new store will be created.
     * This happens on an initial attempt to import.
     *
     * @throws IOException on I/O error.
     * @throws IllegalStateException if {@code storeDir} already contains a database.
     */
    public void createNew() throws IOException
    {
        assertDatabaseIsEmptyOrNonExistent();

        // There may have been a previous import which was killed before it even started, where the label scan store could
        // be in a semi-initialized state. Better to be on the safe side and deleted it. We get her after determining that
        // the db is either completely empty or non-existent anyway, so deleting this file is OK.
        fileSystem.deleteFile( getLabelScanStoreFile( storeDir ) );

        instantiateStores();
        neoStores.getMetaDataStore().setLastCommittedAndClosedTransactionId(
                initialIds.lastCommittedTransactionId(), initialIds.lastCommittedTransactionChecksum(),
                BASE_TX_COMMIT_TIMESTAMP, initialIds.lastCommittedTransactionLogByteOffset(),
                initialIds.lastCommittedTransactionLogVersion() );
        neoStores.startCountStore();
    }

    public void assertDatabaseIsEmptyOrNonExistent()
    {
        if ( databaseExistsAndContainsData() )
        {
            throw new IllegalStateException( storeDir + " already contains data, cannot do import here" );
        }
    }

    /**
     * Called when expecting a previous attempt/state of a database to open, where some store files should be kept,
     * but others deleted. All temporary stores will be deleted in this call.
     *
     * @param mainStoresToKeep {@link Predicate} controlling which files to keep, i.e. {@code true} means keep, {@code false} means delete.
     * @param tempStoresToKeep {@link Predicate} controlling which files to keep, i.e. {@code true} means keep, {@code false} means delete.
     * @throws IOException on I/O error.
     */
    public void pruneAndOpenExistingStore( Predicate<StoreType> mainStoresToKeep, Predicate<StoreType> tempStoresToKeep ) throws IOException
    {
        deleteStoreFiles( TEMP_NEOSTORE_NAME, tempStoresToKeep );
        deleteStoreFiles( DEFAULT_NAME, mainStoresToKeep );
        instantiateStores();
        neoStores.startCountStore();
    }

    private void deleteStoreFiles( String storeName, Predicate<StoreType> storesToKeep )
    {
        FileSystemAbstraction fs = pageCache.getCachedFileSystem();
        for ( StoreType type : StoreType.values() )
        {
            if ( type.isRecordStore() && !storesToKeep.test( type ) )
            {
                for ( StoreFileType fileType : StoreFileType.values() )
                {
                    fs.deleteFile( new File( storeDir, fileType.augment( storeName + type.getStoreFile().fileNamePart() ) ) );
                }
            }
        }
    }

    private void instantiateKernelExtensions()
    {
        life = new LifeSupport();
        life.start();
        labelScanStore = new NativeLabelScanStore( pageCache, fileSystem, storeDir, FullStoreChangeStream.EMPTY, false, new Monitors(),
                RecoveryCleanupWorkCollector.immediate() );
        life.add( labelScanStore );
    }

    private void instantiateStores()
    {
        neoStores = newStoreFactory( DEFAULT_NAME ).openAllNeoStores( true );
        propertyKeyRepository = new BatchingPropertyKeyTokenRepository(
                neoStores.getPropertyKeyTokenStore() );
        labelRepository = new BatchingLabelTokenRepository(
                neoStores.getLabelTokenStore() );
        relationshipTypeRepository = new BatchingRelationshipTypeTokenRepository(
                neoStores.getRelationshipTypeTokenStore() );
        temporaryNeoStores = instantiateTempStores();
        instantiateKernelExtensions();

        // Delete the id generators because makeStoreOk isn't atomic in the sense that there's a possibility of an unlucky timing such
        // that if the process is killed at the right time some store may end up with a .id file that looks to be CLEAN and has highId=0,
        // i.e. effectively making the store look empty on the next start. Normal recovery of a db is sort of protected by this recovery
        // recognizing that the db needs recovery when it looks at the tx log and also calling deleteIdGenerators. In the import case
        // there are no tx logs at all, and therefore we do this manually right here.
        neoStores.deleteIdGenerators();
        temporaryNeoStores.deleteIdGenerators();

        neoStores.makeStoreOk();
        temporaryNeoStores.makeStoreOk();
    }

    private NeoStores instantiateTempStores()
    {
        return newStoreFactory( TEMP_NEOSTORE_NAME ).openNeoStores( true, TEMP_STORE_TYPES );
    }

    public static BatchingNeoStores batchingNeoStores( FileSystemAbstraction fileSystem, File storeDir,
            RecordFormats recordFormats, Configuration config, LogService logService, AdditionalInitialIds initialIds,
            Config dbConfig )
    {
        Config neo4jConfig = getNeo4jConfig( config, dbConfig );
        final PageCacheTracer tracer = new DefaultPageCacheTracer();
        PageCache pageCache = createPageCache( fileSystem, neo4jConfig, logService.getInternalLogProvider(), tracer,
                DefaultPageCursorTracerSupplier.INSTANCE, EmptyVersionContextSupplier.EMPTY );

        return new BatchingNeoStores( fileSystem, pageCache, storeDir, recordFormats, neo4jConfig, config, logService,
                initialIds, false, tracer::bytesWritten );
    }

    public static BatchingNeoStores batchingNeoStoresWithExternalPageCache( FileSystemAbstraction fileSystem,
            PageCache pageCache, PageCacheTracer tracer, File storeDir, RecordFormats recordFormats,
            Configuration config, LogService logService, AdditionalInitialIds initialIds, Config dbConfig )
    {
        Config neo4jConfig = getNeo4jConfig( config, dbConfig );

        return new BatchingNeoStores( fileSystem, pageCache, storeDir, recordFormats, neo4jConfig, config, logService,
                initialIds, true, tracer::bytesWritten );
    }

    private static Config getNeo4jConfig( Configuration config, Config dbConfig )
    {
        dbConfig.augment( stringMap(
                dense_node_threshold.name(), valueOf( config.denseNodeThreshold() ),
                pagecache_memory.name(), valueOf( config.pageCacheMemory() ) ) );
        return dbConfig;
    }

    private static PageCache createPageCache( FileSystemAbstraction fileSystem, Config config, LogProvider log,
            PageCacheTracer tracer, PageCursorTracerSupplier cursorTracerSupplier, VersionContextSupplier contextSupplier )
    {
        return new ConfiguringPageCacheFactory( fileSystem, config, tracer, cursorTracerSupplier,
                log.getLog( BatchingNeoStores.class ), contextSupplier ).getOrCreatePageCache();
    }

    private StoreFactory newStoreFactory( String name, OpenOption... openOptions )
    {
        return new StoreFactory( storeDir, name, neo4jConfig, idGeneratorFactory, pageCache, fileSystem, recordFormats, logProvider,
                        EmptyVersionContextSupplier.EMPTY, openOptions );
    }

    /**
     * @return temporary relationship group store which will be deleted in {@link #close()}.
     */
    public RecordStore<RelationshipGroupRecord> getTemporaryRelationshipGroupStore()
    {
        return temporaryNeoStores.getRelationshipGroupStore();
    }

    /**
     * @return temporary property store which will be deleted in {@link #close()}.
     */
    public PropertyStore getTemporaryPropertyStore()
    {
        return temporaryNeoStores.getPropertyStore();
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
        // Here as a safety mechanism when e.g. panicking.
        if ( flusher != null )
        {
            stopFlushingPageCache();
        }

        flushAndForce();

        // Flush out all pending changes
        closeAll( propertyKeyRepository, labelRepository, relationshipTypeRepository );

        // Close the neo store
        life.shutdown();
        closeAll( neoStores, temporaryNeoStores );
        if ( !externalPageCache )
        {
            pageCache.close();
        }

        if ( successful )
        {
            cleanup();
        }
    }

    private void cleanup()
    {
        deleteStoreFiles( TEMP_NEOSTORE_NAME, Predicates.alwaysFalse() );
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

    public void startFlushingPageCache()
    {
        if ( importConfiguration.sequentialBackgroundFlushing() )
        {
            if ( flusher != null )
            {
                throw new IllegalStateException( "Flusher already started" );
            }
            flusher = new PageCacheFlusher( pageCache );
            flusher.start();
        }
    }

    public void stopFlushingPageCache()
    {
        if ( importConfiguration.sequentialBackgroundFlushing() )
        {
            if ( flusher == null )
            {
                throw new IllegalStateException( "Flusher not started" );
            }
            flusher.halt();
            flusher = null;
        }
    }

    @Override
    public void acceptMemoryStatsVisitor( MemoryStatsVisitor visitor )
    {
        visitor.offHeapUsage( pageCache.maxCachedPages() * pageCache.pageSize() );
    }

    public PageCache getPageCache()
    {
        return pageCache;
    }

    public void flushAndForce()
    {
        if ( propertyKeyRepository != null )
        {
            propertyKeyRepository.flush();
        }
        if ( labelRepository != null )
        {
            labelRepository.flush();
        }
        if ( relationshipTypeRepository != null )
        {
            relationshipTypeRepository.flush();
        }
        if ( neoStores != null )
        {
            neoStores.flush( unlimited() );
            flushIdFiles( neoStores, StoreType.values() );
        }
        if ( temporaryNeoStores != null )
        {
            temporaryNeoStores.flush( unlimited() );
            flushIdFiles( temporaryNeoStores, TEMP_STORE_TYPES );
        }
        if ( labelScanStore != null )
        {
            labelScanStore.force( unlimited() );
        }
    }

    public void success()
    {
        successful = true;
    }

    public boolean determineDoubleRelationshipRecordUnits( Estimates inputEstimates )
    {
        doubleRelationshipRecordUnits =
                recordFormats.hasCapability( Capability.SECONDARY_RECORD_UNITS ) &&
                inputEstimates.numberOfRelationships() > DOUBLE_RELATIONSHIP_RECORD_UNIT_THRESHOLD;
        return doubleRelationshipRecordUnits;
    }

    public boolean usesDoubleRelationshipRecordUnits()
    {
        return doubleRelationshipRecordUnits;
    }

    private void flushIdFiles( NeoStores neoStores, StoreType[] storeTypes )
    {
        for ( StoreType type : storeTypes )
        {
            if ( type.isRecordStore() )
            {
                RecordStore<AbstractBaseRecord> recordStore = neoStores.getRecordStore( type );
                idGeneratorFactory.create( new File( storeDir, type.getStoreFile().fileName( StoreFileType.ID ) ), recordStore.getHighId(),
                        false );
            }
        }
    }
}
