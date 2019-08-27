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
package org.neo4j.internal.batchimport.store;

import java.io.File;
import java.io.IOException;
import java.nio.file.OpenOption;
import java.nio.file.StandardOpenOption;
import java.util.function.Predicate;

import org.neo4j.configuration.Config;
import org.neo4j.internal.batchimport.AdditionalInitialIds;
import org.neo4j.internal.batchimport.Configuration;
import org.neo4j.internal.batchimport.cache.MemoryStatsVisitor;
import org.neo4j.internal.batchimport.input.Input;
import org.neo4j.internal.batchimport.store.BatchingTokenRepository.BatchingLabelTokenRepository;
import org.neo4j.internal.batchimport.store.BatchingTokenRepository.BatchingPropertyKeyTokenRepository;
import org.neo4j.internal.batchimport.store.BatchingTokenRepository.BatchingRelationshipTypeTokenRepository;
import org.neo4j.internal.batchimport.store.io.IoTracer;
import org.neo4j.internal.id.DefaultIdGeneratorFactory;
import org.neo4j.internal.id.IdGenerator;
import org.neo4j.internal.id.IdGeneratorFactory;
import org.neo4j.internal.index.label.FullStoreChangeStream;
import org.neo4j.internal.index.label.LabelScanStore;
import org.neo4j.internal.index.label.NativeLabelScanStore;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.layout.DatabaseFile;
import org.neo4j.io.layout.DatabaseLayout;
import org.neo4j.io.mem.MemoryAllocator;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.io.pagecache.PagedFile;
import org.neo4j.io.pagecache.impl.SingleFilePageSwapperFactory;
import org.neo4j.io.pagecache.impl.muninn.MuninnPageCache;
import org.neo4j.io.pagecache.tracing.DefaultPageCacheTracer;
import org.neo4j.io.pagecache.tracing.PageCacheTracer;
import org.neo4j.io.pagecache.tracing.cursor.DefaultPageCursorTracerSupplier;
import org.neo4j.io.pagecache.tracing.cursor.context.EmptyVersionContextSupplier;
import org.neo4j.kernel.impl.store.NeoStores;
import org.neo4j.kernel.impl.store.NodeStore;
import org.neo4j.kernel.impl.store.PropertyStore;
import org.neo4j.kernel.impl.store.RecordStore;
import org.neo4j.kernel.impl.store.RelationshipGroupStore;
import org.neo4j.kernel.impl.store.RelationshipStore;
import org.neo4j.kernel.impl.store.StoreFactory;
import org.neo4j.kernel.impl.store.StoreType;
import org.neo4j.kernel.impl.store.counts.CountsTracker;
import org.neo4j.kernel.impl.store.format.RecordFormats;
import org.neo4j.kernel.impl.store.format.RecordStorageCapability;
import org.neo4j.kernel.impl.store.kvstore.DataInitializer;
import org.neo4j.kernel.impl.store.record.RelationshipGroupRecord;
import org.neo4j.kernel.lifecycle.LifeSupport;
import org.neo4j.kernel.lifecycle.Lifespan;
import org.neo4j.logging.LogProvider;
import org.neo4j.logging.internal.LogService;
import org.neo4j.memory.EmptyMemoryTracker;
import org.neo4j.monitoring.Monitors;
import org.neo4j.scheduler.JobScheduler;

import static java.lang.String.valueOf;
import static org.neo4j.configuration.GraphDatabaseSettings.pagecache_memory;
import static org.neo4j.index.internal.gbptree.RecoveryCleanupWorkCollector.immediate;
import static org.neo4j.io.IOUtils.closeAll;
import static org.neo4j.io.mem.MemoryAllocator.createAllocator;
import static org.neo4j.io.pagecache.IOLimiter.UNLIMITED;
import static org.neo4j.kernel.impl.store.StoreType.PROPERTY;
import static org.neo4j.kernel.impl.store.StoreType.PROPERTY_ARRAY;
import static org.neo4j.kernel.impl.store.StoreType.PROPERTY_STRING;
import static org.neo4j.kernel.impl.store.StoreType.RELATIONSHIP_GROUP;
import static org.neo4j.storageengine.api.TransactionIdStore.BASE_TX_COMMIT_TIMESTAMP;

/**
 * Creator and accessor of {@link NeoStores} with some logic to provide very batch friendly services to the
 * {@link NeoStores} when instantiating it. Different services for specific purposes.
 */
public class BatchingNeoStores implements AutoCloseable, MemoryStatsVisitor.Visitable
{
    private static final String TEMP_STORE_NAME = "temp";
    // Empirical and slightly defensive threshold where relationship records seem to start requiring double record units.
    // Basically decided by picking a maxId of pointer (as well as node ids) in the relationship record and randomizing its data,
    // seeing which is a maxId where records starts to require a secondary unit.
    static final long DOUBLE_RELATIONSHIP_RECORD_UNIT_THRESHOLD = 1L << 33;
    private static final StoreType[] TEMP_STORE_TYPES = {RELATIONSHIP_GROUP, PROPERTY, PROPERTY_ARRAY, PROPERTY_STRING};

    private final FileSystemAbstraction fileSystem;
    private final LogProvider logProvider;
    private final DatabaseLayout databaseLayout;
    private final DatabaseLayout temporaryDatabaseLayout;
    private final Config neo4jConfig;
    private final Configuration importConfiguration;
    private final PageCache pageCache;
    private final IoTracer ioTracer;
    private final RecordFormats recordFormats;
    private final AdditionalInitialIds initialIds;
    private final boolean externalPageCache;
    private final IdGeneratorFactory idGeneratorFactory;
    private final IdGeneratorFactory tempIdGeneratorFactory;
    private final Lifespan countsStoreLife = new Lifespan();

    // Some stores are considered temporary during the import and will be reordered/restructured
    // into the main store. These temporary stores will live here
    private NeoStores neoStores;
    private CountsTracker countsStore;
    private NeoStores temporaryNeoStores;
    private BatchingPropertyKeyTokenRepository propertyKeyRepository;
    private BatchingLabelTokenRepository labelRepository;
    private BatchingRelationshipTypeTokenRepository relationshipTypeRepository;
    private LifeSupport life = new LifeSupport();
    private LabelScanStore labelScanStore;
    private PageCacheFlusher flusher;
    private boolean doubleRelationshipRecordUnits;

    private boolean successful;

    private BatchingNeoStores( FileSystemAbstraction fileSystem, PageCache pageCache, DatabaseLayout databaseLayout,
            RecordFormats recordFormats, Config neo4jConfig, Configuration importConfiguration, LogService logService,
            AdditionalInitialIds initialIds, boolean externalPageCache, IoTracer ioTracer )
    {
        this.fileSystem = fileSystem;
        this.recordFormats = recordFormats;
        this.importConfiguration = importConfiguration;
        this.initialIds = initialIds;
        this.logProvider = logService.getInternalLogProvider();
        this.databaseLayout = databaseLayout;
        this.temporaryDatabaseLayout = DatabaseLayout.of( databaseLayout.file( TEMP_STORE_NAME ), TEMP_STORE_NAME );
        this.neo4jConfig = neo4jConfig;
        this.pageCache = pageCache;
        this.ioTracer = ioTracer;
        this.externalPageCache = externalPageCache;
        this.idGeneratorFactory = new DefaultIdGeneratorFactory( fileSystem, immediate() );
        this.tempIdGeneratorFactory = new DefaultIdGeneratorFactory( fileSystem, immediate() );
    }

    private boolean databaseExistsAndContainsData()
    {
        File metaDataFile = databaseLayout.metadataStore();
        try ( PagedFile pagedFile = pageCache.map( metaDataFile, pageCache.pageSize(), StandardOpenOption.READ ) )
        {
            // OK so the db probably exists
        }
        catch ( IOException e )
        {
            // It's OK
            return false;
        }

        try ( NeoStores stores = newStoreFactory( databaseLayout, idGeneratorFactory ).openNeoStores( StoreType.NODE, StoreType.RELATIONSHIP ) )
        {
            return stores.getNodeStore().getHighId() > 0 || stores.getRelationshipStore().getHighId() > 0;
        }
    }

    /**
     * Called when expecting a clean {@code storeDir} folder and where a new store will be created.
     * This happens on an initial attempt to import.
     *
     * @throws IllegalStateException if {@code storeDir} already contains a database.
     */
    public void createNew() throws IOException
    {
        assertDatabaseIsEmptyOrNonExistent();

        // There may have been a previous import which was killed before it even started, where the label scan store could
        // be in a semi-initialized state. Better to be on the safe side and deleted it. We get her after determining that
        // the db is either completely empty or non-existent anyway, so deleting this file is OK.
        fileSystem.deleteFile( NativeLabelScanStore.getLabelScanStoreFile( databaseLayout ) );

        instantiateStores();
        neoStores.getMetaDataStore().setLastCommittedAndClosedTransactionId(
                initialIds.lastCommittedTransactionId(), initialIds.lastCommittedTransactionChecksum(),
                BASE_TX_COMMIT_TIMESTAMP, initialIds.lastCommittedTransactionLogByteOffset(),
                initialIds.lastCommittedTransactionLogVersion() );
    }

    public void assertDatabaseIsEmptyOrNonExistent()
    {
        if ( databaseExistsAndContainsData() )
        {
            throw new IllegalStateException( databaseLayout.databaseDirectory() + " already contains data, cannot do import here" );
        }
    }

    /**
     * Called when expecting a previous attempt/state of a database to open, where some store files should be kept,
     * but others deleted. All temporary stores will be deleted in this call.
     *
     * @param mainStoresToKeep {@link Predicate} controlling which files to keep, i.e. {@code true} means keep, {@code false} means delete.
     * @param tempStoresToKeep {@link Predicate} controlling which files to keep, i.e. {@code true} means keep, {@code false} means delete.
     */
    public void pruneAndOpenExistingStore( Predicate<StoreType> mainStoresToKeep, Predicate<StoreType> tempStoresToKeep ) throws IOException
    {
        deleteStoreFiles( temporaryDatabaseLayout, tempStoresToKeep );
        deleteStoreFiles( databaseLayout, mainStoresToKeep );
        instantiateStores();
    }

    private void deleteStoreFiles( DatabaseLayout databaseLayout, Predicate<StoreType> storesToKeep )
    {
        for ( StoreType type : StoreType.values() )
        {
            if ( !storesToKeep.test( type ) )
            {
                DatabaseFile databaseFile = type.getDatabaseFile();
                databaseLayout.allFiles( databaseFile ).forEach( fileSystem::deleteFile );
            }
        }
    }

    private void instantiateExtensions()
    {
        life = new LifeSupport();
        life.start();
        labelScanStore = new NativeLabelScanStore( pageCache, databaseLayout, fileSystem, FullStoreChangeStream.EMPTY, false, new Monitors(),
                immediate() );
        life.add( labelScanStore );
    }

    private void instantiateStores() throws IOException
    {
        neoStores = newStoreFactory( databaseLayout, idGeneratorFactory ).openAllNeoStores( true );
        countsStore = new CountsTracker( logProvider, fileSystem, pageCache, neo4jConfig, databaseLayout, EmptyVersionContextSupplier.EMPTY );
        countsStore.setInitializer( DataInitializer.empty( initialIds.lastCommittedTransactionId() ) );
        countsStoreLife.add( countsStore );
        propertyKeyRepository = new BatchingPropertyKeyTokenRepository(
                neoStores.getPropertyKeyTokenStore() );
        labelRepository = new BatchingLabelTokenRepository(
                neoStores.getLabelTokenStore() );
        relationshipTypeRepository = new BatchingRelationshipTypeTokenRepository(
                neoStores.getRelationshipTypeTokenStore() );
        temporaryNeoStores = instantiateTempStores();
        instantiateExtensions();

        neoStores.start();
        temporaryNeoStores.start();
    }

    private NeoStores instantiateTempStores()
    {
        return newStoreFactory( temporaryDatabaseLayout, tempIdGeneratorFactory ).openNeoStores( true, TEMP_STORE_TYPES );
    }

    public static BatchingNeoStores batchingNeoStores( FileSystemAbstraction fileSystem, DatabaseLayout databaseLayout,
            RecordFormats recordFormats, Configuration config, LogService logService, AdditionalInitialIds initialIds,
            Config dbConfig, JobScheduler jobScheduler )
    {
        Config neo4jConfig = getNeo4jConfig( config, dbConfig );
        final PageCacheTracer tracer = new DefaultPageCacheTracer();
        PageCache pageCache = createPageCache( fileSystem, neo4jConfig, tracer, jobScheduler );

        return new BatchingNeoStores( fileSystem, pageCache, databaseLayout, recordFormats, neo4jConfig, config, logService,
                initialIds, false, tracer::bytesWritten );
    }

    public static BatchingNeoStores batchingNeoStoresWithExternalPageCache( FileSystemAbstraction fileSystem,
            PageCache pageCache, PageCacheTracer tracer, DatabaseLayout databaseLayout, RecordFormats recordFormats,
            Configuration config, LogService logService, AdditionalInitialIds initialIds, Config dbConfig )
    {
        Config neo4jConfig = getNeo4jConfig( config, dbConfig );

        return new BatchingNeoStores( fileSystem, pageCache, databaseLayout, recordFormats, neo4jConfig, config, logService,
                initialIds, true, tracer::bytesWritten );
    }

    private static Config getNeo4jConfig( Configuration config, Config dbConfig )
    {
        dbConfig.set( pagecache_memory, valueOf( config.pageCacheMemory() ) );
        return dbConfig;
    }

    private static PageCache createPageCache( FileSystemAbstraction fileSystem, Config config, PageCacheTracer tracer, JobScheduler jobScheduler )
    {
        SingleFilePageSwapperFactory swapperFactory = new SingleFilePageSwapperFactory();
        swapperFactory.open( fileSystem );
        MemoryAllocator memoryAllocator = createAllocator( config.get( pagecache_memory ), EmptyMemoryTracker.INSTANCE );
        return new MuninnPageCache( swapperFactory, memoryAllocator, tracer, DefaultPageCursorTracerSupplier.INSTANCE,
                EmptyVersionContextSupplier.EMPTY, jobScheduler );
    }

    private StoreFactory newStoreFactory( DatabaseLayout databaseLayout, IdGeneratorFactory idGeneratorFactory, OpenOption... openOptions )
    {
        return new StoreFactory( databaseLayout, neo4jConfig, idGeneratorFactory, pageCache, fileSystem, recordFormats, logProvider, openOptions );
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

    public RelationshipGroupStore getRelationshipGroupStore()
    {
        return neoStores.getRelationshipGroupStore();
    }

    public CountsTracker getCountsStore()
    {
        return countsStore;
    }

    @Override
    public void close() throws IOException
    {
        // Here as a safety mechanism when e.g. panicking.
        markHighIds();
        if ( flusher != null )
        {
            stopFlushingPageCache();
        }

        flushAndForce();

        // Flush out all pending changes
        closeAll( propertyKeyRepository, labelRepository, relationshipTypeRepository );

        // Close the neo store
        life.shutdown();
        closeAll( countsStoreLife, neoStores, temporaryNeoStores );
        if ( !externalPageCache )
        {
            pageCache.close();
        }

        if ( successful )
        {
            cleanup();
        }
    }

    public void markHighIds()
    {
        if ( neoStores != null )
        {
            idGeneratorFactory.visit( IdGenerator::markHighestWrittenAtHighId );
        }
    }

    private void cleanup() throws IOException
    {
        File tempStoreDirectory = temporaryDatabaseLayout.getStoreLayout().storeDirectory();
        if ( !tempStoreDirectory.getParentFile().equals( databaseLayout.databaseDirectory() ) )
        {
            throw new IllegalStateException( "Temporary store is dislocated. It should be located under current database directory but instead located in: " +
                    tempStoreDirectory.getParent() );
        }
        fileSystem.deleteRecursively( tempStoreDirectory );
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

    public void flushAndForce() throws IOException
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
            neoStores.flush( UNLIMITED );
        }
        if ( temporaryNeoStores != null )
        {
            temporaryNeoStores.flush( UNLIMITED );
        }
        if ( labelScanStore != null )
        {
            labelScanStore.force( UNLIMITED );
        }
    }

    public void success()
    {
        successful = true;
    }

    public boolean determineDoubleRelationshipRecordUnits( Input.Estimates inputEstimates )
    {
        doubleRelationshipRecordUnits =
                recordFormats.hasCapability( RecordStorageCapability.SECONDARY_RECORD_UNITS ) &&
                inputEstimates.numberOfRelationships() > DOUBLE_RELATIONSHIP_RECORD_UNIT_THRESHOLD;
        return doubleRelationshipRecordUnits;
    }

    public boolean usesDoubleRelationshipRecordUnits()
    {
        return doubleRelationshipRecordUnits;
    }
}
