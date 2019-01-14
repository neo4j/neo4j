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
package org.neo4j.kernel.impl.store;

import java.io.File;
import java.io.IOException;
import java.nio.file.NoSuchFileException;
import java.nio.file.OpenOption;
import java.util.Iterator;
import java.util.function.Predicate;

import org.neo4j.graphdb.config.Setting;
import org.neo4j.graphdb.factory.GraphDatabaseSettings;
import org.neo4j.helpers.ArrayUtil;
import org.neo4j.helpers.Exceptions;
import org.neo4j.helpers.collection.FilteringIterator;
import org.neo4j.helpers.collection.IteratorWrapper;
import org.neo4j.helpers.collection.Visitor;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.pagecache.IOLimiter;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.io.pagecache.PagedFile;
import org.neo4j.io.pagecache.tracing.cursor.context.VersionContextSupplier;
import org.neo4j.kernel.NeoStoresDiagnostics;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.impl.api.CountsAccessor;
import org.neo4j.kernel.impl.store.counts.CountsTracker;
import org.neo4j.kernel.impl.store.counts.ReadOnlyCountsTracker;
import org.neo4j.kernel.impl.store.format.CapabilityType;
import org.neo4j.kernel.impl.store.format.FormatFamily;
import org.neo4j.kernel.impl.store.format.RecordFormatSelector;
import org.neo4j.kernel.impl.store.format.RecordFormats;
import org.neo4j.kernel.impl.store.format.standard.MetaDataRecordFormat;
import org.neo4j.kernel.impl.store.id.IdGeneratorFactory;
import org.neo4j.kernel.impl.store.id.IdType;
import org.neo4j.kernel.impl.store.kvstore.DataInitializer;
import org.neo4j.kernel.impl.store.record.AbstractBaseRecord;
import org.neo4j.kernel.info.DiagnosticsManager;
import org.neo4j.logging.Log;
import org.neo4j.logging.LogProvider;
import org.neo4j.logging.Logger;

import static org.neo4j.helpers.collection.Iterators.iterator;
import static org.neo4j.helpers.collection.Iterators.loop;
import static org.neo4j.kernel.impl.store.MetaDataStore.Position.STORE_VERSION;
import static org.neo4j.kernel.impl.store.MetaDataStore.getRecord;
import static org.neo4j.kernel.impl.store.MetaDataStore.versionLongToString;

/**
 * This class contains the references to the "NodeStore,RelationshipStore,
 * PropertyStore and RelationshipTypeStore". NeoStores doesn't actually "store"
 * anything but extends the AbstractStore for the "type and version" validation
 * performed in there.
 */
public class NeoStores implements AutoCloseable
{
    private static final String STORE_ALREADY_CLOSED_MESSAGE = "Specified store was already closed.";
    private static final String STORE_NOT_INITIALIZED_TEMPLATE = "Specified store was not initialized. Please specify" +
                                                                 " %s as one of the stores types that should be open" +
                                                                 " to be able to use it.";

    public static boolean isStorePresent( PageCache pageCache, File storeDir )
    {
        File metaDataStore = new File( storeDir, MetaDataStore.DEFAULT_NAME );
        try ( PagedFile ignore = pageCache.map( metaDataStore, MetaDataStore.getPageSize( pageCache ) ) )
        {
            return true;
        }
        catch ( IOException e )
        {
            return false;
        }
    }

    private static final StoreType[] STORE_TYPES = StoreType.values();

    private final Predicate<StoreType> INSTANTIATED_RECORD_STORES = new Predicate<StoreType>()
    {
        @Override
        public boolean test( StoreType type )
        {
            return type.isRecordStore() && stores[type.ordinal()] != null;
        }
    };

    private final Config config;
    private final IdGeneratorFactory idGeneratorFactory;
    private final PageCache pageCache;
    private final LogProvider logProvider;
    private final VersionContextSupplier versionContextSupplier;
    private final boolean createIfNotExist;
    private final File storeDir;
    private final File neoStoreFileName;
    private final StoreType[] initializedStores;
    private final FileSystemAbstraction fileSystemAbstraction;
    private final RecordFormats recordFormats;
    // All stores, as Object due to CountsTracker being different that all other stores.
    private final Object[] stores;
    private final OpenOption[] openOptions;

    NeoStores(
            File neoStoreFileName,
            Config config,
            IdGeneratorFactory idGeneratorFactory,
            PageCache pageCache,
            final LogProvider logProvider,
            FileSystemAbstraction fileSystemAbstraction,
            VersionContextSupplier versionContextSupplier,
            RecordFormats recordFormats,
            boolean createIfNotExist,
            StoreType[] storeTypes,
            OpenOption[] openOptions )
    {
        this.neoStoreFileName = neoStoreFileName;
        this.config = config;
        this.idGeneratorFactory = idGeneratorFactory;
        this.pageCache = pageCache;
        this.logProvider = logProvider;
        this.fileSystemAbstraction = fileSystemAbstraction;
        this.versionContextSupplier = versionContextSupplier;
        this.recordFormats = recordFormats;
        this.createIfNotExist = createIfNotExist;
        this.openOptions = openOptions;
        this.storeDir = neoStoreFileName.getParentFile();

        verifyRecordFormat();
        stores = new Object[StoreType.values().length];
        try
        {
            for ( StoreType type : storeTypes )
            {
                getOrCreateStore( type );
            }
        }
        catch ( RuntimeException initException )
        {
            try
            {
                close();
            }
            catch ( RuntimeException closeException )
            {
                initException.addSuppressed( closeException );
            }
            throw initException;
        }
        initializedStores = storeTypes;
    }

    public File getStoreDir()
    {
        return storeDir;
    }

    private File getStoreFile( String substoreName )
    {
        return new File( neoStoreFileName.getPath() + substoreName );
    }

    /**
     * Closes the node,relationship,property and relationship type stores.
     */
    @Override
    public void close()
    {
        RuntimeException ex = null;
        for ( StoreType type : STORE_TYPES )
        {
            try
            {
                closeStore( type );
            }
            catch ( RuntimeException t )
            {
                ex = Exceptions.chain( ex, t );
            }
        }

        if ( ex != null )
        {
            throw ex;
        }
    }

    private void verifyRecordFormat()
    {
        try
        {
            String expectedStoreVersion = recordFormats.storeVersion();
            long record = getRecord( pageCache, neoStoreFileName, STORE_VERSION );
            if ( record != MetaDataRecordFormat.FIELD_NOT_PRESENT )
            {
                String actualStoreVersion = versionLongToString( record );
                RecordFormats actualStoreFormat = RecordFormatSelector.selectForVersion( actualStoreVersion );
                if ( !isCompatibleFormats( actualStoreFormat ) )
                {
                    throw new UnexpectedStoreVersionException( actualStoreVersion, expectedStoreVersion );
                }
            }
        }
        catch ( NoSuchFileException e )
        {
            // Occurs when there is no file, which is obviously when creating a store.
            // Caught as an exception because we want to leave as much interaction with files as possible
            // to the page cache.
        }
        catch ( IOException e )
        {
            throw new UnderlyingStorageException( e );
        }
    }

    private boolean isCompatibleFormats( RecordFormats storeFormat )
    {
        return FormatFamily.isSameFamily( recordFormats, storeFormat ) &&
               recordFormats.hasCompatibleCapabilities( storeFormat, CapabilityType.FORMAT ) &&
               recordFormats.generation() >= storeFormat.generation();
    }

    private void closeStore( StoreType type )
    {
        int i = type.ordinal();
        if ( stores[i] != null )
        {
            try
            {
                type.close( this, stores[i] );
            }
            finally
            {
                stores[i] = null;
            }
        }
    }

    public void flush( IOLimiter limiter )
    {
        try
        {
            CountsTracker counts = (CountsTracker) stores[StoreType.COUNTS.ordinal()];
            if ( counts != null )
            {
                counts.rotate( getMetaDataStore().getLastCommittedTransactionId() );
            }
            pageCache.flushAndForce( limiter );
        }
        catch ( IOException e )
        {
            throw new UnderlyingStorageException( "Failed to flush", e );
        }
    }

    private Object openStore( StoreType type )
    {
        int storeIndex = type.ordinal();
        Object store = type.open( this );
        stores[storeIndex] = store;
        return store;
    }

    <T extends CommonAbstractStore> T initialize( T store )
    {
        store.initialise( createIfNotExist );
        return store;
    }

    /**
     * Returns specified store by type from already opened store array. If store is not opened exception will be
     * thrown.
     *
     * @see #getOrCreateStore
     * @param storeType store type to retrieve
     * @return store of requested type
     * @throws IllegalStateException if opened store not found
     */
    private Object getStore( StoreType storeType )
    {
        Object store = stores[storeType.ordinal()];
        if ( store == null )
        {
            String message = ArrayUtil.contains( initializedStores, storeType ) ? STORE_ALREADY_CLOSED_MESSAGE :
                             String.format( STORE_NOT_INITIALIZED_TEMPLATE, storeType.name() );
            throw new IllegalStateException( message );
        }
        return store;
    }

    /**
     * Returns specified store by type from already opened store array. Will open a new store if can't find any.
     * Should be used only during construction of stores.
     *
     * @see #getStore
     * @param storeType store type to get or create
     * @return store of requested type
     */
    private Object getOrCreateStore( StoreType storeType )
    {
        Object store = stores[storeType.ordinal()];
        if ( store == null )
        {
            store = openStore( storeType );
        }
        return store;
    }

    /**
     * @return the NeoStore.
     */
    public MetaDataStore getMetaDataStore()
    {
        return (MetaDataStore) getStore( StoreType.META_DATA );
    }

    /**
     * @return The node store
     */
    public NodeStore getNodeStore()
    {
        return (NodeStore) getStore( StoreType.NODE );
    }

    private DynamicArrayStore getNodeLabelStore()
    {
        return (DynamicArrayStore) getStore( StoreType.NODE_LABEL );
    }

    /**
     * The relationship store.
     *
     * @return The relationship store
     */
    public RelationshipStore getRelationshipStore()
    {
        return (RelationshipStore) getStore( StoreType.RELATIONSHIP );
    }

    /**
     * Returns the relationship type store.
     *
     * @return The relationship type store
     */
    public RelationshipTypeTokenStore getRelationshipTypeTokenStore()
    {
        return (RelationshipTypeTokenStore) getStore( StoreType.RELATIONSHIP_TYPE_TOKEN );
    }

    private DynamicStringStore getRelationshipTypeTokenNamesStore()
    {
        return (DynamicStringStore) getStore( StoreType.RELATIONSHIP_TYPE_TOKEN_NAME );
    }

    /**
     * Returns the label store.
     *
     * @return The label store
     */
    public LabelTokenStore getLabelTokenStore()
    {
        return (LabelTokenStore) getStore( StoreType.LABEL_TOKEN );
    }

    private DynamicStringStore getLabelTokenNamesStore()
    {
        return (DynamicStringStore) getStore( StoreType.LABEL_TOKEN_NAME );
    }

    /**
     * Returns the property store.
     *
     * @return The property store
     */
    public PropertyStore getPropertyStore()
    {
        return (PropertyStore) getStore( StoreType.PROPERTY );
    }

    private DynamicStringStore getStringPropertyStore()
    {
        return (DynamicStringStore) getStore( StoreType.PROPERTY_STRING );
    }

    private DynamicArrayStore getArrayPropertyStore()
    {
        return (DynamicArrayStore) getStore( StoreType.PROPERTY_ARRAY );
    }

    /**
     * @return the {@link PropertyKeyTokenStore}
     */
    public PropertyKeyTokenStore getPropertyKeyTokenStore()
    {
        return (PropertyKeyTokenStore) getStore( StoreType.PROPERTY_KEY_TOKEN );
    }

    private DynamicStringStore getPropertyKeyTokenNamesStore()
    {
        return (DynamicStringStore) getStore( StoreType.PROPERTY_KEY_TOKEN_NAME );
    }

    /**
     * The relationship group store.
     *
     * @return The relationship group store.
     */
    public RelationshipGroupStore getRelationshipGroupStore()
    {
        return (RelationshipGroupStore) getStore( StoreType.RELATIONSHIP_GROUP );
    }

    /**
     * @return the schema store.
     */
    public SchemaStore getSchemaStore()
    {
        return (SchemaStore) getStore( StoreType.SCHEMA );
    }

    public CountsTracker getCounts()
    {
        return (CountsTracker) getStore( StoreType.COUNTS );
    }

    private CountsTracker createWritableCountsTracker( File fileName )
    {
        return new CountsTracker( logProvider, fileSystemAbstraction, pageCache, config, fileName,
                versionContextSupplier );
    }

    private ReadOnlyCountsTracker createReadOnlyCountsTracker( File fileName )
    {
        return new ReadOnlyCountsTracker( logProvider, fileSystemAbstraction, pageCache, config, fileName );
    }

    private Iterable<CommonAbstractStore> instantiatedRecordStores()
    {
        Iterator<StoreType> storeTypes = new FilteringIterator<>( iterator( STORE_TYPES ), INSTANTIATED_RECORD_STORES );
        return loop( new IteratorWrapper<CommonAbstractStore,StoreType>( storeTypes )
        {
            @Override
            protected CommonAbstractStore underlyingObjectToObject( StoreType type )
            {
                return (CommonAbstractStore) stores[type.ordinal()];
            }
        } );
    }

    public void makeStoreOk()
    {
        visitStore( store ->
        {
            store.makeStoreOk();
            return false;
        } );
    }

    /**
     * Throws cause of store not being OK.
     */
    public void verifyStoreOk()
    {
        visitStore( store ->
        {
            store.checkStoreOk();
            return false;
        } );
    }

    public void logVersions( Logger msgLog )
    {
        msgLog.log( "Store versions:" );
        visitStore( store ->
        {
            store.logVersions( msgLog );
            return false;
        } );
    }

    public void logIdUsage( Logger msgLog )
    {
        msgLog.log( "Id usage:" );
        visitStore( store ->
        {
            store.logIdUsage( msgLog );
            return false;
        } );
    }

    /**
     * Visits this store, and any other store managed by this store.
     * TODO this could, and probably should, replace all override-and-do-the-same-thing-to-all-my-managed-stores
     * methods like:
     * {@link #close()} (where that method could be deleted all together, note a specific behaviour of Counts'Store'})
     */
    public void visitStore( Visitor<CommonAbstractStore,RuntimeException> visitor )
    {
        for ( CommonAbstractStore store : instantiatedRecordStores() )
        {
            store.visitStore( visitor );
        }
    }

    public void startCountStore() throws IOException
    {
        // TODO: move this to LifeCycle
        getCounts().start();
    }

    public void deleteIdGenerators()
    {
        visitStore( store ->
        {
            store.deleteIdGenerator();
            return false;
        } );
    }

    public void assertOpen()
    {
        if ( stores[StoreType.NODE.ordinal()] == null )
        {
            throw new IllegalStateException( "Database has been shutdown" );
        }
    }

    CommonAbstractStore createDynamicArrayStore( String storeName, IdType idType, Setting<Integer> blockSizeProperty )
    {
        return createDynamicArrayStore( storeName, idType, config.get( blockSizeProperty ) );
    }

    CommonAbstractStore createDynamicArrayStore( String storeName, IdType idType, int blockSize )
    {
        if ( blockSize <= 0 )
        {
            throw new IllegalArgumentException( "Block size of dynamic array store should be positive integer." );
        }
        File storeFile = getStoreFile( storeName );
        return initialize( new DynamicArrayStore( storeFile, config, idType, idGeneratorFactory, pageCache,
                logProvider, blockSize, recordFormats, openOptions ) );
    }

    CommonAbstractStore createNodeStore( String storeName )
    {
        File storeFile = getStoreFile( storeName );
        return initialize( new NodeStore( storeFile, config, idGeneratorFactory, pageCache, logProvider,
                (DynamicArrayStore) getOrCreateStore( StoreType.NODE_LABEL ), recordFormats, openOptions ) );
    }

    CommonAbstractStore createPropertyKeyTokenStore( String storeName )
    {
        File storeFile = getStoreFile( storeName );
        return initialize( new PropertyKeyTokenStore( storeFile, config, idGeneratorFactory,
                pageCache, logProvider, (DynamicStringStore) getOrCreateStore( StoreType.PROPERTY_KEY_TOKEN_NAME ),
                recordFormats, openOptions ) );
    }

    CommonAbstractStore createPropertyStore( String storeName )
    {
        File storeFile = getStoreFile( storeName );
        return initialize( new PropertyStore( storeFile, config, idGeneratorFactory, pageCache, logProvider,
                (DynamicStringStore) getOrCreateStore( StoreType.PROPERTY_STRING ),
                (PropertyKeyTokenStore) getOrCreateStore( StoreType.PROPERTY_KEY_TOKEN ),
                (DynamicArrayStore) getOrCreateStore( StoreType.PROPERTY_ARRAY ), recordFormats, openOptions ) );
    }

    CommonAbstractStore createRelationshipStore( String storeName )
    {
        File file = getStoreFile( storeName );
        return initialize( new RelationshipStore( file, config, idGeneratorFactory, pageCache, logProvider,
                recordFormats, openOptions ) );
    }

    CommonAbstractStore createDynamicStringStore( String storeName, IdType idType,
            Setting<Integer> blockSizeProperty )
    {
        return createDynamicStringStore( storeName, idType, config.get( blockSizeProperty ) );
    }

    CommonAbstractStore createDynamicStringStore( String storeName, IdType idType, int blockSize )
    {
        File storeFile = getStoreFile( storeName );
        return initialize( new DynamicStringStore( storeFile, config, idType, idGeneratorFactory,
                pageCache, logProvider, blockSize, recordFormats.dynamic(), recordFormats.storeVersion(),
                openOptions ) );
    }

    CommonAbstractStore createRelationshipTypeTokenStore( String storeName )
    {
        File storeFile = getStoreFile( storeName );
        return initialize( new RelationshipTypeTokenStore( storeFile, config, idGeneratorFactory,
                pageCache, logProvider,
                (DynamicStringStore) getOrCreateStore( StoreType.RELATIONSHIP_TYPE_TOKEN_NAME ), recordFormats,
                openOptions ) );
    }

    CommonAbstractStore createLabelTokenStore( String storeName )
    {
        File fileName = getStoreFile( storeName );
        return initialize( new LabelTokenStore( fileName, config, idGeneratorFactory, pageCache,
                logProvider, (DynamicStringStore) getOrCreateStore( StoreType.LABEL_TOKEN_NAME ), recordFormats,
                openOptions ) );
    }

    CommonAbstractStore createSchemaStore( String storeName )
    {
        File fileName = getStoreFile( storeName );
        return initialize( new SchemaStore( fileName, config, IdType.SCHEMA, idGeneratorFactory, pageCache,
                logProvider, recordFormats, openOptions ) );
    }

    CommonAbstractStore createRelationshipGroupStore( String storeName )
    {
        File storeFile = getStoreFile( storeName );
        return initialize( new RelationshipGroupStore( storeFile, config, idGeneratorFactory, pageCache, logProvider,
                recordFormats, openOptions ) );
    }

    CountsTracker createCountStore( String storeName )
    {
        File storeFile = getStoreFile( storeName );
        boolean readOnly = config.get( GraphDatabaseSettings.read_only );
        CountsTracker counts = readOnly
                               ? createReadOnlyCountsTracker( storeFile )
                               : createWritableCountsTracker( storeFile );
        NeoStores neoStores = this;
        counts.setInitializer( new DataInitializer<CountsAccessor.Updater>()
        {
            private final Log log = logProvider.getLog( MetaDataStore.class );

            @Override
            public void initialize( CountsAccessor.Updater updater )
            {
                log.warn( "Missing counts store, rebuilding it." );
                new CountsComputer( neoStores, pageCache ).initialize( updater );
                log.warn( "Counts store rebuild completed." );
            }

            @Override
            public long initialVersion()
            {
                return ((MetaDataStore) getOrCreateStore( StoreType.META_DATA )).getLastCommittedTransactionId();
            }
        } );

        try
        {
            counts.init(); // TODO: move this to LifeCycle
        }
        catch ( IOException e )
        {
            throw new UnderlyingStorageException( "Failed to initialize counts store", e );
        }
        return counts;
    }

    CommonAbstractStore createMetadataStore()
    {
        return initialize( new MetaDataStore( neoStoreFileName, config, idGeneratorFactory, pageCache, logProvider,
                recordFormats.metaData(), recordFormats.storeVersion(), openOptions ) );
    }

    public void registerDiagnostics( DiagnosticsManager diagnosticsManager )
    {
        diagnosticsManager.registerAll( NeoStoresDiagnostics.class, this );
    }

    @SuppressWarnings( "unchecked" )
    public <RECORD extends AbstractBaseRecord> RecordStore<RECORD> getRecordStore( StoreType type )
    {
        assert type.isRecordStore();
        return (RecordStore<RECORD>) getStore( type );
    }

    public RecordFormats getRecordFormats()
    {
        return recordFormats;
    }
}
