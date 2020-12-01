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
package org.neo4j.kernel.impl.store;

import org.eclipse.collections.api.set.ImmutableSet;

import java.io.IOException;
import java.nio.file.NoSuchFileException;
import java.nio.file.OpenOption;
import java.nio.file.Path;
import java.util.function.Consumer;

import org.neo4j.configuration.Config;
import org.neo4j.configuration.GraphDatabaseInternalSettings;
import org.neo4j.exceptions.UnderlyingStorageException;
import org.neo4j.function.ThrowingConsumer;
import org.neo4j.graphdb.config.Setting;
import org.neo4j.internal.diagnostics.DiagnosticsLogger;
import org.neo4j.internal.helpers.Exceptions;
import org.neo4j.internal.id.IdGeneratorFactory;
import org.neo4j.internal.id.IdType;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.layout.DatabaseLayout;
import org.neo4j.io.pagecache.IOLimiter;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.io.pagecache.tracing.PageCacheTracer;
import org.neo4j.io.pagecache.tracing.cursor.PageCursorTracer;
import org.neo4j.kernel.impl.store.format.FormatFamily;
import org.neo4j.kernel.impl.store.format.RecordFormatSelector;
import org.neo4j.kernel.impl.store.format.RecordFormats;
import org.neo4j.kernel.impl.store.format.standard.MetaDataRecordFormat;
import org.neo4j.kernel.impl.store.record.AbstractBaseRecord;
import org.neo4j.kernel.impl.store.record.RecordLoad;
import org.neo4j.logging.LogProvider;
import org.neo4j.storageengine.api.format.CapabilityType;

import static org.apache.commons.lang3.ArrayUtils.contains;
import static org.neo4j.kernel.impl.store.MetaDataStore.Position.STORE_VERSION;
import static org.neo4j.kernel.impl.store.MetaDataStore.versionLongToString;

/**
 * This class contains the references to the "NodeStore,RelationshipStore,
 * PropertyStore and RelationshipTypeStore". NeoStores doesn't actually "store"
 * anything but extends the AbstractStore for the "type and version" validation
 * performed in there.
 */
public class NeoStores implements AutoCloseable
{
    private static final String ID_USAGE_LOGGER_TAG = "idUsageLogger";

    private static final String STORE_ALREADY_CLOSED_MESSAGE = "Specified store was already closed.";
    private static final String STORE_NOT_INITIALIZED_TEMPLATE = "Specified store was not initialized. Please specify" +
                                                                 " %s as one of the stores types that should be open" +
                                                                 " to be able to use it.";
    private static final String OPEN_ALL_STORES_TAG = "openAllStores";

    private static final StoreType[] STORE_TYPES = StoreType.values();

    private final FileSystemAbstraction fileSystem;
    private final DatabaseLayout layout;
    private final Config config;
    private final IdGeneratorFactory idGeneratorFactory;
    private final PageCache pageCache;
    private final LogProvider logProvider;
    private final boolean createIfNotExist;
    private final StoreType[] initializedStores;
    private final RecordFormats recordFormats;
    private final CommonAbstractStore[] stores;
    private final PageCacheTracer pageCacheTracer;
    private final ImmutableSet<OpenOption> openOptions;

    NeoStores(
            FileSystemAbstraction fileSystem,
            DatabaseLayout layout,
            Config config,
            IdGeneratorFactory idGeneratorFactory,
            PageCache pageCache,
            final LogProvider logProvider,
            RecordFormats recordFormats,
            boolean createIfNotExist,
            PageCacheTracer pageCacheTracer,
            StoreType[] storeTypes,
            ImmutableSet<OpenOption> openOptions )
    {
        this.fileSystem = fileSystem;
        this.layout = layout;
        this.config = config;
        this.idGeneratorFactory = idGeneratorFactory;
        this.pageCache = pageCache;
        this.logProvider = logProvider;
        this.recordFormats = recordFormats;
        this.createIfNotExist = createIfNotExist;
        this.pageCacheTracer = pageCacheTracer;
        this.openOptions = openOptions;

        stores = new CommonAbstractStore[StoreType.values().length];
        // First open the meta data store so that we can verify the record format. We know that this store is of the type MetaDataStore
        try ( var cursorTracer = pageCacheTracer.createPageCursorTracer( OPEN_ALL_STORES_TAG ) )
        {
            verifyRecordFormat( storeTypes, cursorTracer );
            try
            {
                for ( StoreType type : storeTypes )
                {
                    getOrOpenStore( type, cursorTracer );
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
        }
        initializedStores = storeTypes;
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

    private void verifyRecordFormat( StoreType[] storeTypes, PageCursorTracer cursorTracer )
    {
        String expectedStoreVersion = recordFormats.storeVersion();
        long existingFormat;
        if ( !fileSystem.fileExists( layout.metadataStore() ) )
        {
            // If the meta data store doesn't even exist then look no further, there's nothing to verify
            return;
        }

        if ( contains( storeTypes, StoreType.META_DATA ) )
        {
            // We're going to open this store anyway so might as well do it here, like we open the others
            MetaDataStore metaDataStore = (MetaDataStore) getOrOpenStore( StoreType.META_DATA, cursorTracer );
            existingFormat = metaDataStore.getRecord( STORE_VERSION.id(), metaDataStore.newRecord(), RecordLoad.CHECK, cursorTracer ).getValue();
        }
        else
        {
            // We're not quite expected to open this store among the other stores, so instead just read the single version record
            // from the meta data store and be done with it. This will avoid the unwanted side-effect of creating the meta data store
            // if we have createIfNotExists set, but don't have the meta data store in the list of stores to open
            try
            {
                existingFormat = MetaDataStore.getRecord( pageCache, layout.metadataStore(), STORE_VERSION, cursorTracer );
            }
            catch ( NoSuchFileException e )
            {
                // Weird that the file isn't here after we passed the exists check above. Regardless, treat this the same way as above.
                return;
            }
            catch ( IOException e )
            {
                throw new UnderlyingStorageException( e );
            }
        }

        if ( existingFormat != MetaDataRecordFormat.FIELD_NOT_PRESENT )
        {
            String actualStoreVersion = versionLongToString( existingFormat );
            RecordFormats actualStoreFormat = RecordFormatSelector.selectForVersion( actualStoreVersion );
            if ( !isCompatibleFormats( actualStoreFormat ) )
            {
                throw new UnexpectedStoreVersionException( actualStoreVersion, expectedStoreVersion );
            }
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
                stores[i].close();
            }
            finally
            {
                stores[i] = null;
            }
        }
    }

    public void flush( IOLimiter limiter, PageCursorTracer cursorTracer ) throws IOException
    {
        // The thing about flush here is that it won't invoke flush on each individual store and this is because calling
        // the flushAndForce method on the MuninnPageCache has the opportunity to flush things in parallel, something that
        // flushing individual files would not have, or at least it would be a bit more complicated to do.
        // Therefore this method will have to manually checkpoint all the id generators of all stores.
        // The reason we don't use IdGeneratorFactory to get all the IdGenerators is that the design of it and where it sits
        // architecturally makes it fragile and silently overwriting IdGenerator instances from other databases,
        // it's weird I know. The most stable and secure thing we can do is to invoke this on the IdGenerator instances
        // that our stores reference.

        pageCache.flushAndForce( limiter );
        visitStores( store -> store.getIdGenerator().checkpoint( limiter, cursorTracer ) );
    }

    private CommonAbstractStore openStore( StoreType type, PageCursorTracer cursorTracer )
    {
        int storeIndex = type.ordinal();
        CommonAbstractStore store = type.open( this, cursorTracer );
        stores[storeIndex] = store;
        return store;
    }

    private <T extends CommonAbstractStore> T initialize( T store, PageCursorTracer cursorTracer )
    {
        store.initialise( createIfNotExist, cursorTracer );
        return store;
    }

    /**
     * Returns specified store by type from already opened store array. If store is not opened exception will be
     * thrown.
     *
     * @see #getOrOpenStore
     * @param storeType store type to retrieve
     * @return store of requested type
     * @throws IllegalStateException if opened store not found
     */
    private CommonAbstractStore getStore( StoreType storeType )
    {
        CommonAbstractStore store = stores[storeType.ordinal()];
        if ( store == null )
        {
            String message = contains( initializedStores, storeType ) ? STORE_ALREADY_CLOSED_MESSAGE :
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
    private CommonAbstractStore getOrOpenStore( StoreType storeType, PageCursorTracer cursorTracer )
    {
        CommonAbstractStore store = stores[storeType.ordinal()];
        if ( store == null )
        {
            store = openStore( storeType, cursorTracer );
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

    /**
     * Returns the label store.
     *
     * @return The label store
     */
    public LabelTokenStore getLabelTokenStore()
    {
        return (LabelTokenStore) getStore( StoreType.LABEL_TOKEN );
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

    /**
     * @return the {@link PropertyKeyTokenStore}
     */
    public PropertyKeyTokenStore getPropertyKeyTokenStore()
    {
        return (PropertyKeyTokenStore) getStore( StoreType.PROPERTY_KEY_TOKEN );
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

    public void start( PageCursorTracer cursorTracer ) throws IOException
    {
        start( store -> {}, cursorTracer );
    }

    public void start( Consumer<CommonAbstractStore<?,?>> listener, PageCursorTracer cursorTracer ) throws IOException
    {
        visitStores( store ->
        {
            store.start( cursorTracer );
            listener.accept( store );
        } );
    }

    /**
     * Throws cause of store not being OK.
     */
    public void verifyStoreOk()
    {
        visitStores( CommonAbstractStore::checkStoreOk );
    }

    public void logVersions( DiagnosticsLogger msgLog )
    {
        visitStores( store -> store.logVersions( msgLog ) );
    }

    public void logIdUsage( DiagnosticsLogger msgLog )
    {
        try ( var cursorTracer = pageCacheTracer.createPageCursorTracer( ID_USAGE_LOGGER_TAG ) )
        {
            visitStores( store -> store.logIdUsage( msgLog, cursorTracer ) );
        }
    }

    /**
     * Visits this store, and any other store managed by this store.
     * TODO this could, and probably should, replace all override-and-do-the-same-thing-to-all-my-managed-stores
     * methods like:
     * {@link #close()} (where that method could be deleted all together, note a specific behaviour of Counts'Store'})
     */
    private <E extends Exception> void visitStores( ThrowingConsumer<CommonAbstractStore,E> visitor ) throws E
    {
        for ( CommonAbstractStore store : stores )
        {
            if ( store != null )
            {
                visitor.accept( store );
            }
        }
    }

    CommonAbstractStore createNodeStore( PageCursorTracer cursorTracer )
    {
        return initialize(
                new NodeStore( layout.nodeStore(), layout.idNodeStore(), config, idGeneratorFactory, pageCache, logProvider,
                        (DynamicArrayStore) getOrOpenStore( StoreType.NODE_LABEL, cursorTracer ), recordFormats, openOptions ), cursorTracer );
    }

    CommonAbstractStore createNodeLabelStore( PageCursorTracer cursorTracer )
    {
        return createDynamicArrayStore( layout.nodeLabelStore(), layout.idNodeLabelStore(), IdType.NODE_LABELS,
                GraphDatabaseInternalSettings.label_block_size, cursorTracer );
    }

    CommonAbstractStore createPropertyKeyTokenStore( PageCursorTracer cursorTracer )
    {
        return initialize( new PropertyKeyTokenStore( layout.propertyKeyTokenStore(), layout.idPropertyKeyTokenStore(), config,
                idGeneratorFactory, pageCache, logProvider, (DynamicStringStore) getOrOpenStore( StoreType.PROPERTY_KEY_TOKEN_NAME, cursorTracer ),
                recordFormats, openOptions ), cursorTracer );
    }

    CommonAbstractStore createPropertyKeyTokenNamesStore( PageCursorTracer cursorTracer )
    {
        return createDynamicStringStore( layout.propertyKeyTokenNamesStore(), layout.idPropertyKeyTokenNamesStore(),
                IdType.PROPERTY_KEY_TOKEN_NAME, TokenStore.NAME_STORE_BLOCK_SIZE, cursorTracer );
    }

    CommonAbstractStore createPropertyStore( PageCursorTracer cursorTracer )
    {
        return initialize( new PropertyStore( layout.propertyStore(), layout.idPropertyStore(), config, idGeneratorFactory, pageCache,
                logProvider, (DynamicStringStore) getOrOpenStore( StoreType.PROPERTY_STRING, cursorTracer ),
                (PropertyKeyTokenStore) getOrOpenStore( StoreType.PROPERTY_KEY_TOKEN, cursorTracer ),
                (DynamicArrayStore) getOrOpenStore( StoreType.PROPERTY_ARRAY, cursorTracer ), recordFormats, openOptions ), cursorTracer );
    }

    CommonAbstractStore createPropertyStringStore( PageCursorTracer cursorTracer )
    {
        return createDynamicStringStore( layout.propertyStringStore(), layout.idPropertyStringStore(), cursorTracer );
    }

    CommonAbstractStore createPropertyArrayStore( PageCursorTracer cursorTracer )
    {
        return createDynamicArrayStore( layout.propertyArrayStore(), layout.idPropertyArrayStore(), IdType.ARRAY_BLOCK,
                GraphDatabaseInternalSettings.array_block_size, cursorTracer );
    }

    CommonAbstractStore createRelationshipStore( PageCursorTracer cursorTracer )
    {
        return initialize(
                new RelationshipStore( layout.relationshipStore(), layout.idRelationshipStore(), config, idGeneratorFactory,
                        pageCache, logProvider, recordFormats, openOptions ), cursorTracer );
    }

    CommonAbstractStore createRelationshipTypeTokenStore( PageCursorTracer cursorTracer )
    {
        return initialize(
                new RelationshipTypeTokenStore( layout.relationshipTypeTokenStore(), layout.idRelationshipTypeTokenStore(), config,
                        idGeneratorFactory,
                        pageCache, logProvider, (DynamicStringStore) getOrOpenStore( StoreType.RELATIONSHIP_TYPE_TOKEN_NAME, cursorTracer ),
                        recordFormats, openOptions ), cursorTracer );
    }

    CommonAbstractStore createRelationshipTypeTokenNamesStore( PageCursorTracer cursorTracer )
    {
        return createDynamicStringStore( layout.relationshipTypeTokenNamesStore(), layout.idRelationshipTypeTokenNamesStore(),
                IdType.RELATIONSHIP_TYPE_TOKEN_NAME, TokenStore.NAME_STORE_BLOCK_SIZE, cursorTracer );
    }

    CommonAbstractStore createLabelTokenStore( PageCursorTracer cursorTracer )
    {
        return initialize(
                new LabelTokenStore( layout.labelTokenStore(), layout.idLabelTokenStore(), config, idGeneratorFactory, pageCache,
                        logProvider, (DynamicStringStore) getOrOpenStore( StoreType.LABEL_TOKEN_NAME, cursorTracer ), recordFormats, openOptions ),
                cursorTracer );
    }

    CommonAbstractStore createSchemaStore( PageCursorTracer cursorTracer )
    {
        return initialize(
                new SchemaStore( layout.schemaStore(), layout.idSchemaStore(), config, IdType.SCHEMA, idGeneratorFactory, pageCache,
                        logProvider,
                        (PropertyStore) getOrOpenStore( StoreType.PROPERTY, cursorTracer ),
                        recordFormats, openOptions ), cursorTracer );
    }

    CommonAbstractStore createRelationshipGroupStore( PageCursorTracer cursorTracer )
    {
        return initialize( new RelationshipGroupStore( layout.relationshipGroupStore(), layout.idRelationshipGroupStore(), config,
                idGeneratorFactory, pageCache, logProvider, recordFormats, openOptions ), cursorTracer );
    }

    CommonAbstractStore createLabelTokenNamesStore( PageCursorTracer cursorTracer )
    {
        return createDynamicStringStore( layout.labelTokenNamesStore(), layout.idLabelTokenNamesStore(), IdType.LABEL_TOKEN_NAME,
                TokenStore.NAME_STORE_BLOCK_SIZE, cursorTracer );
    }

    CommonAbstractStore createMetadataStore( PageCursorTracer cursorTracer )
    {
        return initialize(
                new MetaDataStore( layout.metadataStore(), layout.idMetadataStore(), config, idGeneratorFactory, pageCache, logProvider,
                        recordFormats.metaData(), recordFormats.storeVersion(), pageCacheTracer, openOptions ), cursorTracer );
    }

    private CommonAbstractStore createDynamicStringStore( Path storeFile, Path idFile, PageCursorTracer cursorTracer )
    {
        return createDynamicStringStore( storeFile, idFile, IdType.STRING_BLOCK, config.get( GraphDatabaseInternalSettings.string_block_size ), cursorTracer );
    }

    private CommonAbstractStore createDynamicStringStore( Path storeFile, Path idFile, IdType idType, int blockSize, PageCursorTracer cursorTracer )
    {
        return initialize( new DynamicStringStore( storeFile, idFile, config, idType, idGeneratorFactory,
                pageCache, logProvider, blockSize, recordFormats.dynamic(), recordFormats.storeVersion(),
                openOptions ), cursorTracer );
    }

    private CommonAbstractStore createDynamicArrayStore( Path storeFile, Path idFile, IdType idType, Setting<Integer> blockSizeProperty,
            PageCursorTracer cursorTracer )
    {
        return createDynamicArrayStore( storeFile, idFile, idType, config.get( blockSizeProperty ), cursorTracer );
    }

    CommonAbstractStore createDynamicArrayStore( Path storeFile, Path idFile, IdType idType, int blockSize, PageCursorTracer cursorTracer )
    {
        if ( blockSize <= 0 )
        {
            throw new IllegalArgumentException( "Block size of dynamic array store should be positive integer." );
        }
        return initialize( new DynamicArrayStore( storeFile, idFile, config, idType, idGeneratorFactory, pageCache,
                logProvider, blockSize, recordFormats, openOptions ), cursorTracer );
    }

    @SuppressWarnings( "unchecked" )
    public <RECORD extends AbstractBaseRecord> RecordStore<RECORD> getRecordStore( StoreType type )
    {
        return getStore( type );
    }

    public RecordFormats getRecordFormats()
    {
        return recordFormats;
    }

    public static boolean isStorePresent( FileSystemAbstraction fs, DatabaseLayout databaseLayout )
    {
        return fs.fileExists( databaseLayout.metadataStore() );
    }
}
