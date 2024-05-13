/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [https://neo4j.com]
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
 * along with this program.  If not, see <https://www.gnu.org/licenses/>.
 */
package org.neo4j.kernel.impl.store;

import static org.apache.commons.lang3.ArrayUtils.contains;

import java.io.IOException;
import java.nio.file.OpenOption;
import java.nio.file.Path;
import java.util.function.Consumer;
import org.apache.commons.lang3.mutable.MutableLong;
import org.eclipse.collections.api.set.ImmutableSet;
import org.neo4j.configuration.Config;
import org.neo4j.configuration.GraphDatabaseInternalSettings;
import org.neo4j.function.ThrowingConsumer;
import org.neo4j.graphdb.config.Setting;
import org.neo4j.internal.diagnostics.DiagnosticsLogger;
import org.neo4j.internal.helpers.Exceptions;
import org.neo4j.internal.id.IdGeneratorFactory;
import org.neo4j.internal.id.SchemaIdType;
import org.neo4j.internal.recordstorage.RecordIdType;
import org.neo4j.internal.recordstorage.RecordStorageEngineFactory;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.layout.recordstorage.RecordDatabaseLayout;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.io.pagecache.context.CursorContext;
import org.neo4j.io.pagecache.context.CursorContextFactory;
import org.neo4j.io.pagecache.tracing.DatabaseFlushEvent;
import org.neo4j.io.pagecache.tracing.PageCacheTracer;
import org.neo4j.kernel.impl.store.format.RecordFormats;
import org.neo4j.kernel.impl.store.record.AbstractBaseRecord;
import org.neo4j.kernel.impl.transaction.log.LogTailLogVersionsMetadata;
import org.neo4j.logging.InternalLog;
import org.neo4j.logging.InternalLogProvider;
import org.neo4j.storageengine.api.StoreId;

/**
 * This class contains the references to the "NodeStore,RelationshipStore,
 * PropertyStore and RelationshipTypeStore". NeoStores doesn't actually "store"
 * anything but extends the AbstractStore for the "type and version" validation
 * performed in there.
 */
public class NeoStores implements AutoCloseable {
    private static final String ID_USAGE_LOGGER_TAG = "idUsageLogger";

    private static final String STORE_ALREADY_CLOSED_MESSAGE = "Specified store was already closed.";
    private static final String STORE_NOT_INITIALIZED_TEMPLATE = "Specified store was not initialized. Please specify"
            + " %s as one of the stores types that should be open" + " to be able to use it.";

    private final FileSystemAbstraction fileSystem;
    private final RecordDatabaseLayout layout;
    private final Config config;
    private final IdGeneratorFactory idGeneratorFactory;
    private final PageCache pageCache;
    private final PageCacheTracer pageCacheTracer;
    private final InternalLogProvider logProvider;
    private final CursorContextFactory contextFactory;
    private final StoreType[] initializedStores;
    private final RecordFormats recordFormats;
    private final CommonAbstractStore[] stores;
    private final LogTailLogVersionsMetadata logTailMetadata;
    private final ImmutableSet<OpenOption> openOptions;
    private final boolean readOnly;
    private final InternalLog log;

    NeoStores(
            FileSystemAbstraction fileSystem,
            RecordDatabaseLayout layout,
            Config config,
            IdGeneratorFactory idGeneratorFactory,
            PageCache pageCache,
            PageCacheTracer pageCacheTracer,
            final InternalLogProvider logProvider,
            RecordFormats recordFormats,
            CursorContextFactory contextFactory,
            boolean readOnly,
            LogTailLogVersionsMetadata logTailMetadata,
            StoreType[] storeTypes,
            ImmutableSet<OpenOption> openOptions) {
        this.fileSystem = fileSystem;
        this.layout = layout;
        this.config = config;
        this.idGeneratorFactory = idGeneratorFactory;
        this.pageCache = pageCache;
        this.pageCacheTracer = pageCacheTracer;
        this.logProvider = logProvider;
        this.log = logProvider.getLog(getClass());
        this.recordFormats = recordFormats;
        this.contextFactory = contextFactory;
        this.readOnly = readOnly;
        this.logTailMetadata = logTailMetadata;
        this.openOptions = openOptions;

        stores = new CommonAbstractStore[StoreType.STORE_TYPES.length];
        // First open the meta data store so that we can verify the record format. We know that this store is of the
        // type MetaDataStore
        try {
            for (StoreType type : storeTypes) {
                getOrOpenStore(type);
            }
        } catch (RuntimeException initException) {
            try {
                close();
            } catch (RuntimeException closeException) {
                initException.addSuppressed(closeException);
            }
            throw initException;
        }
        initializedStores = storeTypes;
    }

    /**
     * Closes the node,relationship,property and relationship type stores.
     */
    @Override
    public void close() {
        RuntimeException ex = null;
        for (StoreType type : StoreType.STORE_TYPES) {
            try {
                closeStore(type);
            } catch (RuntimeException t) {
                ex = Exceptions.chain(ex, t);
            }
        }

        if (ex != null) {
            throw ex;
        }
    }

    private void closeStore(StoreType type) {
        int i = type.ordinal();
        if (stores[i] != null) {
            try {
                stores[i].close();
            } finally {
                stores[i] = null;
            }
        }
    }

    public void flush(DatabaseFlushEvent flushEvent, CursorContext cursorContext) throws IOException {
        pageCache.flushAndForce(flushEvent);
        checkpoint(flushEvent, cursorContext);
    }

    public void checkpoint(DatabaseFlushEvent flushEvent, CursorContext cursorContext) throws IOException {
        visitStores(store -> {
            log.debug("Checkpointing %s", store.storageFile.getFileName());
            try (var fileFlushEvent = flushEvent.beginFileFlush()) {
                store.getIdGenerator().checkpoint(fileFlushEvent, cursorContext);
            }
        });
    }

    private CommonAbstractStore openStore(StoreType type) {
        int storeIndex = type.ordinal();
        CommonAbstractStore store = type.open(this);
        stores[storeIndex] = store;
        return store;
    }

    private <T extends CommonAbstractStore> T initialize(T store, CursorContextFactory contextFactory) {
        store.initialise(contextFactory);
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
    private CommonAbstractStore getStore(StoreType storeType) {
        CommonAbstractStore store = stores[storeType.ordinal()];
        if (store == null) {
            String message = contains(initializedStores, storeType)
                    ? STORE_ALREADY_CLOSED_MESSAGE
                    : String.format(STORE_NOT_INITIALIZED_TEMPLATE, storeType.name());
            throw new IllegalStateException(message);
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
    private CommonAbstractStore getOrOpenStore(StoreType storeType) {
        CommonAbstractStore store = stores[storeType.ordinal()];
        if (store == null) {
            store = openStore(storeType);
        }
        return store;
    }

    /**
     * @return the NeoStore.
     */
    public MetaDataStore getMetaDataStore() {
        return (MetaDataStore) getStore(StoreType.META_DATA);
    }

    /**
     * @return The node store
     */
    public NodeStore getNodeStore() {
        return (NodeStore) getStore(StoreType.NODE);
    }

    /**
     * The relationship store.
     *
     * @return The relationship store
     */
    public RelationshipStore getRelationshipStore() {
        return (RelationshipStore) getStore(StoreType.RELATIONSHIP);
    }

    /**
     * Returns the relationship type store.
     *
     * @return The relationship type store
     */
    public RelationshipTypeTokenStore getRelationshipTypeTokenStore() {
        return (RelationshipTypeTokenStore) getStore(StoreType.RELATIONSHIP_TYPE_TOKEN);
    }

    /**
     * Returns the label store.
     *
     * @return The label store
     */
    public LabelTokenStore getLabelTokenStore() {
        return (LabelTokenStore) getStore(StoreType.LABEL_TOKEN);
    }

    /**
     * Returns the property store.
     *
     * @return The property store
     */
    public PropertyStore getPropertyStore() {
        return (PropertyStore) getStore(StoreType.PROPERTY);
    }

    /**
     * @return the {@link PropertyKeyTokenStore}
     */
    public PropertyKeyTokenStore getPropertyKeyTokenStore() {
        return (PropertyKeyTokenStore) getStore(StoreType.PROPERTY_KEY_TOKEN);
    }

    /**
     * The relationship group store.
     *
     * @return The relationship group store.
     */
    public RelationshipGroupStore getRelationshipGroupStore() {
        return (RelationshipGroupStore) getStore(StoreType.RELATIONSHIP_GROUP);
    }

    /**
     * @return the schema store.
     */
    public SchemaStore getSchemaStore() {
        return (SchemaStore) getStore(StoreType.SCHEMA);
    }

    public void start(CursorContext cursorContext) throws IOException {
        start(store -> {}, cursorContext);
    }

    public void start(Consumer<CommonAbstractStore<?, ?>> listener, CursorContext cursorContext) throws IOException {
        visitStores(store -> {
            store.start(cursorContext);
            listener.accept(store);
        });
    }

    /**
     * Throws cause of store not being OK.
     */
    public void verifyStoreOk() {
        visitStores(CommonAbstractStore::checkStoreOk);
    }

    public void logIdUsage(DiagnosticsLogger msgLog) {
        try (var cursorContext = contextFactory.create(ID_USAGE_LOGGER_TAG)) {
            visitStores(store -> store.logIdUsage(msgLog, cursorContext));
        }
    }

    /**
     * Visits this store, and any other store managed by this store.
     * TODO this could, and probably should, replace all override-and-do-the-same-thing-to-all-my-managed-stores
     * methods like:
     * {@link #close()} (where that method could be deleted all together, note a specific behaviour of Counts'Store'})
     */
    private <E extends Exception> void visitStores(ThrowingConsumer<CommonAbstractStore, E> visitor) throws E {
        for (CommonAbstractStore store : stores) {
            if (store != null) {
                visitor.accept(store);
            }
        }
    }

    CommonAbstractStore createNodeStore() {
        return initialize(
                new NodeStore(
                        fileSystem,
                        layout.nodeStore(),
                        layout.idNodeStore(),
                        config,
                        idGeneratorFactory,
                        pageCache,
                        pageCacheTracer,
                        logProvider,
                        (DynamicArrayStore) getOrOpenStore(StoreType.NODE_LABEL),
                        recordFormats,
                        readOnly,
                        layout.getDatabaseName(),
                        openOptions),
                contextFactory);
    }

    CommonAbstractStore createNodeLabelStore() {
        return createDynamicArrayStore(
                layout.nodeLabelStore(),
                layout.idNodeLabelStore(),
                RecordIdType.NODE_LABELS,
                GraphDatabaseInternalSettings.label_block_size);
    }

    CommonAbstractStore createPropertyKeyTokenStore() {
        return initialize(
                new PropertyKeyTokenStore(
                        fileSystem,
                        layout.propertyKeyTokenStore(),
                        layout.idPropertyKeyTokenStore(),
                        config,
                        idGeneratorFactory,
                        pageCache,
                        pageCacheTracer,
                        logProvider,
                        (DynamicStringStore) getOrOpenStore(StoreType.PROPERTY_KEY_TOKEN_NAME),
                        recordFormats,
                        readOnly,
                        layout.getDatabaseName(),
                        openOptions),
                contextFactory);
    }

    CommonAbstractStore createPropertyKeyTokenNamesStore() {
        return createDynamicStringStore(
                layout.propertyKeyTokenNamesStore(),
                layout.idPropertyKeyTokenNamesStore(),
                RecordIdType.PROPERTY_KEY_TOKEN_NAME,
                TokenStore.NAME_STORE_BLOCK_SIZE);
    }

    CommonAbstractStore createPropertyStore() {
        return initialize(
                new PropertyStore(
                        fileSystem,
                        layout.propertyStore(),
                        layout.idPropertyStore(),
                        config,
                        idGeneratorFactory,
                        pageCache,
                        pageCacheTracer,
                        logProvider,
                        (DynamicStringStore) getOrOpenStore(StoreType.PROPERTY_STRING),
                        (PropertyKeyTokenStore) getOrOpenStore(StoreType.PROPERTY_KEY_TOKEN),
                        (DynamicArrayStore) getOrOpenStore(StoreType.PROPERTY_ARRAY),
                        recordFormats,
                        readOnly,
                        layout.getDatabaseName(),
                        openOptions),
                contextFactory);
    }

    CommonAbstractStore createPropertyStringStore() {
        return createDynamicStringStore(layout.propertyStringStore(), layout.idPropertyStringStore());
    }

    CommonAbstractStore createPropertyArrayStore() {
        return createDynamicArrayStore(
                layout.propertyArrayStore(),
                layout.idPropertyArrayStore(),
                RecordIdType.ARRAY_BLOCK,
                GraphDatabaseInternalSettings.array_block_size);
    }

    CommonAbstractStore createRelationshipStore() {
        return initialize(
                new RelationshipStore(
                        fileSystem,
                        layout.relationshipStore(),
                        layout.idRelationshipStore(),
                        config,
                        idGeneratorFactory,
                        pageCache,
                        pageCacheTracer,
                        logProvider,
                        recordFormats,
                        readOnly,
                        layout.getDatabaseName(),
                        openOptions),
                contextFactory);
    }

    CommonAbstractStore createRelationshipTypeTokenStore() {
        return initialize(
                new RelationshipTypeTokenStore(
                        fileSystem,
                        layout.relationshipTypeTokenStore(),
                        layout.idRelationshipTypeTokenStore(),
                        config,
                        idGeneratorFactory,
                        pageCache,
                        pageCacheTracer,
                        logProvider,
                        (DynamicStringStore) getOrOpenStore(StoreType.RELATIONSHIP_TYPE_TOKEN_NAME),
                        recordFormats,
                        readOnly,
                        layout.getDatabaseName(),
                        openOptions),
                contextFactory);
    }

    CommonAbstractStore createRelationshipTypeTokenNamesStore() {
        return createDynamicStringStore(
                layout.relationshipTypeTokenNamesStore(),
                layout.idRelationshipTypeTokenNamesStore(),
                RecordIdType.RELATIONSHIP_TYPE_TOKEN_NAME,
                TokenStore.NAME_STORE_BLOCK_SIZE);
    }

    CommonAbstractStore createLabelTokenStore() {
        return initialize(
                new LabelTokenStore(
                        fileSystem,
                        layout.labelTokenStore(),
                        layout.idLabelTokenStore(),
                        config,
                        idGeneratorFactory,
                        pageCache,
                        pageCacheTracer,
                        logProvider,
                        (DynamicStringStore) getOrOpenStore(StoreType.LABEL_TOKEN_NAME),
                        recordFormats,
                        readOnly,
                        layout.getDatabaseName(),
                        openOptions),
                contextFactory);
    }

    CommonAbstractStore createSchemaStore() {
        return initialize(
                new SchemaStore(
                        fileSystem,
                        layout.schemaStore(),
                        layout.idSchemaStore(),
                        config,
                        SchemaIdType.SCHEMA,
                        idGeneratorFactory,
                        pageCache,
                        pageCacheTracer,
                        logProvider,
                        (PropertyStore) getOrOpenStore(StoreType.PROPERTY),
                        recordFormats,
                        readOnly,
                        layout.getDatabaseName(),
                        openOptions),
                contextFactory);
    }

    CommonAbstractStore createRelationshipGroupStore() {
        return initialize(
                new RelationshipGroupStore(
                        fileSystem,
                        layout.relationshipGroupStore(),
                        layout.idRelationshipGroupStore(),
                        config,
                        idGeneratorFactory,
                        pageCache,
                        pageCacheTracer,
                        logProvider,
                        recordFormats,
                        readOnly,
                        layout.getDatabaseName(),
                        openOptions),
                contextFactory);
    }

    CommonAbstractStore createLabelTokenNamesStore() {
        return createDynamicStringStore(
                layout.labelTokenNamesStore(),
                layout.idLabelTokenNamesStore(),
                RecordIdType.LABEL_TOKEN_NAME,
                TokenStore.NAME_STORE_BLOCK_SIZE);
    }

    CommonAbstractStore createMetadataStore() {
        return initialize(
                new MetaDataStore(
                        fileSystem,
                        layout.metadataStore(),
                        config,
                        pageCache,
                        pageCacheTracer,
                        logProvider,
                        recordFormats.metaData(),
                        readOnly,
                        logTailMetadata,
                        layout.getDatabaseName(),
                        openOptions,
                        () -> StoreId.generateNew(
                                RecordStorageEngineFactory.NAME,
                                recordFormats.getFormatFamily().name(),
                                recordFormats.majorVersion(),
                                recordFormats.minorVersion())),
                contextFactory);
    }

    private CommonAbstractStore createDynamicStringStore(Path storeFile, Path idFile) {
        return createDynamicStringStore(
                storeFile,
                idFile,
                RecordIdType.STRING_BLOCK,
                config.get(GraphDatabaseInternalSettings.string_block_size));
    }

    private CommonAbstractStore createDynamicStringStore(
            Path storeFile, Path idFile, RecordIdType idType, int blockSize) {
        return initialize(
                new DynamicStringStore(
                        fileSystem,
                        storeFile,
                        idFile,
                        config,
                        idType,
                        idGeneratorFactory,
                        pageCache,
                        pageCacheTracer,
                        logProvider,
                        blockSize,
                        recordFormats.dynamic(),
                        readOnly,
                        layout.getDatabaseName(),
                        openOptions),
                contextFactory);
    }

    private CommonAbstractStore createDynamicArrayStore(
            Path storeFile, Path idFile, RecordIdType idType, Setting<Integer> blockSizeProperty) {
        return createDynamicArrayStore(storeFile, idFile, idType, config.get(blockSizeProperty));
    }

    CommonAbstractStore createDynamicArrayStore(Path storeFile, Path idFile, RecordIdType idType, int blockSize) {
        if (blockSize <= 0) {
            throw new IllegalArgumentException("Block size of dynamic array store should be positive integer.");
        }
        return initialize(
                new DynamicArrayStore(
                        fileSystem,
                        storeFile,
                        idFile,
                        config,
                        idType,
                        idGeneratorFactory,
                        pageCache,
                        pageCacheTracer,
                        logProvider,
                        blockSize,
                        recordFormats,
                        readOnly,
                        layout.getDatabaseName(),
                        openOptions),
                contextFactory);
    }

    @SuppressWarnings("unchecked")
    public <RECORD extends AbstractBaseRecord> RecordStore<RECORD> getRecordStore(StoreType type) {
        return getStore(type);
    }

    public RecordFormats getRecordFormats() {
        return recordFormats;
    }

    public ImmutableSet<OpenOption> getOpenOptions() {
        return openOptions;
    }

    public static boolean isStorePresent(FileSystemAbstraction fs, RecordDatabaseLayout databaseLayout) {
        return fs.fileExists(databaseLayout.pathForExistsMarker());
    }

    public long estimateAvailableReservedSpace() {
        final var bytes = new MutableLong();
        visitStores(store -> bytes.add(store.estimateAvailableReservedSpace()));
        return bytes.longValue();
    }
}
