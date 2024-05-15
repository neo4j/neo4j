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
package org.neo4j.consistency.checking;

import static java.lang.System.currentTimeMillis;
import static org.neo4j.configuration.GraphDatabaseSettings.DEFAULT_DATABASE_NAME;
import static org.neo4j.internal.kernel.api.TokenRead.ANY_LABEL;
import static org.neo4j.internal.recordstorage.RecordCursorTypes.PROPERTY_CURSOR;
import static org.neo4j.internal.recordstorage.StoreTokens.allReadableTokens;
import static org.neo4j.io.pagecache.context.CursorContext.NULL_CONTEXT;
import static org.neo4j.kernel.impl.store.record.Record.NO_LABELS_FIELD;
import static org.neo4j.kernel.impl.store.record.Record.NO_NEXT_PROPERTY;
import static org.neo4j.kernel.impl.store.record.Record.NO_NEXT_RELATIONSHIP;
import static org.neo4j.memory.EmptyMemoryTracker.INSTANCE;
import static org.neo4j.storageengine.api.PropertySelection.ALL_PROPERTIES;
import static org.neo4j.storageengine.api.TransactionIdStore.UNKNOWN_CONSENSUS_INDEX;

import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;
import org.apache.commons.lang3.mutable.MutableInt;
import org.neo4j.common.DependencyResolver;
import org.neo4j.configuration.GraphDatabaseInternalSettings;
import org.neo4j.configuration.GraphDatabaseSettings;
import org.neo4j.consistency.checking.index.IndexAccessors;
import org.neo4j.consistency.store.DirectStoreAccess;
import org.neo4j.dbms.api.DatabaseManagementService;
import org.neo4j.exceptions.KernelException;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.config.Setting;
import org.neo4j.internal.helpers.collection.Iterables;
import org.neo4j.internal.id.IdGeneratorFactory;
import org.neo4j.internal.recordstorage.DirectRecordAccess;
import org.neo4j.internal.recordstorage.Loaders;
import org.neo4j.internal.recordstorage.RecordStorageEngine;
import org.neo4j.internal.recordstorage.RecordStorageReader;
import org.neo4j.internal.schema.IndexDescriptor;
import org.neo4j.internal.schema.SchemaRule;
import org.neo4j.io.layout.DatabaseLayout;
import org.neo4j.io.layout.Neo4jLayout;
import org.neo4j.kernel.impl.api.CommandCommitListeners;
import org.neo4j.kernel.impl.api.InternalTransactionCommitProcess;
import org.neo4j.kernel.impl.api.TransactionToApply;
import org.neo4j.kernel.impl.api.index.IndexProviderMap;
import org.neo4j.kernel.impl.api.index.IndexProxy;
import org.neo4j.kernel.impl.api.index.IndexingService;
import org.neo4j.kernel.impl.api.txid.IdStoreTransactionIdGenerator;
import org.neo4j.kernel.impl.store.DynamicAllocatorProvider;
import org.neo4j.kernel.impl.store.DynamicAllocatorProviders;
import org.neo4j.kernel.impl.store.InlineNodeLabels;
import org.neo4j.kernel.impl.store.NeoStores;
import org.neo4j.kernel.impl.store.NodeLabelsField;
import org.neo4j.kernel.impl.store.NodeStore;
import org.neo4j.kernel.impl.store.PropertyStore;
import org.neo4j.kernel.impl.store.RecordStore;
import org.neo4j.kernel.impl.store.StoreType;
import org.neo4j.kernel.impl.store.TokenStore;
import org.neo4j.kernel.impl.store.cursor.CachedStoreCursors;
import org.neo4j.kernel.impl.store.format.aligned.PageAligned;
import org.neo4j.kernel.impl.store.record.AbstractBaseRecord;
import org.neo4j.kernel.impl.store.record.NodeRecord;
import org.neo4j.kernel.impl.store.record.PrimitiveRecord;
import org.neo4j.kernel.impl.store.record.PropertyBlock;
import org.neo4j.kernel.impl.store.record.PropertyRecord;
import org.neo4j.kernel.impl.store.record.RelationshipGroupRecord;
import org.neo4j.kernel.impl.store.record.RelationshipRecord;
import org.neo4j.kernel.impl.store.record.SchemaRecord;
import org.neo4j.kernel.impl.transaction.log.TransactionAppender;
import org.neo4j.kernel.impl.transaction.log.TransactionCommitmentFactory;
import org.neo4j.kernel.impl.transaction.tracing.TransactionWriteEvent;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.storageengine.api.CommandBatch;
import org.neo4j.storageengine.api.EntityUpdates;
import org.neo4j.storageengine.api.StorageEngine;
import org.neo4j.storageengine.api.StorageNodeCursor;
import org.neo4j.storageengine.api.StoragePropertyCursor;
import org.neo4j.storageengine.api.StorageReader;
import org.neo4j.storageengine.api.StorageRelationshipScanCursor;
import org.neo4j.storageengine.api.TransactionApplicationMode;
import org.neo4j.storageengine.api.TransactionIdStore;
import org.neo4j.storageengine.api.cursor.StoreCursors;
import org.neo4j.test.TestDatabaseManagementServiceBuilder;
import org.neo4j.test.utils.TestDirectory;
import org.neo4j.token.CreatingTokenHolder;
import org.neo4j.token.TokenCreator;
import org.neo4j.token.TokenHolders;
import org.neo4j.token.api.NamedToken;
import org.neo4j.token.api.TokenHolder;
import org.neo4j.values.storable.Value;

public abstract class GraphStoreFixture implements AutoCloseable {
    private DirectStoreAccess directStoreAccess;
    private final long[] highIds = new long[StoreType.STORE_TYPES.length];

    private final TestDirectory testDirectory;

    private DatabaseManagementService managementService;
    private GraphDatabaseAPI database;
    private InternalTransactionCommitProcess commitProcess;
    private TransactionIdStore transactionIdStore;
    private NeoStores neoStores;
    private IndexingService indexingService;
    private RecordStorageEngine storageEngine;
    private StoreCursors storeCursors;
    private TransactionCommitmentFactory commitmentFactory;
    private DirectRecordAccess<PropertyRecord, PrimitiveRecord> recordAccess;

    protected GraphStoreFixture(TestDirectory testDirectory) {
        this.testDirectory = testDirectory;
        startDatabaseAndExtractComponents();
        generateInitialData();
    }

    private void startDatabaseAndExtractComponents() {
        managementService = createBuilder(testDirectory.homePath())
                .setFileSystem(testDirectory.getFileSystem())
                // Some tests using this fixture were written when the label_block_size was 60 and so hardcoded
                // tests and records around that. Those tests could change, but the simpler option is to just
                // keep the block size to 60 and let them be.
                .setConfig(GraphDatabaseInternalSettings.label_block_size, 60)
                .setConfig(GraphDatabaseInternalSettings.consistency_check_on_apply, false)
                .setConfig(GraphDatabaseSettings.db_format, PageAligned.LATEST_NAME) // NOTE: before getConfig()
                .setConfig(getConfig())
                .build();
        database = (GraphDatabaseAPI) managementService.database(DEFAULT_DATABASE_NAME);
        DependencyResolver dependencyResolver = database.getDependencyResolver();

        commitProcess = new InternalTransactionCommitProcess(
                dependencyResolver.resolveDependency(TransactionAppender.class),
                dependencyResolver.resolveDependency(StorageEngine.class),
                false,
                CommandCommitListeners.NO_LISTENERS);
        transactionIdStore = database.getDependencyResolver().resolveDependency(TransactionIdStore.class);
        commitmentFactory = database.getDependencyResolver().resolveDependency(TransactionCommitmentFactory.class);

        storageEngine = dependencyResolver.resolveDependency(RecordStorageEngine.class);
        neoStores = storageEngine.testAccessNeoStores();
        indexingService = dependencyResolver.resolveDependency(IndexingService.class);
        directStoreAccess = new DirectStoreAccess(
                neoStores,
                dependencyResolver.resolveDependency(IndexProviderMap.class),
                dependencyResolver.resolveDependency(TokenHolders.class),
                dependencyResolver.resolveDependency(IdGeneratorFactory.class));
        storeCursors = storageEngine.createStorageCursors(NULL_CONTEXT);
        PropertyStore propertyStore = neoStores.getPropertyStore();
        this.recordAccess = new DirectRecordAccess<>(
                propertyStore,
                Loaders.propertyLoader(propertyStore, storeCursors),
                NULL_CONTEXT,
                PROPERTY_CURSOR,
                storeCursors);
    }

    protected TestDatabaseManagementServiceBuilder createBuilder(Path homePath) {
        return new TestDatabaseManagementServiceBuilder(homePath);
    }

    @Override
    public void close() {
        managementService.shutdown();
    }

    public void apply(Transaction transaction) throws KernelException {
        CommandBatch representation = transaction.representation(
                idGenerator(),
                transactionIdStore.getLastCommittedTransactionId(),
                neoStores,
                indexingService,
                recordAccess);
        var transactionIdGenerator = new IdStoreTransactionIdGenerator(transactionIdStore);
        try (var storeCursors = storageEngine.createStorageCursors(NULL_CONTEXT)) {
            commitProcess.commit(
                    new TransactionToApply(
                            representation,
                            NULL_CONTEXT,
                            storeCursors,
                            commitmentFactory.newCommitment(),
                            transactionIdGenerator),
                    TransactionWriteEvent.NULL,
                    TransactionApplicationMode.EXTERNAL);
        }
    }

    public void apply(Consumer<org.neo4j.graphdb.Transaction> tx) {
        try (org.neo4j.graphdb.Transaction transaction = database.beginTx()) {
            tx.accept(transaction);
            transaction.commit();
        }
    }

    public DirectStoreAccess directStoreAccess() {
        return directStoreAccess;
    }

    public GraphDatabaseAPI database() {
        return database;
    }

    /**
     * Accessors from this IndexAccessorLookup are taken from the running database and should not be closed.
     */
    public IndexAccessors.IndexAccessorLookup indexAccessorLookup() {
        return new LookupAccessorsFromRunningDb(indexingService);
    }

    public DatabaseLayout databaseLayout() {
        return Neo4jLayout.of(testDirectory.homePath()).databaseLayout(DEFAULT_DATABASE_NAME);
    }

    public IndexingService indexingService() {
        return indexingService;
    }

    public StoreCursors getStoreCursors() {
        return storeCursors;
    }

    public EntityUpdates nodeAsUpdates(long nodeId) {
        try (StorageReader storeReader = storageEngine.newReader();
                var storeCursors = storageEngine.createStorageCursors(NULL_CONTEXT);
                StorageNodeCursor nodeCursor = storeReader.allocateNodeCursor(NULL_CONTEXT, storeCursors);
                StoragePropertyCursor propertyCursor =
                        storeReader.allocatePropertyCursor(NULL_CONTEXT, storeCursors, INSTANCE)) {
            nodeCursor.single(nodeId);
            int[] labels;
            if (!nodeCursor.next() || !nodeCursor.hasProperties() || (labels = nodeCursor.labels()).length == 0) {
                return null;
            }
            nodeCursor.properties(propertyCursor, ALL_PROPERTIES);
            EntityUpdates.Builder update = EntityUpdates.forEntity(nodeId, true).withTokens(labels);
            while (propertyCursor.next()) {
                update.added(propertyCursor.propertyKey(), propertyCursor.propertyValue());
            }
            return update.build();
        }
    }

    public EntityUpdates relationshipAsUpdates(long relId) {
        try (StorageReader storeReader = storageEngine.newReader();
                var storeCursors = storageEngine.createStorageCursors(NULL_CONTEXT);
                StorageRelationshipScanCursor relCursor =
                        storeReader.allocateRelationshipScanCursor(NULL_CONTEXT, storeCursors);
                StoragePropertyCursor propertyCursor =
                        storeReader.allocatePropertyCursor(NULL_CONTEXT, storeCursors, INSTANCE)) {
            relCursor.single(relId);
            if (!relCursor.next() || !relCursor.hasProperties()) {
                return null;
            }
            int type = relCursor.type();
            relCursor.properties(propertyCursor, ALL_PROPERTIES);
            EntityUpdates.Builder update = EntityUpdates.forEntity(relId, true).withTokens(type);
            while (propertyCursor.next()) {
                update.added(propertyCursor.propertyKey(), propertyCursor.propertyValue());
            }
            return update.build();
        }
    }

    public List<IndexDescriptor> getIndexDescriptors() {
        return Iterables.stream(indexingService.getIndexProxies())
                .map(IndexProxy::getDescriptor)
                .toList();
    }

    public IndexDescriptor getIndexDescriptorByName(String indexName) {
        try (RecordStorageReader recordStorageReader = storageEngine.newReader()) {
            return recordStorageReader.indexGetForName(indexName);
        }
    }

    public NeoStores neoStores() {
        return neoStores;
    }

    @FunctionalInterface
    interface TokenChange {
        int createToken(String name, boolean internal, TransactionDataBuilder tx, IdGenerator next);
    }

    public TokenHolders writableTokenHolders() {
        TokenHolder propertyKeyTokens = new CreatingTokenHolder(
                buildTokenCreator((name, internal, tx, next) -> {
                    int id = next.propertyKey();
                    tx.propertyKey(id, name, internal);
                    return id;
                }),
                TokenHolder.TYPE_PROPERTY_KEY);
        TokenHolder labelTokens = new CreatingTokenHolder(
                buildTokenCreator((name, internal, tx, next) -> {
                    int id = next.label();
                    tx.nodeLabel(id, name, internal);
                    return id;
                }),
                TokenHolder.TYPE_LABEL);
        TokenHolder relationshipTypeTokens = new CreatingTokenHolder(
                buildTokenCreator((name, internal, tx, next) -> {
                    int id = next.relationshipType();
                    tx.relationshipType(id, name, internal);
                    return id;
                }),
                TokenHolder.TYPE_RELATIONSHIP_TYPE);
        TokenHolders tokenHolders = new TokenHolders(propertyKeyTokens, labelTokens, relationshipTypeTokens);
        try (var storeCursors = new CachedStoreCursors(neoStores, NULL_CONTEXT)) {
            tokenHolders.setInitialTokens(allReadableTokens(directStoreAccess().nativeStores()), storeCursors);
        }
        return tokenHolders;
    }

    private TokenCreator buildTokenCreator(TokenChange propChange) {
        return (name, internal) -> {
            MutableInt keyId = new MutableInt();
            apply(new Transaction() {
                @Override
                protected void transactionData(TransactionDataBuilder tx, IdGenerator next) {
                    keyId.setValue(propChange.createToken(name, internal, tx, next));
                }
            });
            return keyId.intValue();
        };
    }

    public abstract static class Transaction {
        final long startTimestamp = currentTimeMillis();

        protected abstract void transactionData(TransactionDataBuilder tx, IdGenerator next) throws KernelException;

        public CommandBatch representation(
                IdGenerator idGenerator,
                long lastCommittedTx,
                NeoStores neoStores,
                IndexingService indexingService,
                DirectRecordAccess<PropertyRecord, PrimitiveRecord> recordAccess)
                throws KernelException {
            TransactionWriter writer = new TransactionWriter(neoStores);
            transactionData(
                    new TransactionDataBuilder(writer, neoStores, idGenerator, indexingService, recordAccess),
                    idGenerator);
            idGenerator.updateCorrespondingIdGenerators(neoStores);
            return writer.representation(UNKNOWN_CONSENSUS_INDEX, startTimestamp, lastCommittedTx, currentTimeMillis());
        }
    }

    public IdGenerator idGenerator() {
        return new IdGenerator();
    }

    public class IdGenerator {
        private long nextId(StoreType type) {
            return highIds[type.ordinal()]++;
        }

        public long schema() {
            return nextId(StoreType.SCHEMA);
        }

        public long node() {
            return nextId(StoreType.NODE);
        }

        public int label() {
            return (int) nextId(StoreType.LABEL_TOKEN);
        }

        public long nodeLabel() {
            return nextId(StoreType.NODE_LABEL);
        }

        public long relationship() {
            return nextId(StoreType.RELATIONSHIP);
        }

        public long relationshipGroup() {
            return nextId(StoreType.RELATIONSHIP_GROUP);
        }

        public long property() {
            return nextId(StoreType.PROPERTY);
        }

        public long stringProperty() {
            return nextId(StoreType.PROPERTY_STRING);
        }

        public long arrayProperty() {
            return nextId(StoreType.PROPERTY_ARRAY);
        }

        public int relationshipType() {
            return (int) nextId(StoreType.RELATIONSHIP_TYPE_TOKEN);
        }

        public int propertyKey() {
            return (int) nextId(StoreType.PROPERTY_KEY_TOKEN);
        }

        void updateCorrespondingIdGenerators(NeoStores neoStores) {
            neoStores.getNodeStore().setHighId(highIds[StoreType.NODE.ordinal()]);
            neoStores.getRelationshipStore().setHighId(highIds[StoreType.RELATIONSHIP.ordinal()]);
            neoStores.getRelationshipGroupStore().setHighId(highIds[StoreType.RELATIONSHIP_GROUP.ordinal()]);
        }
    }

    public static final class TransactionDataBuilder {
        private final TransactionWriter writer;
        private final NodeStore nodes;
        private final IndexingService indexingService;
        private final TokenHolders tokenHolders;
        private final AtomicInteger propKeyDynIds;
        private final AtomicInteger labelDynIds;
        private final AtomicInteger relTypeDynIds;
        private final NeoStores neoStores;
        private final DirectRecordAccess<PropertyRecord, PrimitiveRecord> recordAccess;
        private final DynamicAllocatorProvider allocatorProvider;

        TransactionDataBuilder(
                TransactionWriter writer,
                NeoStores neoStores,
                IdGenerator next,
                IndexingService indexingService,
                DirectRecordAccess<PropertyRecord, PrimitiveRecord> recordAccess) {
            this.neoStores = neoStores;
            this.allocatorProvider = DynamicAllocatorProviders.nonTransactionalAllocator(neoStores);
            this.recordAccess = recordAccess;
            var propertyKeyNameStore = neoStores.getPropertyKeyTokenStore().getNameStore();
            this.propKeyDynIds = new AtomicInteger(
                    (int) propertyKeyNameStore.getIdGenerator().getHighId());
            var labelTokenNameStore = neoStores.getLabelTokenStore().getNameStore();
            this.labelDynIds =
                    new AtomicInteger((int) labelTokenNameStore.getIdGenerator().getHighId());
            var relTypeTokenNameStore =
                    neoStores.getRelationshipTypeTokenStore().getNameStore();
            this.relTypeDynIds = new AtomicInteger(
                    (int) relTypeTokenNameStore.getIdGenerator().getHighId());
            this.writer = writer;
            this.nodes = neoStores.getNodeStore();
            this.indexingService = indexingService;

            TokenHolder propTokens = new CreatingTokenHolder(
                    (name, internal) -> {
                        int id = next.propertyKey();
                        writer.propertyKey(id, name, internal, dynIds(0, propKeyDynIds, name));
                        return id;
                    },
                    TokenHolder.TYPE_PROPERTY_KEY);

            TokenHolder labelTokens = new CreatingTokenHolder(
                    (name, internal) -> {
                        int id = next.label();
                        writer.label(id, name, internal, dynIds(0, labelDynIds, name));
                        return id;
                    },
                    TokenHolder.TYPE_LABEL);

            TokenHolder relTypeTokens = new CreatingTokenHolder(
                    (name, internal) -> {
                        int id = next.relationshipType();
                        writer.relationshipType(id, name, internal, dynIds(0, relTypeDynIds, name));
                        return id;
                    },
                    TokenHolder.TYPE_RELATIONSHIP_TYPE);

            this.tokenHolders = new TokenHolders(propTokens, labelTokens, relTypeTokens);
            try (var storeCursors = new CachedStoreCursors(neoStores, NULL_CONTEXT)) {
                tokenHolders.setInitialTokens(allReadableTokens(neoStores), storeCursors);
                tokenHolders
                        .propertyKeyTokens()
                        .getAllTokens()
                        .forEach(token -> propKeyDynIds.getAndUpdate(id -> Math.max(id, token.id() + 1)));
                tokenHolders
                        .labelTokens()
                        .getAllTokens()
                        .forEach(token -> labelDynIds.getAndUpdate(id -> Math.max(id, token.id() + 1)));
                tokenHolders
                        .relationshipTypeTokens()
                        .getAllTokens()
                        .forEach(token -> relTypeDynIds.getAndUpdate(id -> Math.max(id, token.id() + 1)));
            }
        }

        private static int[] dynIds(int externalBase, AtomicInteger idGenerator, String name) {
            if (idGenerator.get() <= externalBase) {
                idGenerator.set(externalBase + 1);
            }
            byte[] bytes = name.getBytes(StandardCharsets.UTF_8);
            int blocks = 1 + (bytes.length / TokenStore.NAME_STORE_BLOCK_SIZE);
            int base = idGenerator.getAndAdd(blocks);
            int[] ids = new int[blocks];
            for (int i = 0; i < blocks; i++) {
                ids[i] = base + i;
            }
            return ids;
        }

        public TokenHolders tokenHolders() {
            return tokenHolders;
        }

        public void createSchema(SchemaRecord before, SchemaRecord after, SchemaRule rule) {
            writer.createSchema(before, after, rule);
        }

        public int[] propertyKey(int id, String key, boolean internal) {
            int[] dynamicIds = dynIds(id, propKeyDynIds, key);
            writer.propertyKey(id, key, internal, dynamicIds);
            tokenHolders.propertyKeyTokens().addToken(new NamedToken(key, id));
            return dynamicIds;
        }

        public int[] nodeLabel(int id, String name, boolean internal) {
            int[] dynamicIds = dynIds(id, labelDynIds, name);
            writer.label(id, name, internal, dynamicIds);
            tokenHolders.labelTokens().addToken(new NamedToken(name, id));
            return dynamicIds;
        }

        public int[] relationshipType(int id, String relationshipType, boolean internal) {
            int[] dynamicIds = dynIds(id, relTypeDynIds, relationshipType);
            writer.relationshipType(id, relationshipType, internal, dynamicIds);
            tokenHolders.relationshipTypeTokens().addToken(new NamedToken(relationshipType, id));
            return dynamicIds;
        }

        public void create(NodeRecord node) {
            updateCounts(node, 1);
            writer.create(node);
        }

        public void createNoCountUpdate(NodeRecord node) {
            writer.create(node);
        }

        public void update(NodeRecord before, NodeRecord after) {
            updateCounts(before, -1);
            updateCounts(after, 1);
            writer.update(before, after);
        }

        public void delete(NodeRecord node) {
            updateCounts(node, -1);
            writer.delete(node);
        }

        public NodeRecord newNode(long nodeId, boolean inUse, int... labels) {
            NodeRecord nodeRecord = new NodeRecord(nodeId);
            nodeRecord = nodeRecord.initialize(
                    inUse,
                    NO_NEXT_PROPERTY.longValue(),
                    false,
                    NO_NEXT_RELATIONSHIP.longValue(),
                    NO_LABELS_FIELD.longValue());
            if (inUse) {
                InlineNodeLabels labelFieldWriter = new InlineNodeLabels(nodeRecord);
                labelFieldWriter.put(labels, null, null, NULL_CONTEXT, StoreCursors.NULL, INSTANCE);
            }
            return nodeRecord;
        }

        public RelationshipRecord newRelationship(long relId, boolean inUse, int type) {
            if (!inUse) {
                type = -1;
            }
            return new RelationshipRecord(relId)
                    .initialize(
                            inUse,
                            NO_NEXT_PROPERTY.longValue(),
                            0,
                            0,
                            type,
                            NO_NEXT_RELATIONSHIP.longValue(),
                            NO_NEXT_RELATIONSHIP.longValue(),
                            NO_NEXT_RELATIONSHIP.longValue(),
                            NO_NEXT_RELATIONSHIP.longValue(),
                            true,
                            false);
        }

        public PropertyRecord createProperty(long propId, PrimitiveRecord entityRecord, Value value, int propertyKey) {
            var propertyRecord =
                    recordAccess.create(propId, entityRecord, NULL_CONTEXT).forChangingData();
            propertyRecord.setInUse(true);
            propertyRecord.setCreated();

            PropertyBlock propertyBlock = new PropertyBlock();
            PropertyStore.encodeValue(
                    propertyBlock,
                    propertyKey,
                    value,
                    allocatorProvider.allocator(StoreType.PROPERTY_STRING),
                    allocatorProvider.allocator(StoreType.PROPERTY_ARRAY),
                    NULL_CONTEXT,
                    INSTANCE);
            propertyRecord.addPropertyBlock(propertyBlock);

            return propertyRecord;
        }

        public void create(RelationshipRecord relationship) {
            writer.create(relationship);
        }

        public void delete(RelationshipRecord relationship) {
            writer.delete(relationship);
        }

        public void create(RelationshipGroupRecord group) {
            writer.create(group);
        }

        public void delete(RelationshipGroupRecord group) {
            writer.delete(group);
        }

        public void create(PropertyRecord property) {
            writer.create(property);
        }

        public void update(PropertyRecord before, PropertyRecord property) {
            writer.update(before, property);
        }

        public void delete(PropertyRecord before, PropertyRecord property) {
            writer.delete(before, property);
        }

        private void updateCounts(NodeRecord node, int delta) {
            writer.incrementNodeCount(ANY_LABEL, delta);
            for (int label : NodeLabelsField.parseLabelsField(node).get(nodes, StoreCursors.NULL)) {
                writer.incrementNodeCount(label, delta);
            }
        }

        public void incrementNodeCount(int labelId, long delta) {
            writer.incrementNodeCount(labelId, delta);
        }

        public void incrementRelationshipCount(int startLabelId, int typeId, int endLabelId, long delta) {
            writer.incrementRelationshipCount(startLabelId, typeId, endLabelId, delta);
        }

        public IndexDescriptor completeConfiguration(IndexDescriptor indexDescriptor) {
            return indexingService.completeConfiguration(indexDescriptor);
        }
    }

    protected abstract void generateInitialData(GraphDatabaseService graphDb);

    private void generateInitialData() {
        generateInitialData(database);
        keepHighId(StoreType.SCHEMA, neoStores.getSchemaStore());
        keepHighId(StoreType.NODE, neoStores.getNodeStore());
        keepHighId(StoreType.LABEL_TOKEN, neoStores.getLabelTokenStore());
        keepHighId(StoreType.NODE_LABEL, neoStores.getNodeStore().getDynamicLabelStore());
        keepHighId(StoreType.RELATIONSHIP, neoStores.getRelationshipStore());
        keepHighId(StoreType.RELATIONSHIP_GROUP, neoStores.getRelationshipGroupStore());
        keepHighId(StoreType.PROPERTY, neoStores.getPropertyStore());
        keepHighId(StoreType.PROPERTY_STRING, neoStores.getPropertyStore().getStringStore());
        keepHighId(StoreType.PROPERTY_ARRAY, neoStores.getPropertyStore().getArrayStore());
        keepHighId(StoreType.RELATIONSHIP_TYPE_TOKEN, neoStores.getRelationshipTypeTokenStore());
        keepHighId(StoreType.PROPERTY_KEY_TOKEN, neoStores.getPropertyKeyTokenStore());
    }

    private void keepHighId(StoreType storeType, RecordStore<? extends AbstractBaseRecord> store) {
        highIds[storeType.ordinal()] = store.getIdGenerator().getHighId();
    }

    protected abstract Map<Setting<?>, Object> getConfig();
}
