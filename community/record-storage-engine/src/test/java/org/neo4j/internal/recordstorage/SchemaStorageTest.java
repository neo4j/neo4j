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
package org.neo4j.internal.recordstorage;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.neo4j.index.internal.gbptree.RecoveryCleanupWorkCollector.immediate;
import static org.neo4j.io.pagecache.context.CursorContext.NULL_CONTEXT;
import static org.neo4j.io.pagecache.context.FixedVersionContextSupplier.EMPTY_CONTEXT_SUPPLIER;
import static org.neo4j.test.mockito.matcher.KernelExceptionUserMessageAssert.assertThat;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.stream.Stream;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;
import org.neo4j.common.EntityType;
import org.neo4j.common.TokenNameLookup;
import org.neo4j.configuration.Config;
import org.neo4j.exceptions.KernelException;
import org.neo4j.internal.id.DefaultIdGeneratorFactory;
import org.neo4j.internal.id.IdGenerator;
import org.neo4j.internal.id.IdType;
import org.neo4j.internal.id.SchemaIdType;
import org.neo4j.internal.kernel.api.exceptions.schema.DuplicateSchemaRuleException;
import org.neo4j.internal.kernel.api.exceptions.schema.SchemaRuleNotFoundException;
import org.neo4j.internal.schema.ConstraintDescriptor;
import org.neo4j.internal.schema.SchemaRule;
import org.neo4j.internal.schema.constraints.ConstraintDescriptorFactory;
import org.neo4j.io.fs.EphemeralFileSystemAbstraction;
import org.neo4j.io.layout.recordstorage.RecordDatabaseLayout;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.io.pagecache.PageCursor;
import org.neo4j.io.pagecache.context.CursorContext;
import org.neo4j.io.pagecache.context.CursorContextFactory;
import org.neo4j.io.pagecache.tracing.PageCacheTracer;
import org.neo4j.kernel.impl.store.DynamicAllocatorProvider;
import org.neo4j.kernel.impl.store.DynamicAllocatorProviders;
import org.neo4j.kernel.impl.store.NeoStores;
import org.neo4j.kernel.impl.store.RecordStore;
import org.neo4j.kernel.impl.store.StoreFactory;
import org.neo4j.kernel.impl.store.StoreType;
import org.neo4j.kernel.impl.store.cursor.CachedStoreCursors;
import org.neo4j.kernel.impl.store.record.AbstractBaseRecord;
import org.neo4j.kernel.impl.store.record.RecordLoad;
import org.neo4j.kernel.impl.transaction.log.LogTailLogVersionsMetadata;
import org.neo4j.logging.NullLogProvider;
import org.neo4j.memory.EmptyMemoryTracker;
import org.neo4j.storageengine.api.cursor.StoreCursors;
import org.neo4j.storageengine.util.IdUpdateListener;
import org.neo4j.test.extension.EphemeralNeo4jLayoutExtension;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.pagecache.EphemeralPageCacheExtension;
import org.neo4j.token.RegisteringCreatingTokenHolder;
import org.neo4j.token.TokenHolders;
import org.neo4j.token.api.TokenHolder;

@EphemeralPageCacheExtension
@EphemeralNeo4jLayoutExtension
class SchemaStorageTest {
    private static final String LABEL1 = "Label1";
    private static final int LABEL1_ID = 1;
    private static final String TYPE1 = "Type1";
    private static final int TYPE1_ID = 1;
    private static final String PROP1 = "prop1";
    private static final int PROP1_ID = 1;

    @Inject
    private PageCache pageCache;

    @Inject
    private RecordDatabaseLayout databaseLayout;

    @Inject
    private EphemeralFileSystemAbstraction fs;

    private SchemaStorage storage;
    private NeoStores neoStores;
    private StoreCursors storeCursors;
    private final RandomSchema randomSchema = new RandomSchema() {
        @Override
        public int nextRuleId() {
            return (int) storage.newRuleId(NULL_CONTEXT);
        }
    };
    private DynamicAllocatorProvider allocatorProvider;

    @BeforeEach
    void before() {
        var pageCacheTracer = PageCacheTracer.NULL;
        var storeFactory = new StoreFactory(
                databaseLayout,
                Config.defaults(),
                new DefaultIdGeneratorFactory(fs, immediate(), pageCacheTracer, databaseLayout.getDatabaseName()),
                pageCache,
                pageCacheTracer,
                fs,
                NullLogProvider.getInstance(),
                new CursorContextFactory(pageCacheTracer, EMPTY_CONTEXT_SUPPLIER),
                false,
                LogTailLogVersionsMetadata.EMPTY_LOG_TAIL);
        neoStores = storeFactory.openNeoStores(
                StoreType.SCHEMA,
                StoreType.PROPERTY_KEY_TOKEN,
                StoreType.LABEL_TOKEN,
                StoreType.RELATIONSHIP_TYPE_TOKEN);
        allocatorProvider = DynamicAllocatorProviders.nonTransactionalAllocator(neoStores);

        var tokenHolders = new TokenHolders(
                new RegisteringCreatingTokenHolder(new SimpleTokenCreator(), TokenHolder.TYPE_PROPERTY_KEY),
                new RegisteringCreatingTokenHolder(new SimpleTokenCreator(), TokenHolder.TYPE_LABEL),
                new RegisteringCreatingTokenHolder(new SimpleTokenCreator(), TokenHolder.TYPE_RELATIONSHIP_TYPE));
        storage = new SchemaStorage(neoStores.getSchemaStore(), tokenHolders);
        storeCursors = new CachedStoreCursors(neoStores, NULL_CONTEXT);
    }

    @AfterEach
    void after() {
        neoStores.close();
    }

    @Test
    void shouldThrowExceptionOnNodeRuleNotFound() {
        TokenNameLookup tokenNameLookup = getDefaultTokenNameLookup();

        var e = assertThrows(
                SchemaRuleNotFoundException.class,
                () -> storage.constraintsGetSingle(
                        ConstraintDescriptorFactory.existsForLabel(LABEL1_ID, PROP1_ID), StoreCursors.NULL));

        assertThat(e, tokenNameLookup)
                .hasUserMessage("No label property existence constraint was found for (:Label1 {prop1}).");
    }

    @Test
    void shouldThrowExceptionOnNodeDuplicateRuleFound() {
        TokenNameLookup tokenNameLookup = getDefaultTokenNameLookup();

        SchemaStorage schemaStorageSpy = Mockito.spy(storage);
        when(schemaStorageSpy.streamAllSchemaRules(false, StoreCursors.NULL))
                .thenReturn(Stream.of(
                        getUniquePropertyConstraintRule(1L, LABEL1_ID, PROP1_ID),
                        getUniquePropertyConstraintRule(2L, LABEL1_ID, PROP1_ID)));

        var e = assertThrows(
                DuplicateSchemaRuleException.class,
                () -> schemaStorageSpy.constraintsGetSingle(
                        ConstraintDescriptorFactory.uniqueForLabel(LABEL1_ID, PROP1_ID), StoreCursors.NULL));

        assertThat(e, tokenNameLookup)
                .hasUserMessage("Multiple label uniqueness constraints found for (:Label1 {prop1}).");
    }

    @Test
    void shouldThrowExceptionOnRelationshipRuleNotFound() {
        TokenNameLookup tokenNameLookup = getDefaultTokenNameLookup();

        var e = assertThrows(
                SchemaRuleNotFoundException.class,
                () -> storage.constraintsGetSingle(
                        ConstraintDescriptorFactory.existsForRelType(TYPE1_ID, PROP1_ID), StoreCursors.NULL));
        assertThat(e, tokenNameLookup)
                .hasUserMessage(
                        "No relationship type property existence constraint was found for ()-[:Type1 {prop1}]-().");
    }

    @Test
    void shouldThrowExceptionOnRelationshipDuplicateRuleFound() {
        TokenNameLookup tokenNameLookup = getDefaultTokenNameLookup();

        SchemaStorage schemaStorageSpy = Mockito.spy(storage);
        when(schemaStorageSpy.streamAllSchemaRules(false, StoreCursors.NULL))
                .thenReturn(Stream.of(
                        getRelationshipPropertyExistenceConstraintRule(1L, TYPE1_ID, PROP1_ID),
                        getRelationshipPropertyExistenceConstraintRule(2L, TYPE1_ID, PROP1_ID)));

        var e = assertThrows(
                DuplicateSchemaRuleException.class,
                () -> schemaStorageSpy.constraintsGetSingle(
                        ConstraintDescriptorFactory.existsForRelType(TYPE1_ID, PROP1_ID), StoreCursors.NULL));

        assertThat(e, tokenNameLookup)
                .hasUserMessage(
                        "Multiple relationship type property existence constraints found for ()-[:Type1 {prop1}]-().");
    }

    @Test
    void shouldMarkAllRecordIdsAsUnusedOnDeletion() throws KernelException {
        // Given
        var tracker = new TrackingIdUpdaterListener();

        // When
        SchemaRule schemaRule = randomSchema.nextSchemaRule();
        storage.writeSchemaRule(
                schemaRule, tracker, allocatorProvider, NULL_CONTEXT, EmptyMemoryTracker.INSTANCE, storeCursors);

        // Then
        assertThat(tracker.usedIdsPerType).isNotEmpty();
        assertThat(tracker.usedIdsPerType).containsKeys(expectedUsedIdTypes(schemaRule));
        assertThat(tracker.unusedIdsPerType).isEmpty();

        // When
        storage.deleteSchemaRule(schemaRule.getId(), tracker, NULL_CONTEXT, EmptyMemoryTracker.INSTANCE, storeCursors);

        // Then
        assertThat(tracker.unusedIdsPerType).isNotEmpty();
        assertThat(tracker.unusedIdsPerType).isEqualTo(tracker.usedIdsPerType);
    }

    @Test
    void shouldMarkAllRecordsAsNotInUseOnDeletion() throws KernelException {
        // Given
        SchemaRule schemaRule = randomSchema.nextSchemaRule();
        var tracker = new TrackingIdUpdaterListener();
        storage.writeSchemaRule(
                schemaRule, tracker, allocatorProvider, NULL_CONTEXT, EmptyMemoryTracker.INSTANCE, storeCursors);

        // Then
        verifyExpectedInUse(tracker.usedIdsPerType, true);

        // When
        storage.deleteSchemaRule(
                schemaRule.getId(), IdUpdateListener.DIRECT, NULL_CONTEXT, EmptyMemoryTracker.INSTANCE, storeCursors);

        // Then
        verifyExpectedInUse(tracker.usedIdsPerType, false);
    }

    private void verifyExpectedInUse(Map<IdType, Set<Long>> idsByType, boolean expectInUse) {
        for (Map.Entry<IdType, Set<Long>> entry : idsByType.entrySet()) {
            RecordStore<? extends AbstractBaseRecord> store = getStore(entry.getKey());
            verifyInUseStatusForStore(store, entry.getValue(), expectInUse);
        }
    }

    private <T extends AbstractBaseRecord> void verifyInUseStatusForStore(
            RecordStore<T> store, Set<Long> ids, boolean expectInUse) {
        T record = store.newRecord();
        try (PageCursor pageCursor = store.openPageCursorForReading(0, NULL_CONTEXT)) {
            for (Long id : ids) {
                store.getRecordByCursor(id, record, RecordLoad.CHECK, pageCursor);
                assertThat(record.inUse()).isEqualTo(expectInUse);
            }
        }
    }

    private RecordStore<? extends AbstractBaseRecord> getStore(IdType idType) {
        return switch (idType.name()) {
            case "SCHEMA" -> neoStores.getSchemaStore();
            case "PROPERTY" -> neoStores.getPropertyStore();
            case "ARRAY_BLOCK" -> neoStores.getPropertyStore().getArrayStore();
            case "STRING_BLOCK" -> neoStores.getPropertyStore().getStringStore();
            default -> throw new IllegalArgumentException("Did not recognize idType " + idType.name());
        };
    }

    private static IdType[] expectedUsedIdTypes(SchemaRule schemaRule) {
        var expectedIdTypes = new ArrayList<IdType>();
        expectedIdTypes.add(RecordIdType.PROPERTY);
        expectedIdTypes.add(SchemaIdType.SCHEMA);
        if (schemaRule.getName().length() > 36) {
            expectedIdTypes.add(RecordIdType.STRING_BLOCK);
        }
        if (schemaRule.schema().getPropertyIds().length > 8) {
            expectedIdTypes.add(RecordIdType.ARRAY_BLOCK);
        }
        return expectedIdTypes.toArray(IdType[]::new);
    }

    private static TokenNameLookup getDefaultTokenNameLookup() {
        TokenNameLookup tokenNameLookup = mock(TokenNameLookup.class);
        when(tokenNameLookup.labelGetName(LABEL1_ID)).thenReturn(LABEL1);
        when(tokenNameLookup.propertyKeyGetName(PROP1_ID)).thenReturn(PROP1);
        when(tokenNameLookup.relationshipTypeGetName(TYPE1_ID)).thenReturn(TYPE1);
        when(tokenNameLookup.entityTokensGetNames(EntityType.NODE, new int[] {LABEL1_ID}))
                .thenReturn(new String[] {LABEL1});
        when(tokenNameLookup.entityTokensGetNames(EntityType.RELATIONSHIP, new int[] {TYPE1_ID}))
                .thenReturn(new String[] {TYPE1});
        return tokenNameLookup;
    }

    private static ConstraintDescriptor getUniquePropertyConstraintRule(long id, int label, int property) {
        return ConstraintDescriptorFactory.uniqueForLabel(label, property)
                .withId(id)
                .withOwnedIndexId(0);
    }

    private static ConstraintDescriptor getRelationshipPropertyExistenceConstraintRule(
            long id, int type, int property) {
        return ConstraintDescriptorFactory.existsForRelType(type, property).withId(id);
    }

    private static class TrackingIdUpdaterListener implements IdUpdateListener {
        Map<IdType, Set<Long>> usedIdsPerType = new HashMap<>();
        Map<IdType, Set<Long>> unusedIdsPerType = new HashMap<>();

        @Override
        public void markIdAsUsed(IdGenerator idGenerator, long id, int size, CursorContext cursorContext) {
            usedIdsPerType
                    .computeIfAbsent(idGenerator.idType(), k -> new HashSet<>())
                    .add(id);
            IdUpdateListener.DIRECT.markIdAsUsed(idGenerator, id, size, cursorContext);
        }

        @Override
        public void markIdAsUnused(IdGenerator idGenerator, long id, int size, CursorContext cursorContext) {
            unusedIdsPerType
                    .computeIfAbsent(idGenerator.idType(), k -> new HashSet<>())
                    .add(id);
            IdUpdateListener.DIRECT.markIdAsUnused(idGenerator, id, size, cursorContext);
        }

        @Override
        public void close() throws Exception {}
    }
}
