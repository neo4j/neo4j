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

import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.neo4j.index.internal.gbptree.RecoveryCleanupWorkCollector.immediate;
import static org.neo4j.io.IOUtils.closeAllUnchecked;
import static org.neo4j.io.pagecache.context.CursorContext.NULL_CONTEXT;
import static org.neo4j.io.pagecache.context.FixedVersionContextSupplier.EMPTY_CONTEXT_SUPPLIER;
import static org.neo4j.memory.EmptyMemoryTracker.INSTANCE;

import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.RepeatedTest;
import org.junit.jupiter.api.TestInstance;
import org.neo4j.configuration.Config;
import org.neo4j.internal.id.DefaultIdGeneratorFactory;
import org.neo4j.internal.schema.SchemaRule;
import org.neo4j.io.fs.EphemeralFileSystemAbstraction;
import org.neo4j.io.layout.DatabaseLayout;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.io.pagecache.context.CursorContextFactory;
import org.neo4j.io.pagecache.tracing.PageCacheTracer;
import org.neo4j.kernel.impl.store.DynamicAllocatorProvider;
import org.neo4j.kernel.impl.store.DynamicAllocatorProviders;
import org.neo4j.kernel.impl.store.NeoStores;
import org.neo4j.kernel.impl.store.StoreFactory;
import org.neo4j.kernel.impl.store.StoreType;
import org.neo4j.kernel.impl.store.cursor.CachedStoreCursors;
import org.neo4j.kernel.impl.transaction.log.LogTailLogVersionsMetadata;
import org.neo4j.logging.NullLogProvider;
import org.neo4j.storageengine.util.IdUpdateListener;
import org.neo4j.test.extension.EphemeralNeo4jLayoutExtension;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.pagecache.EphemeralPageCacheExtension;
import org.neo4j.test.utils.TestDirectory;
import org.neo4j.token.RegisteringCreatingTokenHolder;
import org.neo4j.token.TokenCreator;
import org.neo4j.token.TokenHolders;
import org.neo4j.token.api.TokenHolder;

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
@EphemeralPageCacheExtension
@EphemeralNeo4jLayoutExtension
class SchemaStorageReadAndWriteTest {
    @Inject
    private PageCache pageCache;

    @Inject
    private TestDirectory testDirectory;

    @Inject
    private EphemeralFileSystemAbstraction fs;

    @Inject
    private DatabaseLayout databaseLayout;

    private final RandomSchema randomSchema = new RandomSchema() {
        @Override
        public int nextRuleId() {
            return (int) storage.newRuleId(NULL_CONTEXT);
        }
    };

    private SchemaStorage storage;
    private NeoStores neoStores;
    private CachedStoreCursors storeCursors;
    private DynamicAllocatorProvider allocatorProvider;

    @BeforeAll
    void before() throws Exception {
        testDirectory.prepareDirectory(getClass(), "test");
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

        AtomicInteger tokenIdCounter = new AtomicInteger();
        TokenCreator tokenCreator = (name, internal) -> tokenIdCounter.incrementAndGet();
        TokenHolders tokens = new TokenHolders(
                new RegisteringCreatingTokenHolder(tokenCreator, TokenHolder.TYPE_PROPERTY_KEY),
                new RegisteringCreatingTokenHolder(tokenCreator, TokenHolder.TYPE_LABEL),
                new RegisteringCreatingTokenHolder(tokenCreator, TokenHolder.TYPE_RELATIONSHIP_TYPE));
        storeCursors = new CachedStoreCursors(neoStores, NULL_CONTEXT);
        tokens.setInitialTokens(StoreTokens.allTokens(neoStores), storeCursors);
        tokenIdCounter.set(
                Math.max(tokenIdCounter.get(), tokens.propertyKeyTokens().size()));
        tokenIdCounter.set(Math.max(tokenIdCounter.get(), tokens.labelTokens().size()));
        tokenIdCounter.set(
                Math.max(tokenIdCounter.get(), tokens.relationshipTypeTokens().size()));
        storage = new SchemaStorage(neoStores.getSchemaStore(), tokens);
    }

    @AfterAll
    void after() {
        closeAllUnchecked(storeCursors, neoStores);
    }

    @RepeatedTest(2000)
    void shouldPerfectlyPreserveSchemaRules() throws Exception {
        SchemaRule schemaRule = randomSchema.nextSchemaRule();
        storage.writeSchemaRule(
                schemaRule, IdUpdateListener.DIRECT, allocatorProvider, NULL_CONTEXT, INSTANCE, storeCursors);
        SchemaRule returnedRule = storage.loadSingleSchemaRule(schemaRule.getId(), storeCursors);
        assertTrue(
                RandomSchema.schemaDeepEquals(returnedRule, schemaRule),
                () -> "\n" + returnedRule + "\nwas not equal to\n" + schemaRule);
    }
}
