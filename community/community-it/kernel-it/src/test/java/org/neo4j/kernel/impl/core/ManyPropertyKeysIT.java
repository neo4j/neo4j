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
package org.neo4j.kernel.impl.core;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.neo4j.configuration.GraphDatabaseSettings.DEFAULT_DATABASE_NAME;
import static org.neo4j.index.internal.gbptree.RecoveryCleanupWorkCollector.immediate;
import static org.neo4j.internal.recordstorage.RecordCursorTypes.PROPERTY_KEY_TOKEN_CURSOR;
import static org.neo4j.io.pagecache.context.CursorContext.NULL_CONTEXT;
import static org.neo4j.io.pagecache.context.FixedVersionContextSupplier.EMPTY_CONTEXT_SUPPLIER;
import static org.neo4j.kernel.impl.store.StoreType.PROPERTY_KEY_TOKEN_NAME;
import static org.neo4j.memory.EmptyMemoryTracker.INSTANCE;

import java.io.IOException;
import java.util.Collection;
import org.junit.jupiter.api.Test;
import org.neo4j.configuration.Config;
import org.neo4j.configuration.GraphDatabaseSettings;
import org.neo4j.dbms.api.DatabaseManagementService;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Transaction;
import org.neo4j.internal.helpers.collection.Iterables;
import org.neo4j.internal.id.DefaultIdGeneratorFactory;
import org.neo4j.internal.kernel.api.exceptions.TransactionFailureException;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.layout.DatabaseLayout;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.io.pagecache.context.CursorContextFactory;
import org.neo4j.io.pagecache.tracing.DatabaseFlushEvent;
import org.neo4j.io.pagecache.tracing.PageCacheTracer;
import org.neo4j.kernel.api.Kernel;
import org.neo4j.kernel.api.KernelTransaction;
import org.neo4j.kernel.api.security.AnonymousContext;
import org.neo4j.kernel.impl.store.DynamicAllocatorProvider;
import org.neo4j.kernel.impl.store.DynamicAllocatorProviders;
import org.neo4j.kernel.impl.store.NeoStores;
import org.neo4j.kernel.impl.store.PropertyKeyTokenStore;
import org.neo4j.kernel.impl.store.PropertyStore;
import org.neo4j.kernel.impl.store.StoreFactory;
import org.neo4j.kernel.impl.store.cursor.CachedStoreCursors;
import org.neo4j.kernel.impl.store.record.DynamicRecord;
import org.neo4j.kernel.impl.store.record.PropertyKeyTokenRecord;
import org.neo4j.kernel.impl.transaction.log.LogTailLogVersionsMetadata;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.logging.NullLogProvider;
import org.neo4j.test.OtherThreadExecutor;
import org.neo4j.test.TestDatabaseManagementServiceBuilder;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.Neo4jLayoutExtension;
import org.neo4j.test.extension.pagecache.PageCacheExtension;

/**
 * Tests for handling many property keys (even after restart of database)
 * as well as concurrent creation of property keys.
 */
@PageCacheExtension
@Neo4jLayoutExtension
class ManyPropertyKeysIT {
    @Inject
    private FileSystemAbstraction fileSystem;

    @Inject
    private DatabaseLayout databaseLayout;

    @Inject
    private PageCache pageCache;

    private DatabaseManagementService managementService;

    @Test
    void creating_many_property_keys_should_have_all_loaded_the_next_restart() throws Exception {
        // GIVEN
        // The previous limit to load was 2500, so go some above that
        GraphDatabaseAPI db = databaseWithManyPropertyKeys(3000);
        int countBefore = propertyKeyCount(db);

        // WHEN
        managementService.shutdown();
        db = database();
        createNodeWithProperty(db, key(2800), true);

        // THEN
        assertEquals(countBefore, propertyKeyCount(db));
        managementService.shutdown();
    }

    @Test
    void concurrently_creating_same_property_key_in_different_transactions_should_end_up_with_same_key_id()
            throws Exception {
        // GIVEN
        DatabaseManagementService managementService =
                new TestDatabaseManagementServiceBuilder().impermanent().build();
        GraphDatabaseAPI db = (GraphDatabaseAPI) managementService.database(DEFAULT_DATABASE_NAME);
        Worker worker1 = new Worker("w1", db);
        Worker worker2 = new Worker("w2", db);
        worker1.beginTx();
        worker2.beginTx();

        // WHEN
        String key = "mykey";
        worker1.setProperty(key);
        worker2.setProperty(key);
        worker1.commit();
        worker2.commit();
        worker1.close();
        worker2.close();

        // THEN
        assertEquals(1, propertyKeyCount(db));
        managementService.shutdown();
    }

    private GraphDatabaseAPI database() {
        managementService = new TestDatabaseManagementServiceBuilder(databaseLayout)
                .setConfig(GraphDatabaseSettings.fail_on_missing_files, false)
                .build();
        return (GraphDatabaseAPI) managementService.database(DEFAULT_DATABASE_NAME);
    }

    private GraphDatabaseAPI databaseWithManyPropertyKeys(int propertyKeyCount) throws IOException {
        var cacheTracer = PageCacheTracer.NULL;
        var contextFactory = new CursorContextFactory(cacheTracer, EMPTY_CONTEXT_SUPPLIER);
        var cursorContext = contextFactory.create("databaseWithManyPropertyKeys");
        StoreFactory storeFactory = new StoreFactory(
                databaseLayout,
                Config.defaults(),
                new DefaultIdGeneratorFactory(fileSystem, immediate(), cacheTracer, databaseLayout.getDatabaseName()),
                pageCache,
                cacheTracer,
                fileSystem,
                NullLogProvider.getInstance(),
                contextFactory,
                false,
                LogTailLogVersionsMetadata.EMPTY_LOG_TAIL);
        NeoStores neoStores = storeFactory.openAllNeoStores();
        PropertyKeyTokenStore store = neoStores.getPropertyKeyTokenStore();
        DynamicAllocatorProvider allocatorProvider = DynamicAllocatorProviders.nonTransactionalAllocator(neoStores);
        try (var storeCursors = new CachedStoreCursors(neoStores, NULL_CONTEXT);
                var cursor = storeCursors.writeCursor(PROPERTY_KEY_TOKEN_CURSOR)) {
            for (int i = 0; i < propertyKeyCount; i++) {
                PropertyKeyTokenRecord record =
                        new PropertyKeyTokenRecord((int) store.getIdGenerator().nextId(cursorContext));
                record.setInUse(true);
                Collection<DynamicRecord> nameRecords = store.allocateNameRecords(
                        PropertyStore.encodeString(key(i)),
                        allocatorProvider.allocator(PROPERTY_KEY_TOKEN_NAME),
                        cursorContext,
                        INSTANCE);
                record.addNameRecords(nameRecords);
                record.setNameId((int) Iterables.first(nameRecords).getId());
                store.updateRecord(record, cursor, NULL_CONTEXT, storeCursors);
            }
        }

        neoStores.flush(DatabaseFlushEvent.NULL, cursorContext);
        neoStores.close();

        return database();
    }

    private static String key(int i) {
        return "key" + i;
    }

    private static void createNodeWithProperty(GraphDatabaseService db, String key, Object value) {
        try (Transaction tx = db.beginTx()) {
            Node node = tx.createNode();
            node.setProperty(key, value);
            tx.commit();
        }
    }

    private static int propertyKeyCount(GraphDatabaseAPI db) throws TransactionFailureException {
        Kernel kernelAPI = db.getDependencyResolver().resolveDependency(Kernel.class);
        try (KernelTransaction tx =
                kernelAPI.beginTransaction(KernelTransaction.Type.IMPLICIT, AnonymousContext.read())) {
            return tx.tokenRead().propertyKeyCount();
        }
    }

    private static class Worker extends OtherThreadExecutor {
        private final GraphDatabaseService db;
        private Transaction tx;

        Worker(String name, GraphDatabaseService db) {
            super(name);
            this.db = db;
        }

        void beginTx() throws Exception {
            execute(() -> {
                tx = db.beginTx();
                return null;
            });
        }

        void setProperty(String key) throws Exception {
            execute(() -> {
                Node node = tx.createNode();
                node.setProperty(key, true);
                return null;
            });
        }

        void commit() throws Exception {
            execute(() -> {
                tx.commit();
                return null;
            });
        }
    }
}
