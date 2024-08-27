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
package org.neo4j.kernel.impl;

import static org.neo4j.io.pagecache.context.CursorContext.NULL_CONTEXT;

import org.junit.jupiter.api.TestInstance;
import org.neo4j.configuration.GraphDatabaseSettings;
import org.neo4j.dbms.api.DatabaseManagementService;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Transaction;
import org.neo4j.internal.id.IdGenerator;
import org.neo4j.internal.id.IdGeneratorFactory;
import org.neo4j.internal.id.IdType;
import org.neo4j.internal.recordstorage.RecordStorageEngine;
import org.neo4j.kernel.impl.store.NeoStores;
import org.neo4j.kernel.impl.store.NodeStore;
import org.neo4j.kernel.impl.store.PropertyStore;
import org.neo4j.kernel.impl.store.RecordStore;
import org.neo4j.kernel.impl.store.format.FormatFamily;
import org.neo4j.kernel.impl.store.record.AbstractBaseRecord;
import org.neo4j.kernel.impl.store.record.RecordLoad;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.memory.EmptyMemoryTracker;
import org.neo4j.storageengine.api.cursor.StoreCursors;
import org.neo4j.test.TestDatabaseManagementServiceBuilder;
import org.neo4j.test.extension.ExtensionCallback;
import org.neo4j.test.extension.ImpermanentDbmsExtension;
import org.neo4j.test.extension.Inject;

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
@ImpermanentDbmsExtension(configurationCallback = "configureDb")
public abstract class AbstractNeo4jTestCase {
    @Inject
    private DatabaseManagementService managementService;

    @Inject
    private GraphDatabaseAPI graphDb;

    @ExtensionCallback
    void configureDb(TestDatabaseManagementServiceBuilder builder) {
        builder.setConfig(GraphDatabaseSettings.db_format, FormatFamily.ALIGNED.name());
    }

    protected GraphDatabaseService getGraphDb() {
        return graphDb;
    }

    protected DatabaseManagementService getManagementService() {
        return managementService;
    }

    protected GraphDatabaseAPI getGraphDbAPI() {
        return graphDb;
    }

    protected Node createNode() {
        Node node;
        try (Transaction transaction = graphDb.beginTx()) {
            node = transaction.createNode();
            transaction.commit();
        }
        return node;
    }

    protected IdGenerator getIdGenerator(IdType idType) {
        return graphDb.getDependencyResolver()
                .resolveDependency(IdGeneratorFactory.class)
                .get(idType);
    }

    protected long propertyRecordsInUse() {
        return numberOfRecordsInUse(propertyStore());
    }

    private static <RECORD extends AbstractBaseRecord> int numberOfRecordsInUse(RecordStore<RECORD> store) {
        int inUse = 0;
        try (var cursor = store.openPageCursorForReading(0, NULL_CONTEXT)) {
            IdGenerator idGenerator = store.getIdGenerator();
            for (long id = store.getNumberOfReservedLowIds(); id < idGenerator.getHighId(); id++) {
                RECORD record = store.getRecordByCursor(
                        id, store.newRecord(), RecordLoad.FORCE, cursor, EmptyMemoryTracker.INSTANCE);
                if (record.inUse()) {
                    inUse++;
                }
            }
        }
        return inUse;
    }

    protected static <RECORD extends AbstractBaseRecord> long lastUsedRecordId(RecordStore<RECORD> store) {
        long usedIds = store.getIdGenerator().getHighId();
        try (var cursor = store.openPageCursorForReading(usedIds, NULL_CONTEXT)) {
            for (long id = usedIds; id > store.getNumberOfReservedLowIds(); id--) {
                RECORD record = store.getRecordByCursor(
                        id, store.newRecord(), RecordLoad.FORCE, cursor, EmptyMemoryTracker.INSTANCE);
                if (record.inUse()) {
                    return id;
                }
            }
        }
        return 0;
    }

    protected long dynamicStringRecordsInUse() {
        return numberOfRecordsInUse(propertyStore().getStringStore());
    }

    protected long dynamicArrayRecordsInUse() {
        return numberOfRecordsInUse(propertyStore().getArrayStore());
    }

    protected StoreCursors createStoreCursors() {
        var storageEngine = graphDb.getDependencyResolver().resolveDependency(RecordStorageEngine.class);
        return storageEngine.createStorageCursors(NULL_CONTEXT);
    }

    protected NeoStores neoStores() {
        return graphDb.getDependencyResolver()
                .resolveDependency(RecordStorageEngine.class)
                .testAccessNeoStores();
    }

    protected PropertyStore propertyStore() {
        return neoStores().getPropertyStore();
    }

    protected NodeStore nodeStore() {
        return neoStores().getNodeStore();
    }
}
