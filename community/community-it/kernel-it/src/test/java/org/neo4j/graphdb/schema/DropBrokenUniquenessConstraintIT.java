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
package org.neo4j.graphdb.schema;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.neo4j.internal.helpers.collection.Iterators.loop;
import static org.neo4j.internal.helpers.collection.Iterators.single;
import static org.neo4j.internal.recordstorage.RecordCursorTypes.PROPERTY_CURSOR;
import static org.neo4j.internal.recordstorage.RecordCursorTypes.SCHEMA_CURSOR;
import static org.neo4j.io.pagecache.context.CursorContext.NULL_CONTEXT;
import static org.neo4j.kernel.impl.store.DynamicAllocatorProviders.nonTransactionalAllocator;
import static org.neo4j.kernel.impl.store.record.Record.NO_NEXT_PROPERTY;
import static org.neo4j.kernel.impl.store.record.RecordLoad.NORMAL;

import org.assertj.core.util.Streams;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.neo4j.configuration.GraphDatabaseSettings;
import org.neo4j.exceptions.KernelException;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Transaction;
import org.neo4j.internal.recordstorage.RecordStorageEngine;
import org.neo4j.internal.recordstorage.SchemaRuleAccess;
import org.neo4j.internal.schema.IndexDescriptor;
import org.neo4j.internal.schema.SchemaRule;
import org.neo4j.io.pagecache.PageCursor;
import org.neo4j.io.pagecache.context.CursorContext;
import org.neo4j.kernel.impl.store.DynamicAllocatorProvider;
import org.neo4j.kernel.impl.store.PropertyStore;
import org.neo4j.kernel.impl.store.SchemaStore;
import org.neo4j.kernel.impl.store.format.FormatFamily;
import org.neo4j.kernel.impl.store.record.PropertyRecord;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.memory.EmptyMemoryTracker;
import org.neo4j.storageengine.api.cursor.StoreCursors;
import org.neo4j.storageengine.util.IdUpdateListener;
import org.neo4j.test.TestDatabaseManagementServiceBuilder;
import org.neo4j.test.extension.DbmsExtension;
import org.neo4j.test.extension.ExtensionCallback;
import org.neo4j.test.extension.Inject;

@DbmsExtension(configurationCallback = "configure")
class DropBrokenUniquenessConstraintIT {
    private final Label label = Label.label("Label");
    private final String key = "key";

    @Inject
    private GraphDatabaseAPI db;

    @Inject
    private RecordStorageEngine storageEngine;

    private long initialConstraintCount;
    private long initialIndexCount;
    private SchemaStore schemaStore;

    @ExtensionCallback
    void configure(TestDatabaseManagementServiceBuilder builder) {
        builder.setConfig(GraphDatabaseSettings.db_format, FormatFamily.ALIGNED.name());
    }

    @BeforeEach
    void getInitialCounts() {
        try (Transaction tx = db.beginTx()) {
            initialConstraintCount =
                    Streams.stream(tx.schema().getConstraints()).count();
            initialIndexCount = Streams.stream(tx.schema().getIndexes()).count();
        }
        schemaStore = storageEngine.testAccessNeoStores().getSchemaStore();
    }

    @AfterEach
    void assertNoAdditionalConstraintsOrIndexes() {
        try (Transaction tx = db.beginTx()) {
            assertEquals(
                    initialConstraintCount,
                    Streams.stream(tx.schema().getConstraints()).count());
            assertEquals(
                    initialIndexCount, Streams.stream(tx.schema().getIndexes()).count());
        }
    }

    @Test
    void shouldDropUniquenessConstraintWithBackingIndexNotInUse() {
        // given
        String backingIndexName;
        try (Transaction tx = db.beginTx()) {
            tx.schema().constraintFor(label).assertPropertyIsUnique(key).create();
            backingIndexName = single(tx.schema().getIndexes(label).iterator()).getName();
            tx.commit();
        }

        // when intentionally breaking the schema by setting the backing index rule to unused
        SchemaRuleAccess schemaRules = storageEngine.testAccessSchemaRules();
        try (var storeCursors = storageEngine.createStorageCursors(NULL_CONTEXT)) {
            deleteSchemaRule(
                    schemaRules.indexGetForName(backingIndexName, storeCursors, EmptyMemoryTracker.INSTANCE),
                    NULL_CONTEXT,
                    storeCursors);
        }
        // At this point the SchemaCache doesn't know about this change so we have to reload it
        storageEngine.loadSchemaCache();
        try (Transaction tx = db.beginTx()) {
            single(tx.schema().getConstraints(label).iterator()).drop();
            tx.commit();
        }
    }

    @Test
    void shouldDropUniquenessConstraintWithBackingIndexHavingNoOwner() throws KernelException {
        // given
        try (Transaction tx = db.beginTx()) {
            tx.schema().constraintFor(label).assertPropertyIsUnique(key).create();
            tx.commit();
        }

        // when intentionally breaking the schema by setting the backing index rule to unused
        SchemaRuleAccess schemaRules = storageEngine.testAccessSchemaRules();
        try (var storeCursors = storageEngine.createStorageCursors(NULL_CONTEXT)) {
            writeSchemaRulesWithoutConstraint(
                    schemaRules, nonTransactionalAllocator(storageEngine.testAccessNeoStores()), storeCursors);
        }
        // At this point the SchemaCache doesn't know about this change so we have to reload it
        storageEngine.loadSchemaCache();
        try (Transaction tx = db.beginTx()) {
            single(tx.schema().getConstraints(label).iterator()).drop();
            tx.commit();
        }

        // then
        // AfterEach
    }

    @Test
    void shouldDropUniquenessConstraintWhereConstraintRecordIsMissing() {
        // given
        try (Transaction tx = db.beginTx()) {
            tx.schema().constraintFor(label).assertPropertyIsUnique(key).create();
            tx.commit();
        }

        // when intentionally breaking the schema by setting the backing index rule to unused
        SchemaRuleAccess schemaRules = storageEngine.testAccessSchemaRules();
        try (var storeCursors = storageEngine.createStorageCursors(NULL_CONTEXT)) {
            schemaRules
                    .constraintsGetAllIgnoreMalformed(storeCursors, EmptyMemoryTracker.INSTANCE)
                    .forEachRemaining(rule -> deleteSchemaRule(rule, NULL_CONTEXT, storeCursors));
        }

        // At this point the SchemaCache doesn't know about this change so we have to reload it
        storageEngine.loadSchemaCache();
        try (Transaction tx = db.beginTx()) {
            // We don't use single() here, because it is okay for the schema cache reload to clean up after us.
            tx.schema().getConstraints(label).forEach(ConstraintDefinition::drop);
            tx.schema().getIndexes(label).forEach(IndexDefinition::drop);
            tx.commit();
        }
    }

    @Test
    void shouldDropUniquenessConstraintWhereConstraintRecordIsMissingAndIndexHasNoOwner() throws KernelException {
        // given
        try (Transaction tx = db.beginTx()) {
            tx.schema().constraintFor(label).assertPropertyIsUnique(key).create();
            tx.commit();
        }

        // when intentionally breaking the schema by setting the backing index rule to unused
        SchemaRuleAccess schemaRules = storageEngine.testAccessSchemaRules();
        try (var storeCursors = storageEngine.createStorageCursors(NULL_CONTEXT)) {
            schemaRules
                    .constraintsGetAllIgnoreMalformed(storeCursors, EmptyMemoryTracker.INSTANCE)
                    .forEachRemaining(rule -> deleteSchemaRule(rule, NULL_CONTEXT, storeCursors));
            writeSchemaRulesWithoutConstraint(
                    schemaRules, nonTransactionalAllocator(storageEngine.testAccessNeoStores()), storeCursors);
        }

        // At this point the SchemaCache doesn't know about this change so we have to reload it
        storageEngine.loadSchemaCache();
        try (Transaction tx = db.beginTx()) {
            // We don't use single() here, because it is okay for the schema cache reload to clean up after us.
            tx.schema().getConstraints(label).forEach(ConstraintDefinition::drop);
            tx.schema().getIndexes(label).forEach(IndexDefinition::drop);
            tx.commit();
        }
    }

    private void deleteSchemaRule(SchemaRule rule, CursorContext cursorContext, StoreCursors storeCursors) {
        var record = schemaStore.getRecordByCursor(
                rule.getId(),
                schemaStore.newRecord(),
                NORMAL,
                storeCursors.readCursor(SCHEMA_CURSOR),
                EmptyMemoryTracker.INSTANCE);
        if (record.inUse()) {
            long nextProp = record.getNextProp();
            record.setInUse(false);
            try (PageCursor writeCursor = storeCursors.writeCursor(SCHEMA_CURSOR)) {
                schemaStore.updateRecord(record, writeCursor, cursorContext, storeCursors);
            }
            PropertyStore propertyStore = schemaStore.propertyStore();
            PropertyRecord props = propertyStore.newRecord();
            var propertyReadCursor = storeCursors.readCursor(PROPERTY_CURSOR);
            while (nextProp != NO_NEXT_PROPERTY.longValue()
                    && propertyStore
                            .getRecordByCursor(nextProp, props, NORMAL, propertyReadCursor, EmptyMemoryTracker.INSTANCE)
                            .inUse()) {
                nextProp = props.getNextProp();
                props.setInUse(false);
                try (PageCursor writeCursor = storeCursors.writeCursor(PROPERTY_CURSOR)) {
                    propertyStore.updateRecord(props, writeCursor, cursorContext, storeCursors);
                }
            }
        }
    }

    private static void writeSchemaRulesWithoutConstraint(
            SchemaRuleAccess schemaRules, DynamicAllocatorProvider allocatorProvider, StoreCursors storeCursors)
            throws KernelException {
        for (IndexDescriptor rule : loop(schemaRules.indexesGetAll(storeCursors, EmptyMemoryTracker.INSTANCE))) {
            schemaRules.writeSchemaRule(
                    rule,
                    IdUpdateListener.DIRECT,
                    allocatorProvider,
                    NULL_CONTEXT,
                    EmptyMemoryTracker.INSTANCE,
                    storeCursors);
        }
    }
}
