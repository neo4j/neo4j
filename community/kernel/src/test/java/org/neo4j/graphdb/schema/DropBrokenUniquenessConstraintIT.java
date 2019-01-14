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
package org.neo4j.graphdb.schema;

import org.junit.Rule;
import org.junit.Test;

import java.util.List;

import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Transaction;
import org.neo4j.kernel.impl.storageengine.impl.recordstorage.RecordStorageEngine;
import org.neo4j.kernel.impl.store.SchemaStore;
import org.neo4j.kernel.impl.store.record.ConstraintRule;
import org.neo4j.kernel.impl.store.record.DynamicRecord;
import org.neo4j.kernel.impl.store.record.IndexRule;
import org.neo4j.storageengine.api.schema.SchemaRule;
import org.neo4j.test.rule.DatabaseRule;
import org.neo4j.test.rule.EmbeddedDatabaseRule;

import static org.junit.Assert.assertFalse;
import static org.neo4j.helpers.collection.Iterators.filter;
import static org.neo4j.helpers.collection.Iterators.single;
import static org.neo4j.kernel.impl.store.record.IndexRule.constraintIndexRule;

public class DropBrokenUniquenessConstraintIT
{
    private final Label label = Label.label( "Label" );
    private final String key = "key";

    @Rule
    public final DatabaseRule db = new EmbeddedDatabaseRule();

    @Test
    public void shouldDropUniquenessConstraintWithBackingIndexNotInUse()
    {
        // given
        try ( Transaction tx = db.beginTx() )
        {
            db.schema().constraintFor( label ).assertPropertyIsUnique( key ).create();
            tx.success();
        }

        // when intentionally breaking the schema by setting the backing index rule to unused
        RecordStorageEngine storageEngine = db.getDependencyResolver().resolveDependency( RecordStorageEngine.class );
        SchemaStore schemaStore = storageEngine.testAccessNeoStores().getSchemaStore();
        SchemaRule indexRule = single( filter( rule -> rule instanceof IndexRule, schemaStore.loadAllSchemaRules() ) );
        setSchemaRecordNotInUse( schemaStore, indexRule.getId() );
        // At this point the SchemaCache doesn't know about this change so we have to reload it
        storageEngine.loadSchemaCache();
        try ( Transaction tx = db.beginTx() )
        {
            single( db.schema().getConstraints( label ).iterator() ).drop();
            tx.success();
        }

        // then
        try ( Transaction ignore = db.beginTx() )
        {
            assertFalse( db.schema().getConstraints().iterator().hasNext() );
            assertFalse( db.schema().getIndexes().iterator().hasNext() );
        }
    }

    @Test
    public void shouldDropUniquenessConstraintWithBackingIndexHavingNoOwner() throws Exception
    {
        // given
        try ( Transaction tx = db.beginTx() )
        {
            db.schema().constraintFor( label ).assertPropertyIsUnique( key ).create();
            tx.success();
        }

        // when intentionally breaking the schema by setting the backing index rule to unused
        RecordStorageEngine storageEngine = db.getDependencyResolver().resolveDependency( RecordStorageEngine.class );
        SchemaStore schemaStore = storageEngine.testAccessNeoStores().getSchemaStore();
        SchemaRule indexRule = single( filter( rule -> rule instanceof IndexRule, schemaStore.loadAllSchemaRules() ) );
        setOwnerNull( schemaStore, (IndexRule) indexRule );
        // At this point the SchemaCache doesn't know about this change so we have to reload it
        storageEngine.loadSchemaCache();
        try ( Transaction tx = db.beginTx() )
        {
            single( db.schema().getConstraints( label ).iterator() ).drop();
            tx.success();
        }

        // then
        try ( Transaction ignore = db.beginTx() )
        {
            assertFalse( db.schema().getConstraints().iterator().hasNext() );
            assertFalse( db.schema().getIndexes().iterator().hasNext() );
        }
    }

    @Test
    public void shouldDropUniquenessConstraintWhereConstraintRecordIsMissing() throws Exception
    {
        // given
        try ( Transaction tx = db.beginTx() )
        {
            db.schema().constraintFor( label ).assertPropertyIsUnique( key ).create();
            tx.success();
        }

        // when intentionally breaking the schema by setting the backing index rule to unused
        RecordStorageEngine storageEngine = db.getDependencyResolver().resolveDependency( RecordStorageEngine.class );
        SchemaStore schemaStore = storageEngine.testAccessNeoStores().getSchemaStore();
        SchemaRule indexRule = single( filter( rule -> rule instanceof ConstraintRule, schemaStore.loadAllSchemaRules() ) );
        setSchemaRecordNotInUse( schemaStore, indexRule.getId() );
        // At this point the SchemaCache doesn't know about this change so we have to reload it
        storageEngine.loadSchemaCache();
        try ( Transaction tx = db.beginTx() )
        {
            // We don't use single() here, because it is okay for the schema cache reload to clean up after us.
            db.schema().getConstraints( label ).forEach( ConstraintDefinition::drop );
            db.schema().getIndexes( label ).forEach( IndexDefinition::drop );
            tx.success();
        }

        // then
        try ( Transaction ignore = db.beginTx() )
        {
            assertFalse( db.schema().getConstraints().iterator().hasNext() );
            assertFalse( db.schema().getIndexes().iterator().hasNext() );
        }
    }

    @Test
    public void shouldDropUniquenessConstraintWhereConstraintRecordIsMissingAndIndexHasNoOwner() throws Exception
    {
        // given
        try ( Transaction tx = db.beginTx() )
        {
            db.schema().constraintFor( label ).assertPropertyIsUnique( key ).create();
            tx.success();
        }

        // when intentionally breaking the schema by setting the backing index rule to unused
        RecordStorageEngine storageEngine = db.getDependencyResolver().resolveDependency( RecordStorageEngine.class );
        SchemaStore schemaStore = storageEngine.testAccessNeoStores().getSchemaStore();
        SchemaRule constraintRule = single( filter( rule -> rule instanceof ConstraintRule, schemaStore.loadAllSchemaRules() ) );
        setSchemaRecordNotInUse( schemaStore, constraintRule.getId() );
        SchemaRule indexRule = single( filter( rule -> rule instanceof IndexRule, schemaStore.loadAllSchemaRules() ) );
        setOwnerNull( schemaStore, (IndexRule) indexRule );
        // At this point the SchemaCache doesn't know about this change so we have to reload it
        storageEngine.loadSchemaCache();
        try ( Transaction tx = db.beginTx() )
        {
            // We don't use single() here, because it is okay for the schema cache reload to clean up after us.
            db.schema().getConstraints( label ).forEach( ConstraintDefinition::drop );
            db.schema().getIndexes( label ).forEach( IndexDefinition::drop );
            tx.success();
        }

        // then
        try ( Transaction ignore = db.beginTx() )
        {
            assertFalse( db.schema().getConstraints().iterator().hasNext() );
            assertFalse( db.schema().getIndexes().iterator().hasNext() );
        }
    }

    private void setOwnerNull( SchemaStore schemaStore, IndexRule rule )
    {
        rule = constraintIndexRule( rule.getId(), rule.getIndexDescriptor(), rule.getProviderDescriptor(), null );
        List<DynamicRecord> dynamicRecords = schemaStore.allocateFrom( rule );
        for ( DynamicRecord record : dynamicRecords )
        {
            schemaStore.updateRecord( record );
        }
    }

    private void setSchemaRecordNotInUse( SchemaStore schemaStore, long id )
    {
        DynamicRecord record = schemaStore.newRecord();
        record.setId( id );
        record.setInUse( false );
        schemaStore.updateRecord( record );
    }
}
