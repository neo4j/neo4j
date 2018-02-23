/*
 * Copyright (c) 2002-2018 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
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

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import javax.annotation.Resource;

import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Transaction;
import org.neo4j.kernel.impl.storageengine.impl.recordstorage.RecordStorageEngine;
import org.neo4j.kernel.impl.store.SchemaStore;
import org.neo4j.kernel.impl.store.record.DynamicRecord;
import org.neo4j.kernel.impl.store.record.IndexRule;
import org.neo4j.storageengine.api.schema.SchemaRule;
import org.neo4j.test.extension.EmbeddedDatabaseExtension;
import org.neo4j.test.rule.EmbeddedDatabaseRule;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.neo4j.helpers.collection.Iterators.filter;
import static org.neo4j.helpers.collection.Iterators.single;

@ExtendWith( EmbeddedDatabaseExtension.class )
public class DropBrokenUniquenessConstraintIT
{
    private final Label label = Label.label( "Label" );
    private final String key = "key";

    @Resource
    public EmbeddedDatabaseRule db;

    @Test
    public void shouldDropUniquenessConstraintWithBrokenBackingIndex()
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
        try ( Transaction tx = db.beginTx() )
        {
            assertFalse( db.schema().getConstraints().iterator().hasNext() );
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
