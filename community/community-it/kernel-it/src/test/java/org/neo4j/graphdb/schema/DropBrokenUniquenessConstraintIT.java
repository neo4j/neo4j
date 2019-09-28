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

import org.junit.jupiter.api.Test;

import org.neo4j.exceptions.KernelException;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Transaction;
import org.neo4j.internal.recordstorage.RecordStorageEngine;
import org.neo4j.internal.recordstorage.SchemaRuleAccess;
import org.neo4j.internal.schema.IndexDescriptor;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.test.extension.DbmsExtension;
import org.neo4j.test.extension.Inject;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.neo4j.internal.helpers.collection.Iterators.loop;
import static org.neo4j.internal.helpers.collection.Iterators.single;

@DbmsExtension
class DropBrokenUniquenessConstraintIT
{
    private final Label label = Label.label( "Label" );
    private final String key = "key";

    @Inject
    private GraphDatabaseAPI db;

    @Test
    void shouldDropUniquenessConstraintWithBackingIndexNotInUse()
    {
        // given
        try ( Transaction tx = db.beginTx() )
        {
            tx.schema().constraintFor( label ).assertPropertyIsUnique( key ).create();
            tx.commit();
        }

        // when intentionally breaking the schema by setting the backing index rule to unused
        RecordStorageEngine storageEngine = db.getDependencyResolver().resolveDependency( RecordStorageEngine.class );
        SchemaRuleAccess schemaRules = storageEngine.testAccessSchemaRules();
        schemaRules.indexesGetAll().forEachRemaining( schemaRules::deleteSchemaRule );
        // At this point the SchemaCache doesn't know about this change so we have to reload it
        storageEngine.loadSchemaCache();
        try ( Transaction tx = db.beginTx() )
        {
            single( tx.schema().getConstraints( label ).iterator() ).drop();
            tx.commit();
        }

        // then
        try ( Transaction tx = db.beginTx() )
        {
            assertFalse( tx.schema().getConstraints().iterator().hasNext() );
            assertFalse( tx.schema().getIndexes().iterator().hasNext() );
        }
    }

    @Test
    void shouldDropUniquenessConstraintWithBackingIndexHavingNoOwner() throws KernelException
    {
        // given
        try ( Transaction tx = db.beginTx() )
        {
            tx.schema().constraintFor( label ).assertPropertyIsUnique( key ).create();
            tx.commit();
        }

        // when intentionally breaking the schema by setting the backing index rule to unused
        RecordStorageEngine storageEngine = db.getDependencyResolver().resolveDependency( RecordStorageEngine.class );
        SchemaRuleAccess schemaRules = storageEngine.testAccessSchemaRules();
        writeSchemaRulesWithoutConstraint( schemaRules );
        // At this point the SchemaCache doesn't know about this change so we have to reload it
        storageEngine.loadSchemaCache();
        try ( Transaction tx = db.beginTx() )
        {
            single( tx.schema().getConstraints( label ).iterator() ).drop();
            tx.commit();
        }

        // then
        try ( Transaction tx = db.beginTx() )
        {
            assertFalse( tx.schema().getConstraints().iterator().hasNext() );
            assertFalse( tx.schema().getIndexes().iterator().hasNext() );
        }
    }

    @Test
    void shouldDropUniquenessConstraintWhereConstraintRecordIsMissing()
    {
        // given
        try ( Transaction tx = db.beginTx() )
        {
            tx.schema().constraintFor( label ).assertPropertyIsUnique( key ).create();
            tx.commit();
        }

        // when intentionally breaking the schema by setting the backing index rule to unused
        RecordStorageEngine storageEngine = db.getDependencyResolver().resolveDependency( RecordStorageEngine.class );
        SchemaRuleAccess schemaRules = storageEngine.testAccessSchemaRules();
        schemaRules.constraintsGetAllIgnoreMalformed().forEachRemaining( schemaRules::deleteSchemaRule );

        // At this point the SchemaCache doesn't know about this change so we have to reload it
        storageEngine.loadSchemaCache();
        try ( Transaction tx = db.beginTx() )
        {
            // We don't use single() here, because it is okay for the schema cache reload to clean up after us.
            tx.schema().getConstraints( label ).forEach( ConstraintDefinition::drop );
            tx.schema().getIndexes( label ).forEach( IndexDefinition::drop );
            tx.commit();
        }

        // then
        try ( Transaction tx = db.beginTx() )
        {
            assertFalse( tx.schema().getConstraints().iterator().hasNext() );
            assertFalse( tx.schema().getIndexes().iterator().hasNext() );
        }
    }

    @Test
    void shouldDropUniquenessConstraintWhereConstraintRecordIsMissingAndIndexHasNoOwner() throws KernelException
    {
        // given
        try ( Transaction tx = db.beginTx() )
        {
            tx.schema().constraintFor( label ).assertPropertyIsUnique( key ).create();
            tx.commit();
        }

        // when intentionally breaking the schema by setting the backing index rule to unused
        RecordStorageEngine storageEngine = db.getDependencyResolver().resolveDependency( RecordStorageEngine.class );
        SchemaRuleAccess schemaRules = storageEngine.testAccessSchemaRules();
        schemaRules.constraintsGetAllIgnoreMalformed().forEachRemaining( schemaRules::deleteSchemaRule );
        writeSchemaRulesWithoutConstraint( schemaRules );

        // At this point the SchemaCache doesn't know about this change so we have to reload it
        storageEngine.loadSchemaCache();
        try ( Transaction tx = db.beginTx() )
        {
            // We don't use single() here, because it is okay for the schema cache reload to clean up after us.
            tx.schema().getConstraints( label ).forEach( ConstraintDefinition::drop );
            tx.schema().getIndexes( label ).forEach( IndexDefinition::drop );
            tx.commit();
        }

        // then
        try ( Transaction tx = db.beginTx() )
        {
            assertFalse( tx.schema().getConstraints().iterator().hasNext() );
            assertFalse( tx.schema().getIndexes().iterator().hasNext() );
        }
    }

    private void writeSchemaRulesWithoutConstraint( SchemaRuleAccess schemaRules ) throws KernelException
    {
        for ( IndexDescriptor rule : loop( schemaRules.indexesGetAll() ) )
        {
            schemaRules.writeSchemaRule( rule );
        }
    }
}
