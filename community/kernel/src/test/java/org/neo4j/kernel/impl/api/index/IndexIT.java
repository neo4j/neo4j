/*
 * Copyright (c) 2002-2015 "Neo Technology,"
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
package org.neo4j.kernel.impl.api.index;

import java.util.Set;

import org.junit.Test;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.schema.IndexDefinition;
import org.neo4j.kernel.api.SchemaWriteOperations;
import org.neo4j.kernel.api.exceptions.schema.SchemaKernelException;
import org.neo4j.kernel.api.index.IndexDescriptor;
import org.neo4j.kernel.impl.api.integrationtest.KernelIntegrationTest;
import org.neo4j.kernel.impl.api.state.ConstraintIndexCreator;

import static org.junit.Assert.*;
import static org.neo4j.helpers.collection.IteratorUtil.asSet;
import static org.neo4j.helpers.collection.IteratorUtil.emptySetOf;
import static org.neo4j.kernel.impl.util.Providers.singletonProvider;

public class IndexIT extends KernelIntegrationTest
{
    int labelId = 5, propertyKey = 8;

    @Test
    public void addIndexRuleInATransaction() throws Exception
    {
        // GIVEN
        IndexDescriptor expectedRule;
        {
            SchemaWriteOperations statement = schemaWriteOperationsInNewTransaction();

            // WHEN
            expectedRule = statement.indexCreate( labelId, propertyKey );
            commit();
        }

        // THEN
        {
            SchemaWriteOperations statement = schemaWriteOperationsInNewTransaction();
            assertEquals( asSet( expectedRule ),
                          asSet( statement.indexesGetForLabel( labelId ) ) );
            assertEquals( expectedRule, statement.indexesGetForLabelAndPropertyKey( labelId, propertyKey ) );
            commit();
        }
    }

    @Test
    public void committedAndTransactionalIndexRulesShouldBeMerged() throws Exception
    {
        // GIVEN
        IndexDescriptor existingRule;
        {
            SchemaWriteOperations statement = schemaWriteOperationsInNewTransaction();
            existingRule = statement.indexCreate( labelId, propertyKey );
            commit();
        }

        // WHEN
        IndexDescriptor addedRule;
        Set<IndexDescriptor> indexRulesInTx;
        {
            SchemaWriteOperations statement = schemaWriteOperationsInNewTransaction();
            int propertyKey2 = 10;
            addedRule = statement.indexCreate( labelId, propertyKey2 );
            indexRulesInTx = asSet( statement.indexesGetForLabel( labelId ) );
            commit();
        }

        // THEN
        assertEquals( asSet( existingRule, addedRule ), indexRulesInTx );
    }

    @Test
    public void rollBackIndexRuleShouldNotBeCommitted() throws Exception
    {
        // GIVEN
        {
            SchemaWriteOperations statement = schemaWriteOperationsInNewTransaction();

            // WHEN
            statement.indexCreate( labelId, propertyKey );
            // don't mark as success
            rollback();
        }

        // THEN
        {
            SchemaWriteOperations statement = schemaWriteOperationsInNewTransaction();
            assertEquals( emptySetOf( IndexDescriptor.class ), asSet( statement.indexesGetForLabel( labelId ) ) );
            commit();
        }
    }

    @Test
    public void shouldRemoveAConstraintIndexWithoutOwnerInRecovery() throws Exception
    {
        // given
        ConstraintIndexCreator creator = new ConstraintIndexCreator( singletonProvider( kernel ), indexingService );
        creator.createConstraintIndex( labelId, propertyKey );

        // when
        restartDb();

        // then
        {
            SchemaWriteOperations statement = schemaWriteOperationsInNewTransaction();
            assertEquals( emptySetOf( IndexDescriptor.class ), asSet( statement.indexesGetForLabel( labelId ) ) );
            commit();
        }
    }

    @Test
    public void shouldDisallowDroppingIndexThatDoesNotExist() throws Exception
    {
        // given
        IndexDescriptor index;
        {
            SchemaWriteOperations statement = schemaWriteOperationsInNewTransaction();
            index = statement.indexCreate( labelId, propertyKey );
            commit();
        }
        {
            SchemaWriteOperations statement = schemaWriteOperationsInNewTransaction();
            statement.indexDrop( index );
            commit();
        }

        // when
        try
        {
            SchemaWriteOperations statement = schemaWriteOperationsInNewTransaction();
            statement.indexDrop( index );
            commit();
        }
        // then
        catch ( SchemaKernelException e )
        {
            assertEquals( "Unable to drop index on :label[5](property[8]): No such INDEX ON :label[5](property[8]).",
                    e.getMessage() );
        }
    }

    @Test
    public void shouldFailToCreateIndexWhereAConstraintAlreadyExists() throws Exception
    {
        // given
        {
            SchemaWriteOperations statement = schemaWriteOperationsInNewTransaction();
            statement.uniquenessConstraintCreate( labelId, propertyKey );
            commit();
        }

        // when
        try
        {
            SchemaWriteOperations statement = schemaWriteOperationsInNewTransaction();
            statement.indexCreate( labelId, propertyKey );
            commit();

            fail( "expected exception" );
        }
        // then
        catch ( SchemaKernelException e )
        {
            assertEquals( "Label 'label[5]' and property 'key[8]' have a unique constraint defined on " +
                          "them, so an index is already created that matches this.", e.getMessage() );
        }
    }

    @Test
    public void shouldListConstraintIndexesInTheBeansAPI() throws Exception
    {
        // given
        {
            SchemaWriteOperations statement = schemaWriteOperationsInNewTransaction();
            statement.uniquenessConstraintCreate( statement.labelGetOrCreateForName( "Label1" ),
                                         statement.propertyKeyGetOrCreateForName( "property1" ) );
            commit();
        }

        // when
        try ( Transaction tx = db.beginTx() )
        {
            Set<IndexDefinition> indexes;
            IndexDefinition index;
            indexes = asSet( db.schema().getIndexes() );

            // then
            assertEquals( 1, indexes.size() );
            index = indexes.iterator().next();
            assertEquals( "Label1", index.getLabel().name() );
            assertEquals( asSet( "property1" ), asSet( index.getPropertyKeys() ) );
            assertTrue( "index should be a constraint index", index.isConstraintIndex() );

            // when
            try
            {
                index.drop();

                fail( "expected exception" );
            }
            // then
            catch ( IllegalStateException e )
            {
                assertEquals( "Constraint indexes cannot be dropped directly, " +
                        "instead drop the owning uniqueness constraint.", e.getMessage() );
            }
        }
    }

    @Test
    public void shouldNotListConstraintIndexesAmongIndexes() throws Exception
    {
        // given
        {
            SchemaWriteOperations statement = schemaWriteOperationsInNewTransaction();
            statement.uniquenessConstraintCreate( labelId, propertyKey );
            commit();
        }

        // then/when
        {
            SchemaWriteOperations statement = schemaWriteOperationsInNewTransaction();
            assertFalse( statement.indexesGetAll().hasNext() );
            assertFalse( statement.indexesGetForLabel( labelId ).hasNext() );
        }
    }

    @Test
    public void shouldNotListIndexesAmongConstraintIndexes() throws Exception
    {
        // given
        {
            SchemaWriteOperations statement = schemaWriteOperationsInNewTransaction();
            statement.indexCreate( labelId, propertyKey );
            commit();
        }

        // then/when
        {
            SchemaWriteOperations statement = schemaWriteOperationsInNewTransaction();
            assertFalse( statement.uniqueIndexesGetAll().hasNext() );
            assertFalse( statement.uniqueIndexesGetForLabel( labelId ).hasNext() );
        }
    }
}
