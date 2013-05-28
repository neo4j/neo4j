/**
 * Copyright (c) 2002-2013 "Neo Technology,"
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

import org.neo4j.graphdb.schema.IndexDefinition;
import org.neo4j.kernel.api.StatementContext;
import org.neo4j.kernel.api.exceptions.index.IndexNotFoundKernelException;
import org.neo4j.kernel.api.exceptions.schema.SchemaKernelException;
import org.neo4j.kernel.impl.api.integrationtest.KernelIntegrationTest;

import static java.lang.String.format;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import static org.neo4j.helpers.collection.IteratorUtil.asSet;
import static org.neo4j.helpers.collection.IteratorUtil.emptySetOf;

public class IndexIT extends KernelIntegrationTest
{
    long labelId = 5, propertyKey = 8;

    @Test
    public void createANewIndex() throws Exception
    {
        // GIVEN
        newTransaction();

        // WHEN
        IndexDescriptor rule = statement.indexCreate( labelId, propertyKey );
        commit();

        // AND WHEN the index is created
        awaitIndexOnline( rule );

        // THEN
    }

    @Test
    public void addIndexRuleInATransaction() throws Exception
    {
        // GIVEN
        newTransaction();

        // WHEN
        IndexDescriptor expectedRule = statement.indexCreate( labelId, propertyKey );
        commit();

        // THEN
        StatementContext roStatement = readOnlyContext();
        assertEquals( asSet( expectedRule ),
                      asSet( roStatement.indexesGetForLabel( labelId ) ) );
        assertEquals( expectedRule, roStatement.indexesGetForLabelAndPropertyKey( labelId, propertyKey ) );
    }

    @Test
    public void committedAndTransactionalIndexRulesShouldBeMerged() throws Exception
    {
        // GIVEN
        newTransaction();
        IndexDescriptor existingRule = statement.indexCreate( labelId, propertyKey );
        commit();

        // WHEN
        newTransaction();
        long propertyKey2 = 10;
        IndexDescriptor addedRule = statement.indexCreate( labelId, propertyKey2 );
        Set<IndexDescriptor> indexRulesInTx = asSet( statement.indexesGetForLabel( labelId ) );
        commit();

        // THEN
        assertEquals( asSet( existingRule, addedRule ), indexRulesInTx );
    }

    @Test
    public void rollBackIndexRuleShouldNotBeCommitted() throws Exception
    {
        // GIVEN
        newTransaction();

        // WHEN
        statement.indexCreate( labelId, propertyKey );
        // don't mark as success
        rollback();

        // THEN
        assertEquals( emptySetOf( IndexDescriptor.class ), asSet( readOnlyContext().indexesGetForLabel( labelId ) ) );
    }

    @Test
    public void shouldRemoveAConstraintIndexWithoutOwnerInRecovery() throws Exception
    {
        // given
        newTransaction();
        statement.uniqueIndexCreate( labelId, propertyKey );
        commit();

        // when
        restartDb();

        // then
        assertEquals( emptySetOf( IndexDescriptor.class ), asSet( readOnlyContext().indexesGetForLabel( labelId ) ) );
    }

    @Test
    public void shouldDisallowDroppingIndexThatDoesNotExist() throws Exception
    {
        // given
        newTransaction();
        IndexDescriptor index = statement.indexCreate( labelId, propertyKey );
        commit();
        newTransaction();
        statement.indexDrop( index );
        commit();

        // when
        try
        {
            newTransaction();
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
    public void shouldFailToCreateIndexWhereAConstraintIndexAlreadyExists() throws Exception
    {
        // given
        newTransaction();
        statement.uniqueIndexCreate( labelId, propertyKey );
        commit();

        // when
        try
        {
            newTransaction();
            statement.indexCreate( labelId, propertyKey );
            commit();

            fail( "expected exception" );
        }
        // then
        catch ( SchemaKernelException e )
        {
            assertEquals( format( "Unable to add index on [label: %s, %s] : Already constrained " +
                    "CONSTRAINT ON ( n:label[%s] ) ASSERT n.property[%s] IS UNIQUE.",
                    labelId, propertyKey, labelId, propertyKey ), e.getMessage() );
        }
    }

    @Test
    public void shouldNotBeAbleToRemoveAConstraintIndexAsIfItWasARegularIndex() throws Exception
    {
        // given
        newTransaction();
        IndexDescriptor index = statement.uniqueIndexCreate( labelId, propertyKey );
        commit();

        // when
        try
        {
            newTransaction();
            statement.indexDrop( index );
            commit();

            fail( "expected exception" );
        }
        // then
        catch ( SchemaKernelException e )
        {
            assertEquals( "Unable to drop index on :label[5](property[8]): Index belongs to constraint: " +
                    ":label[5](property[8])", e.getMessage() );
        }
    }

    @Test
    public void shouldListConstraintIndexesInTheBeansAPI() throws Exception
    {
        // given
        newTransaction();
        statement.uniqueIndexCreate( statement.labelGetOrCreateForName( "Label1" ),
                                     statement.propertyKeyGetOrCreateForName( "property1" ) );
        commit();

        // when
        Set<IndexDefinition> indexes = asSet( db.schema().getIndexes() );

        // then
        assertEquals( 1, indexes.size() );
        IndexDefinition index = indexes.iterator().next();
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

    @Test
    public void shouldNotListConstraintIndexesAmongIndexes() throws Exception
    {
        // given
        newTransaction();
        statement.uniqueIndexCreate( labelId, propertyKey );
        commit();

        // then/when
        newTransaction();
        assertFalse( statement.indexesGetAll().hasNext() );
        assertFalse( statement.indexesGetForLabel( labelId ).hasNext() );
    }

    @Test
    public void shouldNotListIndexesAmongConstraintIndexes() throws Exception
    {
        // given
        newTransaction();
        statement.indexCreate( labelId, propertyKey );
        commit();

        // then/when
        newTransaction();
        assertFalse( statement.uniqueIndexesGetAll().hasNext() );
        assertFalse( statement.uniqueIndexesGetForLabel( labelId ).hasNext() );
    }

    private void awaitIndexOnline( IndexDescriptor indexRule ) throws IndexNotFoundKernelException
    {
        SchemaIndexTestHelper.awaitIndexOnline( readOnlyContext(), indexRule );
    }
}
