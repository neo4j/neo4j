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
    public void addIndexRuleInATransaction() throws Exception
    {
        // GIVEN
        newTransaction();

        // WHEN
        IndexDescriptor expectedRule = statement.indexCreate( getState(), labelId, propertyKey );
        commit();

        // THEN
        newTransaction();
        assertEquals( asSet( expectedRule ),
                      asSet( statement.indexesGetForLabel( getState(), labelId ) ) );
        assertEquals( expectedRule, statement.indexesGetForLabelAndPropertyKey( getState(), labelId, propertyKey ) );
        commit();
    }

    @Test
    public void committedAndTransactionalIndexRulesShouldBeMerged() throws Exception
    {
        // GIVEN
        newTransaction();
        IndexDescriptor existingRule = statement.indexCreate( getState(), labelId, propertyKey );
        commit();

        // WHEN
        newTransaction();
        long propertyKey2 = 10;
        IndexDescriptor addedRule = statement.indexCreate( getState(), labelId, propertyKey2 );
        Set<IndexDescriptor> indexRulesInTx = asSet( statement.indexesGetForLabel( getState(), labelId ) );
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
        statement.indexCreate( getState(), labelId, propertyKey );
        // don't mark as success
        rollback();

        // THEN
        newTransaction();
        assertEquals( emptySetOf( IndexDescriptor.class ),
                asSet( readOnlyContext().indexesGetForLabel( getState(), labelId ) ) );
        commit();
    }

    @Test
    public void shouldRemoveAConstraintIndexWithoutOwnerInRecovery() throws Exception
    {
        // given
        newTransaction();
        statement.uniqueIndexCreate( getState(), labelId, propertyKey );
        commit();

        // when
        restartDb();

        // then
        newTransaction();
        assertEquals( emptySetOf( IndexDescriptor.class ),
                asSet( readOnlyContext().indexesGetForLabel( getState(), labelId ) ) );
        commit();
    }

    @Test
    public void shouldDisallowDroppingIndexThatDoesNotExist() throws Exception
    {
        // given
        newTransaction();
        IndexDescriptor index = statement.indexCreate( getState(), labelId, propertyKey );
        commit();
        newTransaction();
        statement.indexDrop( getState(), index );
        commit();

        // when
        try
        {
            newTransaction();
            statement.indexDrop( getState(), index );
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
        statement.uniqueIndexCreate( getState(), labelId, propertyKey );
        commit();

        // when
        try
        {
            newTransaction();
            statement.indexCreate( getState(), labelId, propertyKey );
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
        IndexDescriptor index = statement.uniqueIndexCreate( getState(), labelId, propertyKey );
        commit();

        // when
        try
        {
            newTransaction();
            statement.indexDrop( getState(), index );
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
        statement.uniqueIndexCreate( getState(), statement.labelGetOrCreateForName( getState(), "Label1" ),
                                     statement.propertyKeyGetOrCreateForName( getState(), "property1" ) );
        commit();

        // when
        newTransaction();
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
        commit();
    }

    @Test
    public void shouldNotListConstraintIndexesAmongIndexes() throws Exception
    {
        // given
        newTransaction();
        statement.uniqueIndexCreate( getState(), labelId, propertyKey );
        commit();

        // then/when
        newTransaction();
        assertFalse( statement.indexesGetAll( getState() ).hasNext() );
        assertFalse( statement.indexesGetForLabel( getState(), labelId ).hasNext() );
    }

    @Test
    public void shouldNotListIndexesAmongConstraintIndexes() throws Exception
    {
        // given
        newTransaction();
        statement.indexCreate( getState(), labelId, propertyKey );
        commit();

        // then/when
        newTransaction();
        assertFalse( statement.uniqueIndexesGetAll( getState() ).hasNext() );
        assertFalse( statement.uniqueIndexesGetForLabel( getState(), labelId ).hasNext() );
    }
}
