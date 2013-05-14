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
import org.neo4j.kernel.api.DataIntegrityKernelException;
import org.neo4j.kernel.api.StatementContext;
import org.neo4j.kernel.api.index.IndexNotFoundKernelException;
import org.neo4j.kernel.impl.api.integrationtest.KernelIntegrationTest;

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
        IndexDescriptor rule = statement.addIndex( labelId, propertyKey );
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
        IndexDescriptor expectedRule = statement.addIndex( labelId, propertyKey );
        commit();

        // THEN
        StatementContext roStatement = readOnlyContext();
        assertEquals( asSet( expectedRule ),
                      asSet( roStatement.getIndexes( labelId ) ) );
        assertEquals( expectedRule, roStatement.getIndex( labelId, propertyKey ) );
    }

    @Test
    public void committedAndTransactionalIndexRulesShouldBeMerged() throws Exception
    {
        // GIVEN
        newTransaction();
        IndexDescriptor existingRule = statement.addIndex( labelId, propertyKey );
        commit();

        // WHEN
        newTransaction();
        long propertyKey2 = 10;
        IndexDescriptor addedRule = statement.addIndex( labelId, propertyKey2 );
        Set<IndexDescriptor> indexRulesInTx = asSet( statement.getIndexes( labelId ) );
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
        statement.addIndex( labelId, propertyKey );
        // don't mark as success
        rollback();

        // THEN
        assertEquals( emptySetOf( IndexDescriptor.class ), asSet( readOnlyContext().getIndexes( labelId ) ) );
    }

    @Test
    public void shouldRemoveAConstraintIndexWithoutOwnerInRecovery() throws Exception
    {
        // given
        newTransaction();
        statement.addConstraintIndex( labelId, propertyKey );
        commit();

        // when
        restartDb();

        // then
        assertEquals( emptySetOf( IndexDescriptor.class ), asSet( readOnlyContext().getIndexes( labelId ) ) );
    }

    @Test
    public void shouldDisallowDroppingIndexThatDoesNotExist() throws Exception
    {
        // given
        newTransaction();
        IndexDescriptor index = statement.addIndex( labelId, propertyKey );
        commit();
        newTransaction();
        statement.dropIndex( index );
        commit();

        // when
        try
        {
            newTransaction();
            statement.dropIndex( index );
            commit();
        }
        // then
        catch ( DataIntegrityKernelException e )
        {
            assertEquals( String.format( "There is no index for property %d for label %d.", propertyKey, labelId ),
                          e.getMessage() );
        }
    }

    @Test
    public void shouldFailToCreateIndexWhereAConstraintIndexAlreadyExists() throws Exception
    {
        // given
        newTransaction();
        statement.addConstraintIndex( labelId, propertyKey );
        commit();

        // when
        try
        {
            newTransaction();
            statement.addIndex( labelId, propertyKey );
            commit();

            fail( "expected exception" );
        }
        // then
        catch ( DataIntegrityKernelException e )
        {
            assertEquals( String.format( "Property %d is already indexed for label %d through a constraint.",
                                         propertyKey, labelId ),
                          e.getMessage() );
        }
    }

    @Test
    public void shouldNotBeAbleToRemoveAConstraintIndexAsIfItWasARegularIndex() throws Exception
    {
        // given
        newTransaction();
        IndexDescriptor index = statement.addConstraintIndex( labelId, propertyKey );
        commit();

        // when
        try
        {
            newTransaction();
            statement.dropIndex( index );
            commit();

            fail( "expected exception" );
        }
        // then
        catch ( DataIntegrityKernelException e )
        {
            assertEquals( String.format( "There is no index for property %d for label %d.", propertyKey, labelId ),
                          e.getMessage() );
        }
    }

    @Test
    public void shouldListConstraintIndexesInTheBeansAPI() throws Exception
    {
        // given
        newTransaction();
        statement.addConstraintIndex( statement.getOrCreateLabelId( "Label1" ),
                                      statement.getOrCreatePropertyKeyId( "property1" ) );
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
        statement.addConstraintIndex( labelId, propertyKey );
        commit();

        // then/when
        newTransaction();
        assertFalse( statement.getIndexes().hasNext() );
        assertFalse( statement.getIndexes( labelId ).hasNext() );
    }

    @Test
    public void shouldNotListIndexesAmongConstraintIndexes() throws Exception
    {
        // given
        newTransaction();
        statement.addIndex( labelId, propertyKey );
        commit();

        // then/when
        newTransaction();
        assertFalse( statement.getConstraintIndexes().hasNext() );
        assertFalse( statement.getConstraintIndexes( labelId ).hasNext() );
    }

    private void awaitIndexOnline( IndexDescriptor indexRule ) throws IndexNotFoundKernelException
    {
        SchemaIndexTestHelper.awaitIndexOnline( readOnlyContext(), indexRule );
    }
}
