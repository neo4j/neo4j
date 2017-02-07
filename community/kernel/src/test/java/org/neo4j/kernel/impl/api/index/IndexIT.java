/*
 * Copyright (c) 2002-2017 "Neo Technology,"
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

import org.junit.Before;
import org.junit.Test;

import java.util.Set;

import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.schema.IndexDefinition;
import org.neo4j.helpers.collection.Iterables;
import org.neo4j.kernel.api.ReadOperations;
import org.neo4j.kernel.api.SchemaWriteOperations;
import org.neo4j.kernel.api.Statement;
import org.neo4j.kernel.api.TokenWriteOperations;
import org.neo4j.kernel.api.constraints.UniquenessConstraint;
import org.neo4j.kernel.api.exceptions.schema.SchemaKernelException;
import org.neo4j.kernel.api.schema.IndexDescriptor;
import org.neo4j.kernel.api.schema.NodePropertyDescriptor;
import org.neo4j.kernel.api.security.AnonymousContext;
import org.neo4j.kernel.api.security.SecurityContext;
import org.neo4j.kernel.api.schema_new.index.IndexBoundary;
import org.neo4j.kernel.api.schema_new.index.NewIndexDescriptor;
import org.neo4j.kernel.impl.api.integrationtest.KernelIntegrationTest;
import org.neo4j.kernel.impl.api.operations.SchemaReadOperations;
import org.neo4j.kernel.impl.api.state.ConstraintIndexCreator;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.neo4j.helpers.collection.Iterators.asSet;
import static org.neo4j.helpers.collection.Iterators.emptySetOf;

public class IndexIT extends KernelIntegrationTest
{
    private static final String LABEL = "Label";
    private static final String PROPERTY_KEY = "prop";

    private int labelId;
    private int propertyKeyId;
    private NodePropertyDescriptor descriptor;

    @Before
    public void createLabelAndProperty() throws Exception
    {
        TokenWriteOperations tokenWrites = tokenWriteOperationsInNewTransaction();
        labelId = tokenWrites.labelGetOrCreateForName( LABEL );
        propertyKeyId = tokenWrites.propertyKeyGetOrCreateForName( PROPERTY_KEY );
        descriptor = new NodePropertyDescriptor( labelId, propertyKeyId );
        commit();
    }

    @Test
    public void addIndexRuleInATransaction() throws Exception
    {
        // GIVEN
        SchemaWriteOperations schemaWriteOperations = schemaWriteOperationsInNewTransaction();

        // WHEN
        NewIndexDescriptor expectedRule = schemaWriteOperations.indexCreate( descriptor );
        commit();

        // THEN
        ReadOperations readOperations = readOperationsInNewTransaction();
        assertEquals( asSet( expectedRule ), asSet( readOperations.indexesGetForLabel( labelId ) ) );
        assertEquals( expectedRule, readOperations.indexGetForLabelAndPropertyKey( descriptor ) );
        commit();
    }

    @Test
    public void committedAndTransactionalIndexRulesShouldBeMerged() throws Exception
    {
        // GIVEN
        SchemaWriteOperations schemaWriteOperations = schemaWriteOperationsInNewTransaction();
        NewIndexDescriptor existingRule = schemaWriteOperations.indexCreate( descriptor );
        commit();

        // WHEN
        Statement statement = statementInNewTransaction( AnonymousContext.AUTH_DISABLED );
        NewIndexDescriptor addedRule = statement.schemaWriteOperations()
                                            .indexCreate( new NodePropertyDescriptor( labelId, 10 ) );
        Set<NewIndexDescriptor> indexRulesInTx = asSet( statement.readOperations().indexesGetForLabel( labelId ) );
        commit();

        // THEN
        assertEquals( asSet( existingRule, addedRule ), indexRulesInTx );
    }

    @Test
    public void rollBackIndexRuleShouldNotBeCommitted() throws Exception
    {
        // GIVEN
        SchemaWriteOperations schemaWriteOperations = schemaWriteOperationsInNewTransaction();

        // WHEN
        schemaWriteOperations.indexCreate( descriptor );
        // don't mark as success
        rollback();

        // THEN
        ReadOperations readOperations = readOperationsInNewTransaction();
        assertEquals( emptySetOf( IndexDescriptor.class ), asSet( readOperations.indexesGetForLabel( labelId ) ) );
        commit();
    }

    @Test
    public void shouldRemoveAConstraintIndexWithoutOwnerInRecovery() throws Exception
    {
        // given
        ConstraintIndexCreator creator = new ConstraintIndexCreator( () -> kernel, indexingService );
        creator.createConstraintIndex( new UniquenessConstraint( descriptor ) );

        // when
        restartDb();

        // then
        ReadOperations readOperations = readOperationsInNewTransaction();
        assertEquals( emptySetOf( IndexDescriptor.class ), asSet( readOperations.indexesGetForLabel( labelId ) ) );
        commit();
    }

    @Test
    public void shouldDisallowDroppingIndexThatDoesNotExist() throws Exception
    {
        // given
        NewIndexDescriptor index;
        {
            SchemaWriteOperations statement = schemaWriteOperationsInNewTransaction();
            index = statement.indexCreate( descriptor );
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
            assertEquals( "Unable to drop index on :label[" + labelId + "](property[" + propertyKeyId + "]): " +
                          "No such INDEX ON :label[" + labelId + "](property[" + propertyKeyId + "]).", e.getMessage() );
        }
    }

    @Test
    public void shouldFailToCreateIndexWhereAConstraintAlreadyExists() throws Exception
    {
        // given
        {
            SchemaWriteOperations statement = schemaWriteOperationsInNewTransaction();
            statement.uniquePropertyConstraintCreate( descriptor );
            commit();
        }

        // when
        try
        {
            SchemaWriteOperations statement = schemaWriteOperationsInNewTransaction();
            statement.indexCreate( descriptor );
            commit();

            fail( "expected exception" );
        }
        // then
        catch ( SchemaKernelException e )
        {
            assertEquals( "Label '" + LABEL + "' and property '" + PROPERTY_KEY + "' have a unique constraint defined" +
                          " on them, so an index is already created that matches this.", e.getMessage() );
        }
    }

    @Test
    public void shouldListConstraintIndexesInTheBeansAPI() throws Exception
    {
        // given
        Statement statement = statementInNewTransaction( SecurityContext.AUTH_DISABLED );
        statement.schemaWriteOperations().uniquePropertyConstraintCreate(
                new NodePropertyDescriptor( statement.tokenWriteOperations().labelGetOrCreateForName( "Label1" ),
                        statement.tokenWriteOperations().propertyKeyGetOrCreateForName( "property1" ) ) );
        commit();

        // when
        try ( Transaction tx = db.beginTx() )
        {
            Set<IndexDefinition> indexes;
            IndexDefinition index;
            indexes = Iterables.asSet( db.schema().getIndexes() );

            // then
            assertEquals( 1, indexes.size() );
            index = indexes.iterator().next();
            assertEquals( "Label1", index.getLabel().name() );
            assertEquals( asSet( "property1" ), Iterables.asSet( index.getPropertyKeys() ) );
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
        SchemaWriteOperations schemaWriteOperations = schemaWriteOperationsInNewTransaction();
        schemaWriteOperations.uniquePropertyConstraintCreate( descriptor );
        commit();

        // then/when
        ReadOperations readOperations = readOperationsInNewTransaction();
        assertFalse( readOperations.indexesGetAll().hasNext() );
        assertFalse( readOperations.indexesGetForLabel( labelId ).hasNext() );
    }

    @Test
    public void shouldNotListIndexesAmongConstraintIndexes() throws Exception
    {
        // given
        SchemaWriteOperations schemaWriteOperations = schemaWriteOperationsInNewTransaction();
        schemaWriteOperations.indexCreate( descriptor );
        commit();

        // then/when
        ReadOperations readOperations = readOperationsInNewTransaction();
        assertFalse( readOperations.uniqueIndexesGetAll().hasNext() );
        assertFalse( readOperations.uniqueIndexesGetForLabel( labelId ).hasNext() );
    }
}
