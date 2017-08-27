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

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.List;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.schema.IndexDefinition;
import org.neo4j.helpers.collection.Iterables;
import org.neo4j.helpers.collection.Iterators;
import org.neo4j.kernel.api.DataWriteOperations;
import org.neo4j.kernel.api.ReadOperations;
import org.neo4j.kernel.api.SchemaWriteOperations;
import org.neo4j.kernel.api.Statement;
import org.neo4j.kernel.api.TokenWriteOperations;
import org.neo4j.kernel.api.exceptions.KernelException;
import org.neo4j.kernel.api.exceptions.schema.SchemaKernelException;
import org.neo4j.kernel.api.index.PropertyAccessor;
import org.neo4j.kernel.api.schema.LabelSchemaDescriptor;
import org.neo4j.kernel.api.schema.SchemaDescriptorFactory;
import org.neo4j.kernel.api.schema.index.IndexDescriptor;
import org.neo4j.kernel.api.security.AnonymousContext;
import org.neo4j.kernel.api.security.SecurityContext;
import org.neo4j.kernel.impl.api.integrationtest.KernelIntegrationTest;
import org.neo4j.kernel.impl.api.state.ConstraintIndexCreator;
import org.neo4j.kernel.internal.GraphDatabaseAPI;

import static java.util.Collections.emptySet;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.mock;
import static org.neo4j.helpers.collection.Iterators.asSet;

public class IndexIT extends KernelIntegrationTest
{
    private static final String LABEL = "Label";
    private static final String PROPERTY_KEY = "prop";
    private static final String PROPERTY_KEY2 = "prop2";

    private int labelId;
    private int propertyKeyId;
    private LabelSchemaDescriptor descriptor;
    private LabelSchemaDescriptor descriptor2;
    private ExecutorService executorService;

    @Before
    public void createLabelAndProperty() throws Exception
    {
        TokenWriteOperations tokenWrites = tokenWriteOperationsInNewTransaction();
        labelId = tokenWrites.labelGetOrCreateForName( LABEL );
        propertyKeyId = tokenWrites.propertyKeyGetOrCreateForName( PROPERTY_KEY );
        int propertyKeyId2 = tokenWrites.propertyKeyGetOrCreateForName( PROPERTY_KEY2 );
        descriptor = SchemaDescriptorFactory.forLabel( labelId, propertyKeyId );
        descriptor2 = SchemaDescriptorFactory.forLabel( labelId, propertyKeyId2 );
        commit();
        executorService = Executors.newCachedThreadPool();
    }

    @After
    public void tearDown() throws Exception
    {
        executorService.shutdown();
    }

    @Test
    public void createIndexForAnotherLabelWhileHoldingSharedLockOnOtherLabel() throws KernelException
    {
        TokenWriteOperations tokenWriteOperations = tokenWriteOperationsInNewTransaction();
        int label2 = tokenWriteOperations.labelGetOrCreateForName( "Label2" );

        DataWriteOperations dataWriteOperations = dataWriteOperationsInNewTransaction();
        long nodeId = dataWriteOperations.nodeCreate();
        dataWriteOperations.nodeAddLabel( nodeId, label2 );

        schemaWriteOperationsInNewTransaction().indexCreate( descriptor );
        commit();
    }

    @Test( timeout = 10_000 )
    public void createIndexesForDifferentLabelsConcurrently() throws Throwable
    {
        TokenWriteOperations tokenWriteOperations = tokenWriteOperationsInNewTransaction();
        int label2 = tokenWriteOperations.labelGetOrCreateForName( "Label2" );

        LabelSchemaDescriptor anotherLabelDescriptor = SchemaDescriptorFactory.forLabel( label2, propertyKeyId );
        schemaWriteOperationsInNewTransaction().indexCreate( anotherLabelDescriptor );

        Future<?> indexFuture = executorService.submit( createIndex( db, Label.label( LABEL ), PROPERTY_KEY ) );
        indexFuture.get();
        commit();
    }

    @Test
    public void addIndexRuleInATransaction() throws Exception
    {
        // GIVEN
        SchemaWriteOperations schemaWriteOperations = schemaWriteOperationsInNewTransaction();

        // WHEN
        IndexDescriptor expectedRule = schemaWriteOperations.indexCreate( descriptor );
        commit();

        // THEN
        ReadOperations readOperations = readOperationsInNewTransaction();
        assertEquals( asSet( expectedRule ), asSet( readOperations.indexesGetForLabel( labelId ) ) );
        assertEquals( expectedRule, readOperations.indexGetForSchema( descriptor ) );
        commit();
    }

    @Test
    public void committedAndTransactionalIndexRulesShouldBeMerged() throws Exception
    {
        // GIVEN
        SchemaWriteOperations schemaWriteOperations = schemaWriteOperationsInNewTransaction();
        IndexDescriptor existingRule = schemaWriteOperations.indexCreate( descriptor );
        commit();

        // WHEN
        Statement statement = statementInNewTransaction( AnonymousContext.AUTH_DISABLED );
        IndexDescriptor addedRule = statement.schemaWriteOperations()
                                            .indexCreate( SchemaDescriptorFactory.forLabel( labelId, 10 ) );
        Set<IndexDescriptor> indexRulesInTx = asSet( statement.readOperations().indexesGetForLabel( labelId ) );
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
        assertEquals( emptySet(), asSet( readOperations.indexesGetForLabel( labelId ) ) );
        commit();
    }

    @Test
    public void shouldBeAbleToRemoveAConstraintIndexWithoutOwner() throws Exception
    {
        // given
        PropertyAccessor propertyAccessor = mock( PropertyAccessor.class );
        ConstraintIndexCreator creator = new ConstraintIndexCreator( () -> kernel, indexingService, propertyAccessor );

        IndexDescriptor constraintIndex = creator.createConstraintIndex( descriptor );
        // then
        ReadOperations readOperations = readOperationsInNewTransaction();
        assertEquals( emptySet(), asSet( readOperations.constraintsGetForLabel( labelId ) ) );
        commit();

        // when
        SchemaWriteOperations schemaWriteOperations = schemaWriteOperationsInNewTransaction();
        schemaWriteOperations.indexDrop( constraintIndex );
        commit();

        // then
        readOperations = readOperationsInNewTransaction();
        assertEquals( emptySet(), asSet( readOperations.indexesGetForLabel( labelId ) ) );
        commit();
    }

    @Test
    public void shouldDisallowDroppingIndexThatDoesNotExist() throws Exception
    {
        // given
        IndexDescriptor index;
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
        commit();
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
        commit();
    }

    @Test
    public void shouldListConstraintIndexesInTheBeansAPI() throws Exception
    {
        // given
        Statement statement = statementInNewTransaction( SecurityContext.AUTH_DISABLED );
        statement.schemaWriteOperations().uniquePropertyConstraintCreate(
                SchemaDescriptorFactory.forLabel(
                        statement.tokenWriteOperations().labelGetOrCreateForName( "Label1" ),
                        statement.tokenWriteOperations().propertyKeyGetOrCreateForName( "property1" )
                ) );
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
    public void shouldListAll() throws Exception
    {
        // given
        SchemaWriteOperations schemaWriteOperations = schemaWriteOperationsInNewTransaction();
        IndexDescriptor index1 = schemaWriteOperations.indexCreate( descriptor );
        IndexDescriptor index2 = schemaWriteOperations.uniquePropertyConstraintCreate( descriptor2 )
                                                            .ownedIndexDescriptor();
        commit();

        // then/when
        ReadOperations readOperations = readOperationsInNewTransaction();
        List<IndexDescriptor> indexes = Iterators.asList( readOperations.indexesGetAll() );
        assertThat( indexes, containsInAnyOrder( index1, index2 ) );
        commit();
    }

    private Runnable createIndex( GraphDatabaseAPI db, Label label, String propertyKey )
    {
        return () ->
        {
            try ( Transaction transaction = db.beginTx() )
            {
                db.schema().indexFor( label ).on( propertyKey ).create();
                transaction.success();
            }

            try ( Transaction transaction = db.beginTx() )
            {
                db.schema().awaitIndexesOnline( 1, TimeUnit.MINUTES );
                transaction.success();
            }
        };
    }
}
