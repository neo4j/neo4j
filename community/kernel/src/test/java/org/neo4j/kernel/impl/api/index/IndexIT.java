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
import org.neo4j.graphdb.schema.IndexDefinition;
import org.neo4j.helpers.collection.Iterables;
import org.neo4j.helpers.collection.Iterators;
import org.neo4j.internal.kernel.api.IndexReference;
import org.neo4j.internal.kernel.api.SchemaRead;
import org.neo4j.internal.kernel.api.SchemaWrite;
import org.neo4j.internal.kernel.api.TokenWrite;
import org.neo4j.internal.kernel.api.Transaction;
import org.neo4j.internal.kernel.api.Write;
import org.neo4j.internal.kernel.api.exceptions.KernelException;
import org.neo4j.internal.kernel.api.exceptions.schema.SchemaKernelException;
import org.neo4j.internal.kernel.api.schema.LabelSchemaDescriptor;
import org.neo4j.kernel.api.index.PropertyAccessor;
import org.neo4j.kernel.api.schema.SchemaDescriptorFactory;
import org.neo4j.kernel.api.schema.constaints.IndexBackedConstraintDescriptor;
import org.neo4j.kernel.api.schema.index.SchemaIndexDescriptor;
import org.neo4j.kernel.impl.api.integrationtest.KernelIntegrationTest;
import org.neo4j.kernel.impl.api.state.ConstraintIndexCreator;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.logging.AssertableLogProvider;

import static java.util.Collections.emptySet;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.mock;
import static org.neo4j.helpers.collection.Iterators.asSet;
import static org.neo4j.internal.kernel.api.security.LoginContext.AUTH_DISABLED;
import static org.neo4j.kernel.impl.api.store.DefaultIndexReference.fromDescriptor;

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
        TokenWrite tokenWrites = tokenWriteInNewTransaction();
        labelId = tokenWrites.labelGetOrCreateForName( LABEL );
        propertyKeyId = tokenWrites.propertyKeyGetOrCreateForName( PROPERTY_KEY );
        int propertyKeyId2 = tokenWrites.propertyKeyGetOrCreateForName( PROPERTY_KEY2 );
        descriptor = SchemaDescriptorFactory.forLabel( labelId, propertyKeyId );
        descriptor2 = SchemaDescriptorFactory.forLabel( labelId, propertyKeyId2 );
        commit();
        executorService = Executors.newCachedThreadPool();
    }

    @After
    public void tearDown()
    {
        executorService.shutdown();
    }

    @Test
    public void createIndexForAnotherLabelWhileHoldingSharedLockOnOtherLabel() throws KernelException
    {
        TokenWrite tokenWrite = tokenWriteInNewTransaction();
        int label2 = tokenWrite.labelGetOrCreateForName( "Label2" );

        Write write = dataWriteInNewTransaction();
        long nodeId = write.nodeCreate();
        write.nodeAddLabel( nodeId, label2 );

        schemaWriteInNewTransaction().indexCreate( descriptor, null );
        commit();
    }

    @Test( timeout = 10_000 )
    public void createIndexesForDifferentLabelsConcurrently() throws Throwable
    {
        TokenWrite tokenWrite = tokenWriteInNewTransaction();
        int label2 = tokenWrite.labelGetOrCreateForName( "Label2" );

        LabelSchemaDescriptor anotherLabelDescriptor = SchemaDescriptorFactory.forLabel( label2, propertyKeyId );
        schemaWriteInNewTransaction().indexCreate( anotherLabelDescriptor, null );

        Future<?> indexFuture = executorService.submit( createIndex( db, Label.label( LABEL ), PROPERTY_KEY ) );
        indexFuture.get();
        commit();
    }

    @Test
    public void addIndexRuleInATransaction() throws Exception
    {
        // GIVEN
        SchemaWrite schemaWriteOperations = schemaWriteInNewTransaction();

        // WHEN
        IndexReference expectedRule = schemaWriteOperations.indexCreate( descriptor, null );
        commit();

        // THEN
        SchemaRead schemaRead = newTransaction().schemaRead();
        assertEquals( asSet( expectedRule ), asSet( schemaRead.indexesGetForLabel( labelId ) ) );
        assertEquals( expectedRule, schemaRead.index( descriptor.getLabelId(), descriptor.getPropertyIds() ) );
        commit();
    }

    @Test
    public void committedAndTransactionalIndexRulesShouldBeMerged() throws Exception
    {
        // GIVEN
        SchemaWrite schemaWriteOperations = schemaWriteInNewTransaction();
        IndexReference existingRule = schemaWriteOperations.indexCreate( descriptor, null );
        commit();

        // WHEN
        Transaction transaction = newTransaction( AUTH_DISABLED );
        IndexReference addedRule = transaction.schemaWrite()
                                                   .indexCreate( SchemaDescriptorFactory.forLabel( labelId, 10 ), null );
        Set<IndexReference> indexRulesInTx = asSet( transaction.schemaRead().indexesGetForLabel( labelId ) );
        commit();

        // THEN
        assertEquals( asSet( existingRule, addedRule ), indexRulesInTx );
    }

    @Test
    public void rollBackIndexRuleShouldNotBeCommitted() throws Exception
    {
        // GIVEN
        SchemaWrite schemaWrite = schemaWriteInNewTransaction();

        // WHEN
        schemaWrite.indexCreate( descriptor, null );
        // don't mark as success
        rollback();

        // THEN
        Transaction transaction = newTransaction();
        assertEquals( emptySet(), asSet( transaction.schemaRead().indexesGetForLabel( labelId ) ) );
        commit();
    }

    @Test
    public void shouldBeAbleToRemoveAConstraintIndexWithoutOwner() throws Exception
    {
        // given
        PropertyAccessor propertyAccessor = mock( PropertyAccessor.class );
        AssertableLogProvider logProvider = new AssertableLogProvider();
        ConstraintIndexCreator creator = new ConstraintIndexCreator( () -> kernel, indexingService, propertyAccessor, logProvider );

        SchemaIndexDescriptor constraintIndex = creator.createConstraintIndex( descriptor, null );
        // then
        Transaction transaction = newTransaction();
        assertEquals( emptySet(), asSet( transaction.schemaRead().constraintsGetForLabel( labelId ) ) );
        commit();

        // when
        SchemaWrite schemaWrite = schemaWriteInNewTransaction();
        schemaWrite.indexDrop( fromDescriptor( constraintIndex ) );
        commit();

        // then
        transaction = newTransaction();
        assertEquals( emptySet(), asSet( transaction.schemaRead().indexesGetForLabel( labelId ) ) );
        commit();
    }

    @Test
    public void shouldDisallowDroppingIndexThatDoesNotExist() throws Exception
    {
        // given
        IndexReference index;
        {
            SchemaWrite statement = schemaWriteInNewTransaction();
            index = statement.indexCreate( descriptor, null );
            commit();
        }
        {
            SchemaWrite statement = schemaWriteInNewTransaction();
            statement.indexDrop( index );
            commit();
        }

        // when
        try
        {
            SchemaWrite statement = schemaWriteInNewTransaction();
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
            SchemaWrite statement = schemaWriteInNewTransaction();
            statement.uniquePropertyConstraintCreate( descriptor );
            commit();
        }

        // when
        try
        {
            SchemaWrite statement = schemaWriteInNewTransaction();
            statement.indexCreate( descriptor, null );
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
        Transaction transaction = newTransaction( AUTH_DISABLED );
        transaction.schemaWrite().uniquePropertyConstraintCreate(
                SchemaDescriptorFactory.forLabel(
                        transaction.tokenWrite().labelGetOrCreateForName( "Label1" ),
                        transaction.tokenWrite().propertyKeyGetOrCreateForName( "property1" )
                ) );
        commit();

        // when
        try ( org.neo4j.graphdb.Transaction tx = db.beginTx() )
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
        SchemaWrite schemaWrite = schemaWriteInNewTransaction();
        IndexReference index1 = schemaWrite.indexCreate( descriptor, null );
        IndexReference index2 = fromDescriptor(
                ((IndexBackedConstraintDescriptor) schemaWrite.uniquePropertyConstraintCreate( descriptor2 )).ownedIndexDescriptor()) ;
        commit();

        // then/when
        SchemaRead schemaRead = newTransaction().schemaRead();
        List<IndexReference> indexes = Iterators.asList( schemaRead.indexesGetAll() );
        assertThat( indexes, containsInAnyOrder( index1, index2 ) );
        commit();
    }

    private Runnable createIndex( GraphDatabaseAPI db, Label label, String propertyKey )
    {
        return () ->
        {
            try ( org.neo4j.graphdb.Transaction transaction = db.beginTx() )
            {
                db.schema().indexFor( label ).on( propertyKey ).create();
                transaction.success();
            }

            try ( org.neo4j.graphdb.Transaction transaction = db.beginTx() )
            {
                db.schema().awaitIndexesOnline( 1, TimeUnit.MINUTES );
                transaction.success();
            }
        };
    }
}
