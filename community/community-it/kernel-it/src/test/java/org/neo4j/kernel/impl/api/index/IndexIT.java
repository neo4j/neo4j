/*
 * Copyright (c) 2002-2020 "Neo4j,"
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
import org.neo4j.internal.kernel.api.schema.SchemaDescriptor;
import org.neo4j.storageengine.api.NodePropertyAccessor;
import org.neo4j.kernel.api.schema.MultiTokenSchemaDescriptor;
import org.neo4j.kernel.api.schema.SchemaDescriptorFactory;
import org.neo4j.kernel.api.schema.constraints.IndexBackedConstraintDescriptor;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.impl.api.integrationtest.KernelIntegrationTest;
import org.neo4j.kernel.impl.api.state.ConstraintIndexCreator;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.logging.AssertableLogProvider;
import org.neo4j.storageengine.api.EntityType;
import org.neo4j.storageengine.api.schema.IndexDescriptor;

import static java.util.Collections.emptySet;
import static java.util.Collections.singletonList;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.mock;
import static org.neo4j.graphdb.Label.label;
import static org.neo4j.graphdb.RelationshipType.withName;
import static org.neo4j.graphdb.factory.GraphDatabaseSettings.default_schema_provider;
import static org.neo4j.helpers.collection.Iterables.single;
import static org.neo4j.helpers.collection.Iterators.asSet;
import static org.neo4j.internal.kernel.api.security.LoginContext.AUTH_DISABLED;

public class IndexIT extends KernelIntegrationTest
{
    private static final String LABEL = "Label";
    private static final String LABEL2 = "Label2";
    private static final String REL_TYPE = "RelType";
    private static final String REL_TYPE2 = "RelType2";
    private static final String PROPERTY_KEY = "prop";
    private static final String PROPERTY_KEY2 = "prop2";

    private int labelId;
    private int labelId2;
    private int relType;
    private int relType2;
    private int propertyKeyId;
    private int propertyKeyId2;
    private LabelSchemaDescriptor descriptor;
    private LabelSchemaDescriptor descriptor2;
    private ExecutorService executorService;

    @Before
    public void createLabelAndProperty() throws Exception
    {
        TokenWrite tokenWrites = tokenWriteInNewTransaction();
        labelId = tokenWrites.labelGetOrCreateForName( LABEL );
        labelId2 = tokenWrites.labelGetOrCreateForName( LABEL2 );
        relType = tokenWrites.relationshipTypeGetOrCreateForName( REL_TYPE );
        relType2 = tokenWrites.relationshipTypeGetOrCreateForName( REL_TYPE2 );
        propertyKeyId = tokenWrites.propertyKeyGetOrCreateForName( PROPERTY_KEY );
        propertyKeyId2 = tokenWrites.propertyKeyGetOrCreateForName( PROPERTY_KEY2 );
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

        schemaWriteInNewTransaction().indexCreate( descriptor );
        commit();
    }

    @Test( timeout = 10_000 )
    public void createIndexesForDifferentLabelsConcurrently() throws Throwable
    {
        TokenWrite tokenWrite = tokenWriteInNewTransaction();
        int label2 = tokenWrite.labelGetOrCreateForName( "Label2" );

        LabelSchemaDescriptor anotherLabelDescriptor = SchemaDescriptorFactory.forLabel( label2, propertyKeyId );
        schemaWriteInNewTransaction().indexCreate( anotherLabelDescriptor );

        Future<?> indexFuture = executorService.submit( createIndex( db, label( LABEL ), PROPERTY_KEY ) );
        indexFuture.get();
        commit();
    }

    @Test
    public void addIndexRuleInATransaction() throws Exception
    {
        // GIVEN
        SchemaWrite schemaWriteOperations = schemaWriteInNewTransaction();

        // WHEN
        IndexReference expectedRule = schemaWriteOperations.indexCreate( descriptor );
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
        IndexReference existingRule = schemaWriteOperations.indexCreate( descriptor );
        commit();

        // WHEN
        Transaction transaction = newTransaction( AUTH_DISABLED );
        IndexReference addedRule = transaction.schemaWrite()
                                                   .indexCreate( SchemaDescriptorFactory.forLabel( labelId, 10 ) );
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
        schemaWrite.indexCreate( descriptor );
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
        NodePropertyAccessor propertyAccessor = mock( NodePropertyAccessor.class );
        AssertableLogProvider logProvider = new AssertableLogProvider();
        ConstraintIndexCreator creator = new ConstraintIndexCreator( () -> kernel, indexingService, propertyAccessor, logProvider );

        String defaultProvider = Config.defaults().get( default_schema_provider );
        IndexDescriptor constraintIndex = creator.createConstraintIndex( descriptor, defaultProvider );
        // then
        Transaction transaction = newTransaction();
        assertEquals( emptySet(), asSet( transaction.schemaRead().constraintsGetForLabel( labelId ) ) );
        commit();

        // when
        SchemaWrite schemaWrite = schemaWriteInNewTransaction();
        schemaWrite.indexDrop( constraintIndex );
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
            index = statement.indexCreate( descriptor );
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
            statement.indexCreate( descriptor );
            commit();

            fail( "expected exception" );
        }
        // then
        catch ( SchemaKernelException e )
        {
            assertEquals( "There is a uniqueness constraint on :" + LABEL + "(" + PROPERTY_KEY + "), so an index is " +
                          "already created that matches this.", e.getMessage() );
        }
        commit();
    }

    @Test
    public void shouldListConstraintIndexesInTheCoreAPI() throws Exception
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
        try ( org.neo4j.graphdb.Transaction ignore = db.beginTx() )
        {
            Set<IndexDefinition> indexes = Iterables.asSet( db.schema().getIndexes() );

            // then
            assertEquals( 1, indexes.size() );
            IndexDefinition index = indexes.iterator().next();
            assertEquals( "Label1", single( index.getLabels() ).name() );
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
    public void shouldListMultiTokenIndexesInTheCoreAPI() throws Exception
    {
        Transaction transaction = newTransaction( AUTH_DISABLED );
        MultiTokenSchemaDescriptor descriptor = SchemaDescriptorFactory.multiToken(
                new int[]{labelId, labelId2}, EntityType.NODE, propertyKeyId );
        transaction.schemaWrite().indexCreate( descriptor );
        commit();

        try ( @SuppressWarnings( "unused" ) org.neo4j.graphdb.Transaction tx = db.beginTx() )
        {
            Set<IndexDefinition> indexes = Iterables.asSet( db.schema().getIndexes() );

            // then
            assertEquals( 1, indexes.size() );
            IndexDefinition index = indexes.iterator().next();
            try
            {
                index.getLabel();
                fail( "index.getLabel() should have thrown. ");
            }
            catch ( IllegalStateException ignore )
            {
            }
            try
            {
                index.getRelationshipType();
                fail( "index.getRelationshipType() should have thrown. ");
            }
            catch ( IllegalStateException ignore )
            {
            }
            try
            {
                index.getRelationshipTypes();
                fail( "index.getRelationshipTypes() should have thrown. ");
            }
            catch ( IllegalStateException ignore )
            {
            }
            assertThat( index.getLabels(), containsInAnyOrder( label( LABEL ), label( LABEL2 ) ) );
            assertFalse( "should not be a constraint index", index.isConstraintIndex() );
            assertTrue( "should be a multi-token index", index.isMultiTokenIndex() );
            assertFalse( "should not be a composite index", index.isCompositeIndex() );
            assertTrue( "should be a node index", index.isNodeIndex() );
            assertFalse( "should not be a relationship index", index.isRelationshipIndex() );
            assertEquals( asSet( PROPERTY_KEY ), Iterables.asSet( index.getPropertyKeys() ) );
        }
    }

    @Test
    public void shouldListCompositeIndexesInTheCoreAPI() throws Exception
    {
        Transaction transaction = newTransaction( AUTH_DISABLED );
        SchemaDescriptor descriptor = SchemaDescriptorFactory.forLabel( labelId, propertyKeyId, propertyKeyId2 );
        transaction.schemaWrite().indexCreate( descriptor );
        commit();

        try ( @SuppressWarnings( "unused" ) org.neo4j.graphdb.Transaction tx = db.beginTx() )
        {
            Set<IndexDefinition> indexes = Iterables.asSet( db.schema().getIndexes() );

            // then
            assertEquals( 1, indexes.size() );
            IndexDefinition index = indexes.iterator().next();
            assertEquals( LABEL, single( index.getLabels() ).name() );
            assertThat( index.getLabels(), containsInAnyOrder( label( LABEL ) ) );
            try
            {
                index.getRelationshipType();
                fail( "index.getRelationshipType() should have thrown. ");
            }
            catch ( IllegalStateException ignore )
            {
            }
            try
            {
                index.getRelationshipTypes();
                fail( "index.getRelationshipTypes() should have thrown. ");
            }
            catch ( IllegalStateException ignore )
            {
            }
            assertFalse( "should not be a constraint index", index.isConstraintIndex() );
            assertFalse( "should not be a multi-token index", index.isMultiTokenIndex() );
            assertTrue( "should be a composite index", index.isCompositeIndex() );
            assertTrue( "should be a node index", index.isNodeIndex() );
            assertFalse( "should not be a relationship index", index.isRelationshipIndex() );
            assertEquals( asSet( PROPERTY_KEY, PROPERTY_KEY2 ), Iterables.asSet( index.getPropertyKeys() ) );
        }
    }

    @Test
    public void shouldListRelationshipIndexesInTheCoreAPI() throws Exception
    {
        Transaction transaction = newTransaction( AUTH_DISABLED );
        SchemaDescriptor descriptor = SchemaDescriptorFactory.forRelType( relType, propertyKeyId );
        transaction.schemaWrite().indexCreate( descriptor );
        commit();

        try ( org.neo4j.graphdb.Transaction tx = db.beginTx() )
        {
            Set<IndexDefinition> indexes = Iterables.asSet( db.schema().getIndexes() );

            // then
            assertEquals( 1, indexes.size() );
            IndexDefinition index = indexes.iterator().next();
            try
            {
                index.getLabel();
                fail( "index.getLabel() should have thrown. ");
            }
            catch ( IllegalStateException ignore )
            {
            }
            try
            {
                index.getLabels();
                fail( "index.getLabels() should have thrown. ");
            }
            catch ( IllegalStateException ignore )
            {
            }
            assertEquals( REL_TYPE, index.getRelationshipType().name() );
            assertEquals( singletonList( withName( REL_TYPE ) ), index.getRelationshipTypes() );
            assertFalse( "should not be a constraint index", index.isConstraintIndex() );
            assertFalse( "should not be a multi-token index", index.isMultiTokenIndex() );
            assertFalse( "should not be a composite index", index.isCompositeIndex() );
            assertFalse( "should not be a node index", index.isNodeIndex() );
            assertTrue( "should be a relationship index", index.isRelationshipIndex() );
            assertEquals( asSet( PROPERTY_KEY ), Iterables.asSet( index.getPropertyKeys() ) );
        }
    }

    @Test
    public void shouldListCompositeMultiTokenRelationshipIndexesInTheCoreAPI() throws Exception
    {
        Transaction transaction = newTransaction( AUTH_DISABLED );
        SchemaDescriptor descriptor = SchemaDescriptorFactory.multiToken(
                new int[]{relType, relType2}, EntityType.RELATIONSHIP, propertyKeyId, propertyKeyId2 );
        transaction.schemaWrite().indexCreate( descriptor );
        commit();

        try ( org.neo4j.graphdb.Transaction tx = db.beginTx() )
        {
            Set<IndexDefinition> indexes = Iterables.asSet( db.schema().getIndexes() );

            // then
            assertEquals( 1, indexes.size() );
            IndexDefinition index = indexes.iterator().next();
            try
            {
                index.getLabel();
                fail( "index.getLabel() should have thrown. ");
            }
            catch ( IllegalStateException ignore )
            {
            }
            try
            {
                index.getLabels();
                fail( "index.getLabels() should have thrown. ");
            }
            catch ( IllegalStateException ignore )
            {
            }
            try
            {
                index.getRelationshipType();
                fail( "index.getRelationshipType() should have thrown. ");
            }
            catch ( IllegalStateException ignore )
            {
            }
            assertThat( index.getRelationshipTypes(), containsInAnyOrder( withName( REL_TYPE ), withName( REL_TYPE2 ) ) );
            assertFalse( "should not be a constraint index", index.isConstraintIndex() );
            assertTrue( "should be a multi-token index", index.isMultiTokenIndex() );
            assertTrue( "should be a composite index", index.isCompositeIndex() );
            assertFalse( "should not be a node index", index.isNodeIndex() );
            assertTrue( "should be a relationship index", index.isRelationshipIndex() );
            assertEquals( asSet( PROPERTY_KEY, PROPERTY_KEY2 ), Iterables.asSet( index.getPropertyKeys() ) );
        }
    }

    @Test
    public void shouldListAll() throws Exception
    {
        // given
        SchemaWrite schemaWrite = schemaWriteInNewTransaction();
        IndexReference index1 = schemaWrite.indexCreate( descriptor );
        IndexReference index2 =
                ((IndexBackedConstraintDescriptor) schemaWrite.uniquePropertyConstraintCreate( descriptor2 ))
                        .ownedIndexDescriptor();
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
