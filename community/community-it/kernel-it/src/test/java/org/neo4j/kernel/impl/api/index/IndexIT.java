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

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.util.List;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import org.neo4j.common.EntityType;
import org.neo4j.exceptions.KernelException;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.schema.IndexDefinition;
import org.neo4j.internal.helpers.collection.Iterables;
import org.neo4j.internal.helpers.collection.Iterators;
import org.neo4j.internal.kernel.api.SchemaRead;
import org.neo4j.internal.kernel.api.SchemaWrite;
import org.neo4j.internal.kernel.api.TokenWrite;
import org.neo4j.internal.kernel.api.Write;
import org.neo4j.internal.kernel.api.exceptions.schema.SchemaKernelException;
import org.neo4j.internal.schema.ConstraintDescriptor;
import org.neo4j.internal.schema.IndexDescriptor;
import org.neo4j.internal.schema.IndexPrototype;
import org.neo4j.internal.schema.IndexProviderDescriptor;
import org.neo4j.internal.schema.IndexType;
import org.neo4j.internal.schema.LabelSchemaDescriptor;
import org.neo4j.internal.schema.SchemaDescriptor;
import org.neo4j.internal.schema.constraints.IndexBackedConstraintDescriptor;
import org.neo4j.kernel.api.KernelTransaction;
import org.neo4j.kernel.impl.api.integrationtest.KernelIntegrationTest;
import org.neo4j.kernel.impl.api.state.ConstraintIndexCreator;
import org.neo4j.kernel.impl.index.schema.FulltextIndexProviderFactory;
import org.neo4j.kernel.impl.index.schema.GenericNativeIndexProvider;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.logging.AssertableLogProvider;

import static java.util.Collections.emptySet;
import static java.util.Collections.singletonList;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.neo4j.common.TokenNameLookup.idTokenNameLookup;
import static org.neo4j.graphdb.Label.label;
import static org.neo4j.graphdb.RelationshipType.withName;
import static org.neo4j.internal.helpers.collection.Iterables.single;
import static org.neo4j.internal.helpers.collection.Iterators.asSet;
import static org.neo4j.internal.kernel.api.security.LoginContext.AUTH_DISABLED;
import static org.neo4j.internal.schema.IndexPrototype.uniqueForSchema;

class IndexIT extends KernelIntegrationTest
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
    private LabelSchemaDescriptor schema;
    private LabelSchemaDescriptor schema2;
    private ExecutorService executorService;

    @BeforeEach
    void createLabelAndProperty() throws Exception
    {
        TokenWrite tokenWrites = tokenWriteInNewTransaction();
        labelId = tokenWrites.labelGetOrCreateForName( LABEL );
        labelId2 = tokenWrites.labelGetOrCreateForName( LABEL2 );
        relType = tokenWrites.relationshipTypeGetOrCreateForName( REL_TYPE );
        relType2 = tokenWrites.relationshipTypeGetOrCreateForName( REL_TYPE2 );
        propertyKeyId = tokenWrites.propertyKeyGetOrCreateForName( PROPERTY_KEY );
        propertyKeyId2 = tokenWrites.propertyKeyGetOrCreateForName( PROPERTY_KEY2 );
        schema = SchemaDescriptor.forLabel( labelId, propertyKeyId );
        schema2 = SchemaDescriptor.forLabel( labelId, propertyKeyId2 );
        commit();
        executorService = Executors.newCachedThreadPool();
    }

    @AfterEach
    void tearDown()
    {
        executorService.shutdown();
    }

    @Test
    void createIndexForAnotherLabelWhileHoldingSharedLockOnOtherLabel() throws KernelException, ExecutionException, InterruptedException
    {
        TokenWrite tokenWrite = tokenWriteInNewTransaction();
        int label2 = tokenWrite.labelGetOrCreateForName( "Label2" );
        commit();

        Write write = dataWriteInNewTransaction();
        long nodeId = write.nodeCreate();
        write.nodeAddLabel( nodeId, label2 );
        try ( var ignored = captureTransaction() )
        {
            executorService.submit( () -> {
                try
                {
                    schemaWriteInNewTransaction().indexCreate( schema, null );
                    commit();
                }
                catch ( Exception e )
                {
                    throw new RuntimeException( e );
                }
            } ).get();
        }
    }

    @Test
    @Timeout( 10 )
    void createIndexesForDifferentLabelsConcurrently() throws Throwable
    {
        TokenWrite tokenWrite = tokenWriteInNewTransaction();
        int label2 = tokenWrite.labelGetOrCreateForName( "Label2" );
        commit();

        LabelSchemaDescriptor anotherLabelDescriptor = SchemaDescriptor.forLabel( label2, propertyKeyId );
        schemaWriteInNewTransaction().indexCreate( anotherLabelDescriptor, "my index" );

        Future<?> indexFuture = executorService.submit( createIndex( db, label( LABEL ), PROPERTY_KEY ) );
        indexFuture.get();
        commit();
    }

    @Test
    void addIndexRuleInATransaction() throws Exception
    {
        // GIVEN
        SchemaWrite schemaWriteOperations = schemaWriteInNewTransaction();

        // WHEN
        IndexDescriptor expectedRule = schemaWriteOperations.indexCreate( schema, "my index" );
        commit();

        // THEN
        SchemaRead schemaRead = newTransaction().schemaRead();
        assertEquals( asSet( expectedRule ), asSet( schemaRead.indexesGetForLabel( labelId ) ) );
        assertEquals( expectedRule, Iterators.single( schemaRead.index( schema ) ) );
        commit();
    }

    @Test
    void committedAndTransactionalIndexRulesShouldBeMerged() throws Exception
    {
        // GIVEN
        SchemaWrite schemaWriteOperations = schemaWriteInNewTransaction();
        IndexDescriptor existingRule = schemaWriteOperations.indexCreate( schema, "my index" );
        commit();

        // WHEN
        KernelTransaction transaction = newTransaction( AUTH_DISABLED );
        LabelSchemaDescriptor schema = SchemaDescriptor.forLabel( labelId, propertyKeyId2 );
        IndexDescriptor addedRule = transaction.schemaWrite().indexCreate( schema, "my other index" );
        Set<IndexDescriptor> indexRulesInTx = asSet( transaction.schemaRead().indexesGetForLabel( labelId ) );
        commit();

        // THEN
        assertEquals( asSet( existingRule, addedRule ), indexRulesInTx );
    }

    @Test
    void rollBackIndexRuleShouldNotBeCommitted() throws Exception
    {
        // GIVEN
        SchemaWrite schemaWrite = schemaWriteInNewTransaction();

        // WHEN
        schemaWrite.indexCreate( schema, "my index" );
        // don't mark as success
        rollback();

        // THEN
        KernelTransaction transaction = newTransaction();
        assertEquals( emptySet(), asSet( transaction.schemaRead().indexesGetForLabel( labelId ) ) );
        commit();
    }

    @Test
    void shouldBeAbleToRemoveAConstraintIndexWithoutOwner() throws Exception
    {
        // given
        AssertableLogProvider logProvider = new AssertableLogProvider();
        ConstraintIndexCreator creator = new ConstraintIndexCreator( () -> kernel, indexingService, logProvider );

        IndexProviderDescriptor provider = GenericNativeIndexProvider.DESCRIPTOR;
        IndexPrototype prototype = uniqueForSchema( schema ).withName( "constraint name" ).withIndexProvider( provider );
        IndexDescriptor constraintIndex = creator.createConstraintIndex( prototype );
        // then
        KernelTransaction transaction = newTransaction();
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
    void shouldDisallowDroppingIndexThatDoesNotExist() throws Exception
    {
        // given
        IndexDescriptor index;
        {
            SchemaWrite statement = schemaWriteInNewTransaction();
            index = statement.indexCreate( schema, "my index" );
            commit();
        }
        {
            SchemaWrite statement = schemaWriteInNewTransaction();
            statement.indexDrop( index );
            commit();
        }

        var e = assertThrows( SchemaKernelException.class, () ->
        {
            SchemaWrite statement = schemaWriteInNewTransaction();
            statement.indexDrop( index );
        } );
        assertEquals( "Unable to drop index: Index does not exist: Index( 1, 'my index', GENERAL BTREE, :Label(prop), native-btree-1.0 )",
                e.getUserMessage( idTokenNameLookup ) );
        commit();
    }

    @Test
    void shouldDisallowDroppingIndexBySchemaThatDoesNotExist() throws Exception
    {
        // given
        IndexDescriptor index;
        {
            SchemaWrite statement = schemaWriteInNewTransaction();
            index = statement.indexCreate( schema, "my index" );
            commit();
        }
        {
            SchemaWrite statement = schemaWriteInNewTransaction();
            statement.indexDrop( index.schema() );
            commit();
        }

        // when
        SchemaWrite statement = schemaWriteInNewTransaction();
        SchemaKernelException e = assertThrows( SchemaKernelException.class, () -> statement.indexDrop( index.schema() ) );
        assertEquals( "Unable to drop index on :" + LABEL + "(" + PROPERTY_KEY + "). There is no such index.", e.getMessage() );
        commit();
    }

    @Test
    void shouldDisallowDroppingIndexByNameThatDoesNotExist() throws KernelException
    {
        // given
        String indexName = "My fancy index";
        IndexDescriptor index;
        {
            SchemaWrite statement = schemaWriteInNewTransaction();
            index = statement.indexCreate( schema, indexName );
            commit();
        }
        {
            SchemaWrite statement = schemaWriteInNewTransaction();
            statement.indexDrop( index );
            commit();
        }

        // when
        SchemaWrite statement = schemaWriteInNewTransaction();
        SchemaKernelException e = assertThrows( SchemaKernelException.class, () -> statement.indexDrop( indexName ) );
        assertEquals( e.getMessage(), "Unable to drop index called `My fancy index`. There is no such index." );
        rollback();
    }

    @Test
    void shouldDisallowDroppingConstraintByNameThatDoesNotExist() throws KernelException
    {
        // given
        String constraintName = "my constraint";
        ConstraintDescriptor constraint;
        {
            SchemaWrite statement = schemaWriteInNewTransaction();
            constraint = statement.uniquePropertyConstraintCreate( uniqueForSchema( schema ).withName( "constraint name" ) );
            commit();
        }
        {
            SchemaWrite statement = schemaWriteInNewTransaction();
            statement.constraintDrop( constraint );
            commit();
        }

        // when
        SchemaWrite statement = schemaWriteInNewTransaction();
        SchemaKernelException e = assertThrows( SchemaKernelException.class, () -> statement.constraintDrop( constraintName ) );
        assertEquals( "Unable to drop constraint `my constraint`: No such constraint my constraint.", e.getMessage() );
        rollback();
    }

    @Test
    void shouldDisallowDroppingIndexByNameThatBelongsToConstraint() throws KernelException
    {
        // given
        String constraintName = "my constraint";
        {
            SchemaWrite statement = schemaWriteInNewTransaction();
            statement.uniquePropertyConstraintCreate( uniqueForSchema( schema ).withName( "constraint name" ) );
            commit();
        }

        // when
        SchemaWrite statement = schemaWriteInNewTransaction();
        SchemaKernelException e = assertThrows( SchemaKernelException.class, () -> statement.indexDrop( constraintName ) );
        assertEquals( "Unable to drop index called `my constraint`. There is no such index.", e.getMessage() );
        rollback();
    }

    @Test
    void shouldFailToCreateIndexWhereAConstraintAlreadyExists() throws Exception
    {
        // given
        {
            SchemaWrite statement = schemaWriteInNewTransaction();
            statement.uniquePropertyConstraintCreate( uniqueForSchema( schema ).withName( "constraint name" ) );
            commit();
        }

        var e = assertThrows( SchemaKernelException.class, () ->
        {
            SchemaWrite statement = schemaWriteInNewTransaction();
            statement.indexCreate( schema, "my index" );
            commit();
        } );
        assertEquals( "There is a uniqueness constraint on :" + LABEL + "(" + PROPERTY_KEY + "), so an index is " +
            "already created that matches this.", e.getMessage() );
        commit();
    }

    @Test
    void shouldListConstraintIndexesInTheCoreAPI() throws Exception
    {
        // given
        KernelTransaction transaction = newTransaction( AUTH_DISABLED );
        int labelId = transaction.tokenWrite().labelGetOrCreateForName( "Label1" );
        int propertyKeyId = transaction.tokenWrite().propertyKeyGetOrCreateForName( "property1" );
        LabelSchemaDescriptor schema = SchemaDescriptor.forLabel( labelId, propertyKeyId );
        transaction.schemaWrite().uniquePropertyConstraintCreate( uniqueForSchema( schema ).withName( "constraint name" ) );
        commit();

        // when
        try ( org.neo4j.graphdb.Transaction tx = db.beginTx() )
        {
            Set<IndexDefinition> indexes = Iterables.asSet( tx.schema().getIndexes() );

            // then
            assertEquals( 1, indexes.size() );
            IndexDefinition index = indexes.iterator().next();
            assertEquals( "Label1", single( index.getLabels() ).name() );
            assertEquals( asSet( "property1" ), Iterables.asSet( index.getPropertyKeys() ) );
            assertTrue( index.isConstraintIndex(), "index should be a constraint index" );

            // when
            var e = assertThrows( IllegalStateException.class, index::drop );
            assertEquals( "Constraint indexes cannot be dropped directly, instead drop the owning uniqueness constraint.", e.getMessage() );
        }
    }

    @Test
    void shouldListMultiTokenIndexesInTheCoreAPI() throws Exception
    {
        KernelTransaction transaction = newTransaction( AUTH_DISABLED );
        SchemaDescriptor schema = SchemaDescriptor.fulltext(
                EntityType.NODE, new int[]{labelId, labelId2}, new int[]{propertyKeyId} );
        IndexPrototype prototype = IndexPrototype.forSchema( schema, FulltextIndexProviderFactory.DESCRIPTOR ).withIndexType( IndexType.FULLTEXT );
        transaction.schemaWrite().indexCreate( prototype );
        commit();

        try ( org.neo4j.graphdb.Transaction tx = db.beginTx() )
        {
            Set<IndexDefinition> indexes = Iterables.asSet( tx.schema().getIndexes() );

            // then
            assertEquals( 1, indexes.size() );
            IndexDefinition index = indexes.iterator().next();
            assertThrows( IllegalStateException.class, index::getRelationshipTypes );
            assertThat( index.getLabels() ).contains( label( LABEL ), label( LABEL2 ) );
            assertFalse( index.isConstraintIndex(), "should not be a constraint index" );
            assertTrue( index.isMultiTokenIndex(), "should be a multi-token index" );
            assertFalse( index.isCompositeIndex(), "should not be a composite index" );
            assertTrue( index.isNodeIndex(), "should be a node index" );
            assertFalse( index.isRelationshipIndex(), "should not be a relationship index" );
            assertEquals( asSet( PROPERTY_KEY ), Iterables.asSet( index.getPropertyKeys() ) );
        }
    }

    @Test
    void shouldListCompositeIndexesInTheCoreAPI() throws Exception
    {
        KernelTransaction transaction = newTransaction( AUTH_DISABLED );
        SchemaDescriptor schema = SchemaDescriptor.forLabel( labelId, propertyKeyId, propertyKeyId2 );
        transaction.schemaWrite().indexCreate( schema, "my index" );
        commit();

        try ( org.neo4j.graphdb.Transaction tx = db.beginTx() )
        {
            Set<IndexDefinition> indexes = Iterables.asSet( tx.schema().getIndexes() );

            // then
            assertEquals( 1, indexes.size() );
            IndexDefinition index = indexes.iterator().next();
            assertEquals( LABEL, single( index.getLabels() ).name() );
            assertThat( index.getLabels() ).contains( label( LABEL ) );
            assertThrows( IllegalStateException.class, index::getRelationshipTypes );
            assertFalse( index.isConstraintIndex(), "should not be a constraint index" );
            assertFalse( index.isMultiTokenIndex(), "should not be a multi-token index" );
            assertTrue( index.isCompositeIndex(), "should be a composite index" );
            assertTrue( index.isNodeIndex(), "should be a node index" );
            assertFalse( index.isRelationshipIndex(), "should not be a relationship index" );
            assertEquals( asSet( PROPERTY_KEY, PROPERTY_KEY2 ), Iterables.asSet( index.getPropertyKeys() ) );
        }
    }

    @Test
    void shouldListRelationshipIndexesInTheCoreAPI() throws Exception
    {
        KernelTransaction transaction = newTransaction( AUTH_DISABLED );
        SchemaDescriptor schema = SchemaDescriptor.forRelType( relType, propertyKeyId );
        transaction.schemaWrite().indexCreate( schema, "my index" );
        commit();

        try ( org.neo4j.graphdb.Transaction tx = db.beginTx() )
        {
            Set<IndexDefinition> indexes = Iterables.asSet( tx.schema().getIndexes() );

            // then
            assertEquals( 1, indexes.size() );
            IndexDefinition index = indexes.iterator().next();
            assertThrows( IllegalStateException.class, index::getLabels );
            assertEquals( singletonList( withName( REL_TYPE ) ), index.getRelationshipTypes() );
            assertFalse( index.isConstraintIndex(), "should not be a constraint index" );
            assertFalse( index.isMultiTokenIndex(), "should not be a multi-token index" );
            assertFalse( index.isCompositeIndex(), "should not be a composite index" );
            assertFalse( index.isNodeIndex(), "should not be a node index" );
            assertTrue( index.isRelationshipIndex(), "should be a relationship index" );
            assertEquals( asSet( PROPERTY_KEY ), Iterables.asSet( index.getPropertyKeys() ) );
        }
    }

    @Test
    void shouldListCompositeMultiTokenRelationshipIndexesInTheCoreAPI() throws Exception
    {
        KernelTransaction transaction = newTransaction( AUTH_DISABLED );
        SchemaDescriptor schema = SchemaDescriptor.fulltext( EntityType.RELATIONSHIP, new int[]{relType, relType2},
                new int[]{propertyKeyId, propertyKeyId2} );
        IndexPrototype prototype = IndexPrototype.forSchema( schema, FulltextIndexProviderFactory.DESCRIPTOR )
                .withIndexType( IndexType.FULLTEXT )
                .withName( "index name" );
        transaction.schemaWrite().indexCreate( prototype );
        commit();

        try ( org.neo4j.graphdb.Transaction tx = db.beginTx() )
        {
            Set<IndexDefinition> indexes = Iterables.asSet( tx.schema().getIndexes() );

            // then
            assertEquals( 1, indexes.size() );
            IndexDefinition index = indexes.iterator().next();
            assertThrows( IllegalStateException.class, index::getLabels );
            assertThat( index.getRelationshipTypes() ).contains( withName( REL_TYPE ), withName( REL_TYPE2 ) );
            assertFalse( index.isConstraintIndex(), "should not be a constraint index" );
            assertTrue( index.isMultiTokenIndex(), "should be a multi-token index" );
            assertTrue( index.isCompositeIndex(), "should be a composite index" );
            assertFalse( index.isNodeIndex(), "should not be a node index" );
            assertTrue( index.isRelationshipIndex(), "should be a relationship index" );
            assertEquals( asSet( PROPERTY_KEY, PROPERTY_KEY2 ), Iterables.asSet( index.getPropertyKeys() ) );
        }
    }

    @Test
    void shouldListAll() throws Exception
    {
        // given
        SchemaWrite schemaWrite = schemaWriteInNewTransaction();
        IndexDescriptor index1 = schemaWrite.indexCreate( schema, "my index" );
        IndexBackedConstraintDescriptor constraint = schemaWrite.uniquePropertyConstraintCreate( uniqueForSchema( schema2 ).withName( "constraint name" ) )
                .asIndexBackedConstraint();
        commit();

        // then/when
        SchemaRead schemaRead = newTransaction().schemaRead();
        IndexDescriptor index2 = Iterators.single( schemaRead.index( constraint.schema() ) );
        List<IndexDescriptor> indexes = Iterators.asList( schemaRead.indexesGetAll() );
        assertThat( indexes ).contains( index1, index2 );
        commit();
    }

    private static Runnable createIndex( GraphDatabaseAPI db, Label label, String propertyKey )
    {
        return () ->
        {
            try ( org.neo4j.graphdb.Transaction transaction = db.beginTx() )
            {
                transaction.schema().indexFor( label ).on( propertyKey ).create();
                transaction.commit();
            }

            try ( org.neo4j.graphdb.Transaction transaction = db.beginTx() )
            {
                transaction.schema().awaitIndexesOnline( 1, TimeUnit.MINUTES );
                transaction.commit();
            }
        };
    }
}
