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
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import org.neo4j.common.EntityType;
import org.neo4j.configuration.Config;
import org.neo4j.exceptions.KernelException;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.schema.IndexDefinition;
import org.neo4j.internal.helpers.collection.Iterables;
import org.neo4j.internal.helpers.collection.Iterators;
import org.neo4j.internal.kernel.api.SchemaRead;
import org.neo4j.internal.kernel.api.SchemaWrite;
import org.neo4j.internal.kernel.api.TokenWrite;
import org.neo4j.internal.kernel.api.Transaction;
import org.neo4j.internal.kernel.api.Write;
import org.neo4j.internal.kernel.api.exceptions.schema.SchemaKernelException;
import org.neo4j.internal.schema.IndexConfig;
import org.neo4j.internal.schema.IndexDescriptor;
import org.neo4j.internal.schema.LabelSchemaDescriptor;
import org.neo4j.internal.schema.SchemaDescriptor;
import org.neo4j.internal.schema.constraints.ConstraintDescriptorFactory;
import org.neo4j.internal.schema.constraints.IndexBackedConstraintDescriptor;
import org.neo4j.internal.schema.constraints.UniquenessConstraintDescriptor;
import org.neo4j.kernel.api.impl.fulltext.FulltextIndexProviderFactory;
import org.neo4j.kernel.impl.api.integrationtest.KernelIntegrationTest;
import org.neo4j.kernel.impl.api.state.ConstraintIndexCreator;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.logging.AssertableLogProvider;

import static java.util.Collections.emptySet;
import static java.util.Collections.singletonList;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;
import static org.neo4j.configuration.GraphDatabaseSettings.default_schema_provider;
import static org.neo4j.graphdb.Label.label;
import static org.neo4j.graphdb.RelationshipType.withName;
import static org.neo4j.internal.helpers.collection.Iterables.single;
import static org.neo4j.internal.helpers.collection.Iterators.asSet;
import static org.neo4j.internal.kernel.api.security.LoginContext.AUTH_DISABLED;

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
    private LabelSchemaDescriptor descriptor;
    private LabelSchemaDescriptor descriptor2;
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
        descriptor = SchemaDescriptor.forLabel( labelId, propertyKeyId );
        descriptor2 = SchemaDescriptor.forLabel( labelId, propertyKeyId2 );
        commit();
        executorService = Executors.newCachedThreadPool();
    }

    @AfterEach
    void tearDown()
    {
        executorService.shutdown();
    }

    @Test
    void createIndexForAnotherLabelWhileHoldingSharedLockOnOtherLabel() throws KernelException
    {
        TokenWrite tokenWrite = tokenWriteInNewTransaction();
        int label2 = tokenWrite.labelGetOrCreateForName( "Label2" );

        Write write = dataWriteInNewTransaction();
        long nodeId = write.nodeCreate();
        write.nodeAddLabel( nodeId, label2 );

        schemaWriteInNewTransaction().indexCreate( descriptor );
        commit();
    }

    @Test
    @Timeout( 10 )
    void createIndexesForDifferentLabelsConcurrently() throws Throwable
    {
        TokenWrite tokenWrite = tokenWriteInNewTransaction();
        int label2 = tokenWrite.labelGetOrCreateForName( "Label2" );

        LabelSchemaDescriptor anotherLabelDescriptor = SchemaDescriptor.forLabel( label2, propertyKeyId );
        schemaWriteInNewTransaction().indexCreate( anotherLabelDescriptor );

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
        IndexDescriptor expectedRule = schemaWriteOperations.indexCreate( descriptor );
        commit();

        // THEN
        SchemaRead schemaRead = newTransaction().schemaRead();
        assertEquals( asSet( expectedRule ), asSet( schemaRead.indexesGetForLabel( labelId ) ) );
        assertEquals( expectedRule, schemaRead.index( descriptor.getLabelId(), descriptor.getPropertyIds() ) );
        commit();
    }

    @Test
    void committedAndTransactionalIndexRulesShouldBeMerged() throws Exception
    {
        // GIVEN
        SchemaWrite schemaWriteOperations = schemaWriteInNewTransaction();
        IndexDescriptor existingRule = schemaWriteOperations.indexCreate( descriptor );
        commit();

        // WHEN
        Transaction transaction = newTransaction( AUTH_DISABLED );
        IndexDescriptor addedRule = transaction.schemaWrite().indexCreate( SchemaDescriptor.forLabel( labelId, propertyKeyId2 ) );
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
        schemaWrite.indexCreate( descriptor );
        // don't mark as success
        rollback();

        // THEN
        Transaction transaction = newTransaction();
        assertEquals( emptySet(), asSet( transaction.schemaRead().indexesGetForLabel( labelId ) ) );
        commit();
    }

    @Test
    void shouldBeAbleToRemoveAConstraintIndexWithoutOwner() throws Exception
    {
        // given
        AssertableLogProvider logProvider = new AssertableLogProvider();
        ConstraintIndexCreator creator = new ConstraintIndexCreator( () -> kernel, indexingService, logProvider );

        String defaultProvider = Config.defaults().get( default_schema_provider );
        UniquenessConstraintDescriptor constraint = ConstraintDescriptorFactory.uniqueForSchema( descriptor ).withName( "constraint name" );
        IndexDescriptor constraintIndex = creator.createConstraintIndex( constraint, defaultProvider );
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
    void shouldDisallowDroppingIndexThatDoesNotExist() throws Exception
    {
        // given
        IndexDescriptor index;
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

        var e = assertThrows( SchemaKernelException.class, () ->
        {
            SchemaWrite statement = schemaWriteInNewTransaction();
            statement.indexDrop( index );
        } );
        assertEquals( "Unable to drop index on :label[" + labelId + "](property[" + propertyKeyId + "]): " +
            "No such INDEX ON :label[" + labelId + "](property[" + propertyKeyId + "]).", e.getMessage() );
        commit();
    }

    @Test
    void shouldDisallowDroppingIndexBySchemaThatDoesNotExist() throws Exception
    {
        // given
        IndexDescriptor index;
        {
            SchemaWrite statement = schemaWriteInNewTransaction();
            index = statement.indexCreate( descriptor );
            commit();
        }
        {
            SchemaWrite statement = schemaWriteInNewTransaction();
            statement.indexDrop( index.schema() );
            commit();
        }

        // when
        try
        {
            SchemaWrite statement = schemaWriteInNewTransaction();
            statement.indexDrop( index.schema() );
            fail( "Expected to fail" );
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
    void shouldFailToCreateIndexWhereAConstraintAlreadyExists() throws Exception
    {
        // given
        {
            SchemaWrite statement = schemaWriteInNewTransaction();
            statement.uniquePropertyConstraintCreate( descriptor, "constraint name" );
            commit();
        }

        var e = assertThrows( SchemaKernelException.class, () ->
        {
            SchemaWrite statement = schemaWriteInNewTransaction();
            statement.indexCreate( descriptor );
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
        Transaction transaction = newTransaction( AUTH_DISABLED );
        int labelId = transaction.tokenWrite().labelGetOrCreateForName( "Label1" );
        int propertyKeyId = transaction.tokenWrite().propertyKeyGetOrCreateForName( "property1" );
        LabelSchemaDescriptor schema = SchemaDescriptor.forLabel( labelId, propertyKeyId );
        transaction.schemaWrite().uniquePropertyConstraintCreate( schema, "constraint name" );
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
            assertTrue( index.isConstraintIndex(), "index should be a constraint index" );

            // when
            var e = assertThrows( IllegalStateException.class, index::drop );
            assertEquals( "Constraint indexes cannot be dropped directly, instead drop the owning uniqueness constraint.", e.getMessage() );
        }
    }

    @Test
    void shouldListMultiTokenIndexesInTheCoreAPI() throws Exception
    {
        Transaction transaction = newTransaction( AUTH_DISABLED );
        SchemaDescriptor descriptor = SchemaDescriptor.fulltext(
                EntityType.NODE, IndexConfig.empty(), new int[]{labelId, labelId2}, new int[]{propertyKeyId} );
        transaction.schemaWrite().indexCreate( descriptor, FulltextIndexProviderFactory.DESCRIPTOR.name(), null );
        commit();

        try ( org.neo4j.graphdb.Transaction ignore = db.beginTx() )
        {
            Set<IndexDefinition> indexes = Iterables.asSet( db.schema().getIndexes() );

            // then
            assertEquals( 1, indexes.size() );
            IndexDefinition index = indexes.iterator().next();
            assertThrows( IllegalStateException.class, index::getLabel );
            assertThrows( IllegalStateException.class, index::getRelationshipType );
            assertThrows( IllegalStateException.class, index::getRelationshipTypes );
            assertThat( index.getLabels(), containsInAnyOrder( label( LABEL ), label( LABEL2 ) ) );
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
        Transaction transaction = newTransaction( AUTH_DISABLED );
        SchemaDescriptor descriptor = SchemaDescriptor.forLabel( labelId, propertyKeyId, propertyKeyId2 );
        transaction.schemaWrite().indexCreate( descriptor );
        commit();

        try ( org.neo4j.graphdb.Transaction ignore = db.beginTx() )
        {
            Set<IndexDefinition> indexes = Iterables.asSet( db.schema().getIndexes() );

            // then
            assertEquals( 1, indexes.size() );
            IndexDefinition index = indexes.iterator().next();
            assertEquals( LABEL, single( index.getLabels() ).name() );
            assertThat( index.getLabels(), containsInAnyOrder( label( LABEL ) ) );
            assertThrows( IllegalStateException.class, index::getRelationshipType );
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
        Transaction transaction = newTransaction( AUTH_DISABLED );
        SchemaDescriptor descriptor = SchemaDescriptor.forRelType( relType, propertyKeyId );
        transaction.schemaWrite().indexCreate( descriptor );
        commit();

        try ( org.neo4j.graphdb.Transaction ignore = db.beginTx() )
        {
            Set<IndexDefinition> indexes = Iterables.asSet( db.schema().getIndexes() );

            // then
            assertEquals( 1, indexes.size() );
            IndexDefinition index = indexes.iterator().next();
            assertThrows( IllegalStateException.class, index::getLabel );
            assertThrows( IllegalStateException.class, index::getLabels );
            assertEquals( REL_TYPE, index.getRelationshipType().name() );
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
        Transaction transaction = newTransaction( AUTH_DISABLED );
        SchemaDescriptor descriptor = SchemaDescriptor.fulltext( EntityType.RELATIONSHIP, IndexConfig.empty(), new int[]{relType, relType2},
                new int[]{propertyKeyId, propertyKeyId2} );
        transaction.schemaWrite().indexCreate( descriptor, FulltextIndexProviderFactory.DESCRIPTOR.name(), "index name" );
        commit();

        try ( org.neo4j.graphdb.Transaction ignore = db.beginTx() )
        {
            Set<IndexDefinition> indexes = Iterables.asSet( db.schema().getIndexes() );

            // then
            assertEquals( 1, indexes.size() );
            IndexDefinition index = indexes.iterator().next();
            assertThrows( IllegalStateException.class, index::getLabel );
            assertThrows( IllegalStateException.class, index::getLabels );
            assertThrows( IllegalStateException.class, index::getRelationshipType );
            assertThat( index.getRelationshipTypes(), containsInAnyOrder( withName( REL_TYPE ), withName( REL_TYPE2 ) ) );
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
        IndexDescriptor index1 = schemaWrite.indexCreate( descriptor );
        IndexBackedConstraintDescriptor constraint = schemaWrite.uniquePropertyConstraintCreate( descriptor2, "constraint name" ).asIndexBackedConstraint();
        commit();

        // then/when
        SchemaRead schemaRead = newTransaction().schemaRead();
        IndexDescriptor index2 = schemaRead.index( constraint.schema() );
        List<IndexDescriptor> indexes = Iterators.asList( schemaRead.indexesGetAll() );
        assertThat( indexes, containsInAnyOrder( index1, index2 ) );
        commit();
    }

    private static Runnable createIndex( GraphDatabaseAPI db, Label label, String propertyKey )
    {
        return () ->
        {
            try ( org.neo4j.graphdb.Transaction transaction = db.beginTx() )
            {
                db.schema().indexFor( label ).on( propertyKey ).create();
                transaction.commit();
            }

            try ( org.neo4j.graphdb.Transaction transaction = db.beginTx() )
            {
                db.schema().awaitIndexesOnline( 1, TimeUnit.MINUTES );
                transaction.commit();
            }
        };
    }
}
