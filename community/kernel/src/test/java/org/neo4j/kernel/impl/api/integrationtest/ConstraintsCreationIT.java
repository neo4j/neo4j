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
package org.neo4j.kernel.impl.api.integrationtest;

import org.junit.Before;
import org.junit.Test;

import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import org.neo4j.function.Function;
import org.neo4j.graphdb.ConstraintViolationException;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.schema.ConstraintDefinition;
import org.neo4j.graphdb.schema.IndexDefinition;
import org.neo4j.graphdb.schema.Schema;
import org.neo4j.kernel.api.ReadOperations;
import org.neo4j.kernel.api.SchemaWriteOperations;
import org.neo4j.kernel.api.constraints.MandatoryPropertyConstraint;
import org.neo4j.kernel.api.constraints.PropertyConstraint;
import org.neo4j.kernel.api.constraints.UniquenessConstraint;
import org.neo4j.kernel.api.exceptions.KernelException;
import org.neo4j.kernel.api.exceptions.TransactionFailureException;
import org.neo4j.kernel.api.exceptions.schema.AlreadyConstrainedException;
import org.neo4j.kernel.api.exceptions.schema.DropConstraintFailureException;
import org.neo4j.kernel.api.exceptions.schema.NoSuchConstraintException;
import org.neo4j.kernel.api.index.IndexDescriptor;
import org.neo4j.kernel.impl.store.SchemaStorage;
import org.neo4j.kernel.impl.store.UniquePropertyConstraintRule;
import org.neo4j.kernel.impl.store.record.IndexRule;

import static java.util.Collections.singletonList;
import static org.hamcrest.CoreMatchers.instanceOf;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.neo4j.graphdb.DynamicLabel.label;
import static org.neo4j.helpers.collection.IteratorUtil.asCollection;
import static org.neo4j.helpers.collection.IteratorUtil.asList;
import static org.neo4j.helpers.collection.IteratorUtil.asSet;
import static org.neo4j.helpers.collection.IteratorUtil.emptySetOf;
import static org.neo4j.helpers.collection.IteratorUtil.single;

public class ConstraintsCreationIT extends KernelIntegrationTest
{
    @Test
    public void shouldBeAbleToStoreAndRetrieveUniquePropertyConstraintRule() throws Exception
    {
        // given
        PropertyConstraint constraint;
        {
            SchemaWriteOperations statement = schemaWriteOperationsInNewTransaction();

            // when
            constraint = statement.uniquePropertyConstraintCreate( labelId, propertyKeyId );

            // then
            assertEquals( constraint, single( statement.constraintsGetForLabelAndPropertyKey( labelId,propertyKeyId ) ) );
            assertEquals( constraint, single( statement.constraintsGetForLabel(  labelId ) ) );

            // given
            commit();
        }
        {
            ReadOperations statement = readOperationsInNewTransaction();

            // when
            Iterator<PropertyConstraint> constraints = statement
                    .constraintsGetForLabelAndPropertyKey( labelId, propertyKeyId );

            // then
            assertEquals( constraint, single( constraints ) );
        }
    }

    @Test
    public void shouldCreateAndRetrieveMandatoryPropertyConstraint() throws Exception
    {
        // given
        PropertyConstraint constraint;
        {
            SchemaWriteOperations statement = schemaWriteOperationsInNewTransaction();

            // when
            constraint = statement.mandatoryPropertyConstraintCreate( labelId, propertyKeyId );

            // then
            assertEquals( constraint, single( statement.constraintsGetForLabelAndPropertyKey( labelId,propertyKeyId ) ) );
            assertEquals( constraint, single( statement.constraintsGetForLabel(  labelId ) ) );

            // given
            commit();
        }
        {
            ReadOperations statement = readOperationsInNewTransaction();

            // when
            Iterator<PropertyConstraint> constraints = statement
                    .constraintsGetForLabelAndPropertyKey( labelId, propertyKeyId );

            // then
            assertEquals( constraint, single( constraints ) );
        }
    }

    @Test
    public void shouldNotPersistUniquePropertyConstraintsCreatedInAbortedTransaction() throws Exception
    {
        // given
        {
            SchemaWriteOperations statement = schemaWriteOperationsInNewTransaction();

            statement.uniquePropertyConstraintCreate( labelId, propertyKeyId );

            // when
            rollback();
        }
        {
            ReadOperations statement = readOperationsInNewTransaction();

            // then
            Iterator<PropertyConstraint> constraints = statement.constraintsGetForLabelAndPropertyKey( labelId, propertyKeyId );
            assertFalse( "should not have any constraints", constraints.hasNext() );
        }
    }

    @Test
    public void shouldNotPersistMandatoryPropertyConstraintsCreatedInAbortedTransaction() throws Exception
    {
        // given
        {
            SchemaWriteOperations statement = schemaWriteOperationsInNewTransaction();

            statement.mandatoryPropertyConstraintCreate( labelId, propertyKeyId );

            // when
            rollback();
        }
        {
            ReadOperations statement = readOperationsInNewTransaction();

            // then
            Iterator<PropertyConstraint> constraints = statement.constraintsGetForLabelAndPropertyKey( labelId, propertyKeyId );
            assertFalse( "should not have any constraints", constraints.hasNext() );
        }
    }

    @Test
    public void shouldNotStoreUniquePropertyConstraintThatIsRemovedInTheSameTransaction() throws Exception
    {
        // given
        {
            SchemaWriteOperations statement = schemaWriteOperationsInNewTransaction();

            PropertyConstraint constraint = statement.uniquePropertyConstraintCreate( labelId, propertyKeyId );

            // when
            statement.constraintDrop( constraint );

            // then
            assertFalse( "should not have any constraints",
                    statement.constraintsGetForLabelAndPropertyKey( labelId, propertyKeyId ).hasNext() );

            // when
            commit();
        }
        {
            ReadOperations statement = readOperationsInNewTransaction();

            // then
            assertFalse( "should not have any constraints",
                    statement.constraintsGetForLabelAndPropertyKey( labelId, propertyKeyId ).hasNext() );
        }
    }

    @Test
    public void shouldNotStoreMandatoryPropertyConstraintThatIsRemovedInTheSameTransaction() throws Exception
    {
        // given
        {
            SchemaWriteOperations statement = schemaWriteOperationsInNewTransaction();

            PropertyConstraint constraint = statement.mandatoryPropertyConstraintCreate( labelId, propertyKeyId );

            // when
            statement.constraintDrop( constraint );

            // then
            assertFalse( "should not have any constraints",
                    statement.constraintsGetForLabelAndPropertyKey( labelId, propertyKeyId ).hasNext() );

            // when
            commit();
        }
        {
            ReadOperations statement = readOperationsInNewTransaction();

            // then
            assertFalse( "should not have any constraints",
                    statement.constraintsGetForLabelAndPropertyKey( labelId, propertyKeyId ).hasNext() );
        }
    }

    @Test
    public void shouldDropMandatoryPropertyConstraint() throws Exception
    {
        // given
        PropertyConstraint constraint;
        {
            SchemaWriteOperations statement = schemaWriteOperationsInNewTransaction();
            constraint = statement.mandatoryPropertyConstraintCreate( labelId, propertyKeyId );
            commit();
        }

        // when
        {
            SchemaWriteOperations statement = schemaWriteOperationsInNewTransaction();
            statement.constraintDrop( constraint );
            commit();
        }

        // then
        {
            ReadOperations statement = readOperationsInNewTransaction();

            // then
            assertFalse( "should not have any constraints",
                    statement.constraintsGetForLabelAndPropertyKey( labelId, propertyKeyId ).hasNext() );
        }
    }

    @Test
    public void shouldNotCreateUniquePropertyConstraintThatAlreadyExists() throws Exception
    {
        // given
        {
            SchemaWriteOperations statement = schemaWriteOperationsInNewTransaction();
            statement.uniquePropertyConstraintCreate( labelId, propertyKeyId );
            commit();
        }

        // when
        try
        {
            SchemaWriteOperations statement = schemaWriteOperationsInNewTransaction();

            statement.uniquePropertyConstraintCreate( labelId, propertyKeyId );

            fail( "Should not have validated" );
        }
        // then
        catch ( AlreadyConstrainedException e )
        {
            // good
        }
    }

    @Test
    public void shouldNotCreateMandatoryPropertyConstraintThatAlreadyExists() throws Exception
    {
        // given
        {
            SchemaWriteOperations statement = schemaWriteOperationsInNewTransaction();
            statement.mandatoryPropertyConstraintCreate( labelId, propertyKeyId );
            commit();
        }

        // when
        try
        {
            SchemaWriteOperations statement = schemaWriteOperationsInNewTransaction();

            statement.mandatoryPropertyConstraintCreate( labelId, propertyKeyId );

            fail( "Should not have validated" );
        }
        // then
        catch ( AlreadyConstrainedException e )
        {
            // good
        }
    }

    @Test
    public void shouldNotRemoveUniquePropertyConstraintThatGetsReAdded() throws Exception
    {
        // given
        PropertyConstraint constraint;
        {
            SchemaWriteOperations statement = schemaWriteOperationsInNewTransaction();
            constraint = statement.uniquePropertyConstraintCreate( labelId, propertyKeyId );
            commit();
        }
        SchemaStateCheck schemaState = new SchemaStateCheck().setUp();
        {
            SchemaWriteOperations statement = schemaWriteOperationsInNewTransaction();

            // when
            statement.constraintDrop( constraint );
            statement.uniquePropertyConstraintCreate( labelId, propertyKeyId );
            commit();
        }
        {
            ReadOperations statement = readOperationsInNewTransaction();

            // then
            assertEquals( singletonList( constraint ), asCollection( statement.constraintsGetForLabelAndPropertyKey( labelId, propertyKeyId ) ) );
            schemaState.assertNotCleared(statement);
        }
    }

    @Test
    public void shouldNotRemoveMandatoryPropertyConstraintThatGetsReAdded() throws Exception
    {
        // given
        PropertyConstraint constraint;
        {
            SchemaWriteOperations statement = schemaWriteOperationsInNewTransaction();
            constraint = statement.mandatoryPropertyConstraintCreate( labelId, propertyKeyId );
            commit();
        }
        SchemaStateCheck schemaState = new SchemaStateCheck().setUp();
        {
            SchemaWriteOperations statement = schemaWriteOperationsInNewTransaction();

            // when
            statement.constraintDrop( constraint );
            statement.mandatoryPropertyConstraintCreate( labelId, propertyKeyId );
            commit();
        }
        {
            ReadOperations statement = readOperationsInNewTransaction();

            // then
            assertEquals( singletonList( constraint ), asCollection( statement.constraintsGetForLabelAndPropertyKey( labelId, propertyKeyId ) ) );
            schemaState.assertNotCleared(statement);
        }
    }

    @Test
    public void shouldClearSchemaStateWhenConstraintIsCreated() throws Exception
    {
        for ( int i = 0; i < 2; i++ )
        {
            // given
            SchemaStateCheck schemaState = new SchemaStateCheck().setUp();

            SchemaWriteOperations statement = schemaWriteOperationsInNewTransaction();

            // when
            if ( i == 0 )
            {
                statement.uniquePropertyConstraintCreate( labelId, propertyKeyId );
            }
            else
            {
                statement.mandatoryPropertyConstraintCreate( labelId, propertyKeyId );
            }
            commit();

            // then
            schemaState.assertCleared(readOperationsInNewTransaction());
            rollback();
        }
    }

    @Test
    public void shouldClearSchemaStateWhenConstraintIsDropped() throws Exception
    {
        for ( int i = 0; i < 2; i++ )
        {
            // given
            PropertyConstraint constraint;
            SchemaStateCheck schemaState;
            {
                SchemaWriteOperations statement = schemaWriteOperationsInNewTransaction();
                if ( i == 0 )
                {
                    constraint = statement.uniquePropertyConstraintCreate( labelId, propertyKeyId );
                }
                else
                {
                    constraint = statement.mandatoryPropertyConstraintCreate( labelId, propertyKeyId );
                }
                commit();

                schemaState = new SchemaStateCheck().setUp();
            }

            {
                SchemaWriteOperations statement = schemaWriteOperationsInNewTransaction();

                // when
                statement.constraintDrop( constraint );
                commit();
            }

            // then
            schemaState.assertCleared( readOperationsInNewTransaction() );
            rollback();
        }
    }

    @Test
    public void shouldCreateAnIndexToGoAlongWithAUniquePropertyConstraint() throws Exception
    {
        // when
        {
            SchemaWriteOperations statement = schemaWriteOperationsInNewTransaction();
            statement.uniquePropertyConstraintCreate( labelId, propertyKeyId );
            commit();
        }

        // then
        {
            ReadOperations statement = readOperationsInNewTransaction();
            assertEquals( asSet( new IndexDescriptor( labelId, propertyKeyId ) ),
                    asSet( statement.uniqueIndexesGetAll() ) );
        }
    }

    @Test
    public void shouldDropCreatedConstraintIndexWhenRollingBackConstraintCreation() throws Exception
    {
        // given
        {
            SchemaWriteOperations statement = schemaWriteOperationsInNewTransaction();
            statement.uniquePropertyConstraintCreate( labelId, propertyKeyId );
            assertEquals( asSet( new IndexDescriptor( labelId, propertyKeyId ) ),
                    asSet( statement.uniqueIndexesGetAll() ) );
        }

        // when
        rollback();

        // then
        {
            ReadOperations statement = readOperationsInNewTransaction();
            assertEquals( emptySetOf( IndexDescriptor.class ), asSet( statement.uniqueIndexesGetAll() ) );
            commit();
        }
    }

    @Test
    public void shouldDropConstraintIndexWhenDroppingConstraint() throws Exception
    {
        // given
        PropertyConstraint constraint;
        {
            SchemaWriteOperations statement = schemaWriteOperationsInNewTransaction();
            constraint = statement.uniquePropertyConstraintCreate( labelId, propertyKeyId );
            assertEquals( asSet( new IndexDescriptor( labelId, propertyKeyId ) ),
                    asSet( statement.uniqueIndexesGetAll() ) );
            commit();
        }

        // when
        {
            SchemaWriteOperations statement = schemaWriteOperationsInNewTransaction();
            statement.constraintDrop( constraint );
            commit();
        }

        // then
        {
            ReadOperations statement = readOperationsInNewTransaction();
            assertEquals( emptySetOf( IndexDescriptor.class ), asSet( statement.uniqueIndexesGetAll( ) ) );
            commit();
        }
    }

    @Test
    public void shouldNotDropConstraintThatDoesNotExist() throws Exception
    {
        for ( PropertyConstraint constraint : new PropertyConstraint[]{
                new UniquenessConstraint( labelId, propertyKeyId ),
                new MandatoryPropertyConstraint( labelId, propertyKeyId ),
        } )
        {
            // when
            {
                SchemaWriteOperations statement = schemaWriteOperationsInNewTransaction();

                try
                {
                    statement.constraintDrop( constraint );
                    fail( "Should not have dropped constraint" );
                }
                catch ( DropConstraintFailureException e )
                {
                    assertThat( e.getCause(), instanceOf( NoSuchConstraintException.class ) );
                }
                commit();
            }

            // then
            {
                ReadOperations statement = readOperationsInNewTransaction();
                assertEquals( emptySetOf( IndexDescriptor.class ), asSet( statement.uniqueIndexesGetAll() ) );
                commit();
            }
        }
    }

    @Test
    public void shouldNotDropMandatoryPropertyConstraintThatDoesNotExistWhenThereIsAUniquePropertyConstraint()
            throws Exception
    {
        // given
        PropertyConstraint constraint;
        {
            SchemaWriteOperations statement = schemaWriteOperationsInNewTransaction();
            constraint = statement.uniquePropertyConstraintCreate( labelId, propertyKeyId );
            commit();
        }

        // when
        try
        {
            SchemaWriteOperations statement = schemaWriteOperationsInNewTransaction();
            statement.constraintDrop( new MandatoryPropertyConstraint( constraint.label(), constraint.propertyKeyId() ) );

            fail( "expected exception" );
        }
        // then
        catch ( DropConstraintFailureException e )
        {
            assertThat( e.getCause(), instanceOf( NoSuchConstraintException.class ) );
        }
        finally
        {
            rollback();
        }

        // then
        {
            ReadOperations statement = readOperationsInNewTransaction();

            Iterator<PropertyConstraint> constraints = statement
                    .constraintsGetForLabelAndPropertyKey( labelId, propertyKeyId );

            assertEquals( constraint, single( constraints ) );
        }
    }

    @Test
    public void shouldNotDropUniquePropertyConstraintThatDoesNotExistWhenThereIsAMandatoryPropertyConstraint()
            throws Exception
    {
        // given
        PropertyConstraint constraint;
        {
            SchemaWriteOperations statement = schemaWriteOperationsInNewTransaction();
            constraint = statement.mandatoryPropertyConstraintCreate( labelId, propertyKeyId );
            commit();
        }

        // when
        try
        {
            SchemaWriteOperations statement = schemaWriteOperationsInNewTransaction();
            statement.constraintDrop( new UniquenessConstraint( constraint.label(), constraint.propertyKeyId() ) );

            fail( "expected exception" );
        }
        // then
        catch ( DropConstraintFailureException e )
        {
            assertThat( e.getCause(), instanceOf( NoSuchConstraintException.class ) );
        }
        finally
        {
            rollback();
        }

        // then
        {
            ReadOperations statement = readOperationsInNewTransaction();

            Iterator<PropertyConstraint> constraints = statement
                    .constraintsGetForLabelAndPropertyKey( labelId, propertyKeyId );

            assertEquals( constraint, single( constraints ) );
        }
    }

    @Test
    public void committedConstraintRuleShouldCrossReferenceTheCorrespondingIndexRule() throws Exception
    {
        // when
        SchemaWriteOperations statement = schemaWriteOperationsInNewTransaction();
        statement.uniquePropertyConstraintCreate( labelId, propertyKeyId );
        commit();

        // then
        SchemaStorage schema = new SchemaStorage( neoStore().getSchemaStore() );
        IndexRule indexRule = schema.indexRule( labelId, propertyKeyId );
        UniquePropertyConstraintRule constraintRule = schema.uniquenessConstraint( labelId, propertyKeyId );
        assertEquals( constraintRule.getId(), indexRule.getOwningConstraint().longValue() );
        assertEquals( indexRule.getId(), constraintRule.getOwnedIndex() );
    }

    @Test
    public void shouldNotLeaveAnyStateBehindAfterFailingToCreateUniquePropertyConstraint() throws Exception
    {
        // given
        try ( org.neo4j.graphdb.Transaction tx = db.beginTx() )
        {
            assertEquals( Collections.<ConstraintDefinition>emptyList(),
                          asList( db.schema().getConstraints() ) );
            assertEquals( Collections.<IndexDefinition, Schema.IndexState>emptyMap(),
                          indexesWithState( db.schema() ) );
            db.createNode( label( "Foo" ) ).setProperty( "bar", "baz" );
            db.createNode( label( "Foo" ) ).setProperty( "bar", "baz" );

            tx.success();
        }

        // when
        try ( org.neo4j.graphdb.Transaction tx = db.beginTx() )
        {
            db.schema().constraintFor( label( "Foo" ) ).assertPropertyIsUnique( "bar" ).create();

            tx.success();
            fail( "expected failure" );
        }
        catch ( ConstraintViolationException e )
        {
            assertTrue( e.getMessage().startsWith( "Unable to create CONSTRAINT" ) );
        }

        // then
        try ( org.neo4j.graphdb.Transaction tx = db.beginTx() )
        {
            assertEquals( Collections.<ConstraintDefinition>emptyList(),
                          asList( db.schema().getConstraints() ) );
            assertEquals( Collections.<IndexDefinition, Schema.IndexState>emptyMap(),
                          indexesWithState( db.schema() ) );
            tx.success();
        }
    }

    @Test
    public void shouldBeAbleToResolveConflictsAndRecreateConstraintAfterFailingToCreateUniquePropertyConstraintDueToConflict()
            throws Exception
    {
        // given
        Node node1, node2;
        try ( org.neo4j.graphdb.Transaction tx = db.beginTx() )
        {
            assertEquals( Collections.<ConstraintDefinition>emptyList(),
                          asList( db.schema().getConstraints() ) );
            assertEquals( Collections.<IndexDefinition, Schema.IndexState>emptyMap(),
                          indexesWithState( db.schema() ) );
            (node1 = db.createNode( label( "Foo" ) )).setProperty( "bar", "baz" );
            (node2 = db.createNode( label( "Foo" ) )).setProperty( "bar", "baz" );

            tx.success();
        }

        // when
        try ( org.neo4j.graphdb.Transaction tx = db.beginTx() )
        {
            db.schema().constraintFor( label( "Foo" ) ).assertPropertyIsUnique( "bar" ).create();

            tx.success();
            fail( "expected failure" );
        }
        catch ( ConstraintViolationException e )
        {
            assertTrue( e.getMessage().startsWith( "Unable to create CONSTRAINT" ) );
        }
        try ( org.neo4j.graphdb.Transaction tx = db.beginTx() )
        {
            node1.delete();
            node2.delete();
            tx.success();
        }

        // then - this should not fail
        try ( org.neo4j.graphdb.Transaction tx = db.beginTx() )
        {
            db.schema().constraintFor( label( "Foo" ) ).assertPropertyIsUnique( "bar" ).create();

            tx.success();
        }
    }

    private static Map<IndexDefinition, Schema.IndexState> indexesWithState( Schema schema )
    {
        HashMap<IndexDefinition, Schema.IndexState> result = new HashMap<>();
        for ( IndexDefinition definition : schema.getIndexes() )
        {
            result.put( definition, schema.getIndexState( definition ) );
        }
        return result;
    }

    private int labelId, propertyKeyId;

    @Before
    public void createKeys() throws KernelException
    {
        SchemaWriteOperations statement = schemaWriteOperationsInNewTransaction();
        this.labelId = statement.labelGetOrCreateForName( "Foo" );
        this.propertyKeyId = statement.propertyKeyGetOrCreateForName( "bar" );
        commit();
    }

    private class SchemaStateCheck implements Function<String, Integer>
    {
        int invocationCount;

        @Override
        public Integer apply( String s )
        {
            invocationCount++;
            return Integer.parseInt( s );
        }

        public SchemaStateCheck setUp() throws TransactionFailureException
        {
            ReadOperations readOperations = readOperationsInNewTransaction();
            checkState(readOperations);
            commit();
            return this;
        }

        public void assertCleared(ReadOperations readOperations)
        {
            int count = invocationCount;
            checkState( readOperations );
            assertEquals( "schema state should have been cleared.", count + 1, invocationCount );
        }

        public void assertNotCleared(ReadOperations readOperations)
        {
            int count = invocationCount;
            checkState( readOperations );
            assertEquals( "schema state should not have been cleared.", count, invocationCount );
        }

        private SchemaStateCheck checkState( ReadOperations readOperations )
        {
            assertEquals( Integer.valueOf( 7 ), readOperations.schemaStateGetOrCreate( "7", this ) );
            return this;
        }
    }
}
