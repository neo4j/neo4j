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
package org.neo4j.kernel.impl.api.integrationtest;

import java.util.Iterator;

import org.junit.Before;
import org.junit.Test;

import org.neo4j.helpers.Function;
import org.neo4j.kernel.api.SchemaStatement;
import org.neo4j.kernel.api.constraints.UniquenessConstraint;
import org.neo4j.kernel.api.exceptions.schema.AlreadyConstrainedException;
import org.neo4j.kernel.api.exceptions.schema.DropConstraintFailureException;
import org.neo4j.kernel.api.exceptions.schema.NoSuchConstraintException;
import org.neo4j.kernel.api.exceptions.schema.SchemaKernelException;
import org.neo4j.kernel.impl.api.SchemaStorage;
import org.neo4j.kernel.impl.api.index.IndexDescriptor;
import org.neo4j.kernel.impl.nioneo.store.IndexRule;
import org.neo4j.kernel.impl.nioneo.store.UniquenessConstraintRule;

import static java.util.Collections.singletonList;

import static org.hamcrest.CoreMatchers.instanceOf;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;

import static org.neo4j.helpers.collection.IteratorUtil.asCollection;
import static org.neo4j.helpers.collection.IteratorUtil.asSet;
import static org.neo4j.helpers.collection.IteratorUtil.emptySetOf;
import static org.neo4j.helpers.collection.IteratorUtil.single;

public class ConstraintsCreationIT extends KernelIntegrationTest
{
    @Test
    public void shouldBeAbleToStoreAndRetrieveUniquenessConstraintRule() throws Exception
    {
        // given
        UniquenessConstraint constraint;
        {
            SchemaStatement statement = schemaStatementInNewTransaction();

            // when
            constraint = statement.uniquenessConstraintCreate( labelId, propertyKeyId );

            // then
            assertEquals( constraint, single( statement.constraintsGetForLabelAndPropertyKey( labelId,propertyKeyId ) ) );
            assertEquals( constraint, single( statement.constraintsGetForLabel(  labelId ) ) );

            // given
            commit();
        }
        {
            SchemaStatement statement = schemaStatementInNewTransaction();

            // when
            Iterator<UniquenessConstraint> constraints = statement
                    .constraintsGetForLabelAndPropertyKey( labelId, propertyKeyId );

            // then
            assertEquals( constraint, single( constraints ) );
        }
    }

    @Test
    public void shouldNotPersistUniquenessConstraintsCreatedInAbortedTransaction() throws Exception
    {
        // given
        {
            SchemaStatement statement = schemaStatementInNewTransaction();

            statement.uniquenessConstraintCreate( labelId, propertyKeyId );

            // when
            rollback();
        }
        {
            SchemaStatement statement = schemaStatementInNewTransaction();

            // then
            Iterator<UniquenessConstraint> constraints = statement.constraintsGetForLabelAndPropertyKey( labelId, propertyKeyId );
            assertFalse( "should not have any constraints", constraints.hasNext() );
        }
    }

    @Test
    public void shouldNotStoreUniquenessConstraintThatIsRemovedInTheSameTransaction() throws Exception
    {
        // given
        {
            SchemaStatement statement = schemaStatementInNewTransaction();

            UniquenessConstraint constraint = statement.uniquenessConstraintCreate( labelId, propertyKeyId );

            // when
            statement.constraintDrop( constraint );

            // then
            assertFalse( "should not have any constraints", statement.constraintsGetForLabelAndPropertyKey( labelId, propertyKeyId ).hasNext() );

            // when
            commit();
        }
        {
            SchemaStatement statement = schemaStatementInNewTransaction();

            // then
            assertFalse( "should not have any constraints", statement.constraintsGetForLabelAndPropertyKey( labelId, propertyKeyId ).hasNext() );
        }
    }

    @Test
    public void shouldNotCreateUniquenessConstraintThatAlreadyExists() throws Exception
    {
        // given
        {
            SchemaStatement statement = schemaStatementInNewTransaction();
            statement.uniquenessConstraintCreate( labelId, propertyKeyId );
            commit();
        }

        // when
        try
        {
            SchemaStatement statement = schemaStatementInNewTransaction();

            statement.uniquenessConstraintCreate( labelId, propertyKeyId );

            fail( "Should not have validated" );
        }
        // then
        catch ( AlreadyConstrainedException e )
        {
            // good
        }
    }

    @Test
    public void shouldNotRemoveConstraintThatGetsReAdded() throws Exception
    {
        // given
        UniquenessConstraint constraint;
        {
            SchemaStatement statement = schemaStatementInNewTransaction();
            constraint = statement.uniquenessConstraintCreate( labelId, propertyKeyId );
            commit();
        }
        SchemaStateCheck schemaState = new SchemaStateCheck().setUp();
        {
            SchemaStatement statement = schemaStatementInNewTransaction();

            // when
            statement.constraintDrop( constraint );
            statement.uniquenessConstraintCreate( labelId, propertyKeyId );
            commit();
        }
        {
            SchemaStatement statement = schemaStatementInNewTransaction();

            // then
            assertEquals( singletonList( constraint ), asCollection( statement.constraintsGetForLabelAndPropertyKey( labelId, propertyKeyId ) ) );
            schemaState.assertNotCleared();
        }
    }

    @Test
    public void shouldClearSchemaStateWhenConstraintIsCreated() throws Exception
    {
        // given
        SchemaStateCheck schemaState = new SchemaStateCheck().setUp();

        SchemaStatement statement = schemaStatementInNewTransaction();

        // when
        statement.uniquenessConstraintCreate( labelId, propertyKeyId );
        commit();

        // then
        schemaStatementInNewTransaction();
        schemaState.assertCleared();
    }

    @Test
    public void shouldClearSchemaStateWhenConstraintIsDropped() throws Exception
    {
        // given
        UniquenessConstraint constraint;
        SchemaStateCheck schemaState;
        {
            SchemaStatement statement = schemaStatementInNewTransaction();
            constraint = statement.uniquenessConstraintCreate( labelId, propertyKeyId );
            commit();

            schemaState = new SchemaStateCheck().setUp();
        }

        {
            SchemaStatement statement = schemaStatementInNewTransaction();

            // when
            statement.constraintDrop( constraint );
            commit();
        }

        // then
        schemaStatementInNewTransaction();
        schemaState.assertCleared();
    }

    @Test
    public void shouldCreateAnIndexToGoAlongWithAUniquenessConstraint() throws Exception
    {
        // when
        {
            SchemaStatement statement = schemaStatementInNewTransaction();
            statement.uniquenessConstraintCreate( labelId, propertyKeyId );
            commit();
        }

        // then
        {
            SchemaStatement statement = schemaStatementInNewTransaction();
            assertEquals( asSet( new IndexDescriptor( labelId, propertyKeyId ) ), asSet( statement.uniqueIndexesGetAll() ) );
        }
    }

    @Test
    public void shouldDropCreatedConstraintIndexWhenRollingBackConstraintCreation() throws Exception
    {
        // given
        {
            SchemaStatement statement = schemaStatementInNewTransaction();
            statement.uniquenessConstraintCreate( labelId, propertyKeyId );
            assertEquals( asSet( new IndexDescriptor( labelId, propertyKeyId ) ),
                          asSet( statement.uniqueIndexesGetAll() ) );
        }

        // when
        rollback();

        // then
        {
            SchemaStatement statement = schemaStatementInNewTransaction();
            assertEquals( emptySetOf( IndexDescriptor.class ), asSet( statement.uniqueIndexesGetAll() ) );
            commit();
        }
    }

    @Test
    public void shouldDropConstraintIndexWhenDroppingConstraint() throws Exception
    {
        // given
        UniquenessConstraint constraint;
        {
            SchemaStatement statement = schemaStatementInNewTransaction();
            constraint = statement.uniquenessConstraintCreate( labelId, propertyKeyId );
            assertEquals( asSet( new IndexDescriptor( labelId, propertyKeyId ) ), asSet( statement.uniqueIndexesGetAll() ) );
            commit();
        }

        // when
        {
            SchemaStatement statement = schemaStatementInNewTransaction();
            statement.constraintDrop( constraint );
            commit();
        }

        // then
        {
            SchemaStatement statement = schemaStatementInNewTransaction();
            assertEquals( emptySetOf( IndexDescriptor.class ), asSet( statement.uniqueIndexesGetAll( ) ) );
            commit();
        }
    }

    @Test
    public void shouldNotDropConstraintThatDoesNotExist() throws Exception
    {
        // when
        {
            SchemaStatement statement = schemaStatementInNewTransaction();

            try
            {
                statement.constraintDrop( new UniquenessConstraint( labelId, propertyKeyId ) );
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
            SchemaStatement statement = schemaStatementInNewTransaction();
            assertEquals( emptySetOf( IndexDescriptor.class ), asSet( statement.uniqueIndexesGetAll() ) );
            commit();
        }
    }

    @Test
    public void committedConstraintRuleShouldCrossReferenceTheCorrespondingIndexRule() throws Exception
    {
        // when
        SchemaStatement statement = schemaStatementInNewTransaction();
        statement.uniquenessConstraintCreate( labelId, propertyKeyId );
        commit();

        // then
        SchemaStorage schema = new SchemaStorage( neoStore().getSchemaStore() );
        IndexRule indexRule = schema.indexRule( labelId, propertyKeyId );
        UniquenessConstraintRule constraintRule = schema.uniquenessConstraint( labelId, propertyKeyId );
        assertEquals( constraintRule.getId(), indexRule.getOwningConstraint().longValue() );
        assertEquals( indexRule.getId(), constraintRule.getOwnedIndex() );
    }

    private long labelId, propertyKeyId;

    @Before
    public void createKeys() throws SchemaKernelException
    {
        SchemaStatement statement = schemaStatementInNewTransaction();
        this.labelId = statement.labelGetOrCreateForName( "Foo" );
        this.propertyKeyId = statement.propertyKeyGetOrCreateForName( "bar" );
        commit();
    }

    private class SchemaStateCheck implements Function<String, Integer>
    {
        int invocationCount;
        private SchemaStatement statement;

        @Override
        public Integer apply( String s )
        {
            invocationCount++;
            return Integer.parseInt( s );
        }

        public SchemaStateCheck setUp()
        {
            this.statement = schemaStatementInNewTransaction();
            checkState();
            commit();
            return this;
        }

        public void assertCleared()
        {
            int count = invocationCount;
            checkState();
            assertEquals( "schema state should have been cleared.", count + 1, invocationCount );
        }

        public void assertNotCleared()
        {
            int count = invocationCount;
            checkState();
            assertEquals( "schema state should not have been cleared.", count, invocationCount );
        }

        private SchemaStateCheck checkState()
        {
            assertEquals( Integer.valueOf( 7 ), statement.schemaStateGetOrCreate( "7", this ) );
            return this;
        }
    }
}
