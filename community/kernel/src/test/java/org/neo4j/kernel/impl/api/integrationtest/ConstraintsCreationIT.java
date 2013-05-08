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
import org.neo4j.kernel.api.DataIntegrityKernelException;
import org.neo4j.kernel.api.constraints.UniquenessConstraint;
import org.neo4j.kernel.impl.api.SchemaStorage;
import org.neo4j.kernel.impl.api.index.IndexDescriptor;
import org.neo4j.kernel.impl.nioneo.store.IndexRule;
import org.neo4j.kernel.impl.nioneo.store.UniquenessConstraintRule;

import static java.util.Collections.singletonList;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
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
        newTransaction();

        // when
        UniquenessConstraint constraint = statement.addUniquenessConstraint( label, propertyKey );

        // then
        assertEquals( constraint, single( statement.getConstraints( label, propertyKey ) ) );
        assertEquals( constraint, single( statement.getConstraints( label ) ) );

        // given
        commit();
        newTransaction();

        // when
        Iterator<UniquenessConstraint> constraints = statement.getConstraints( label, propertyKey );

        // then
        assertEquals( constraint, single( constraints ) );
    }

    @Test
    public void shouldNotPersistUniquenessConstraintsCreatedInAbortedTransaction() throws Exception
    {
        // given
        newTransaction();

        statement.addUniquenessConstraint( label, propertyKey );

        // when
        rollback();
        newTransaction();

        // then
        Iterator<UniquenessConstraint> constraints = statement.getConstraints( label, propertyKey );
        assertFalse( "should not have any constraints", constraints.hasNext() );
    }

    @Test
    public void shouldNotStoreUniquenessConstraintThatIsRemovedInTheSameTransaction() throws Exception
    {
        // given
        newTransaction();

        UniquenessConstraint constraint = statement.addUniquenessConstraint( label, propertyKey );

        // when
        statement.dropConstraint( constraint );

        // then
        assertFalse( "should not have any constraints", statement.getConstraints( label, propertyKey ).hasNext() );

        // when
        commit();
        newTransaction();

        // then
        assertFalse( "should not have any constraints", statement.getConstraints( label, propertyKey ).hasNext() );
    }

    @Test
    public void shouldNotCreateUniquenessConstraintThatAlreadyExists() throws Exception
    {
        // given
        newTransaction();
        statement.addUniquenessConstraint( label, propertyKey );
        commit();

        // when
        newTransaction();
        try
        {
            statement.addUniquenessConstraint( label, propertyKey );
            fail( "Should not have validated" );
        }
        // then
        catch ( DataIntegrityKernelException e )
        {
            String message = e.getMessage();
            assertTrue( message.contains( "already has a uniqueness constraint" ) );
        }
    }

    @Test
    public void shouldNotRemoveConstraintThatGetsReAdded() throws Exception
    {
        // given
        newTransaction();
        UniquenessConstraint constraint = statement.addUniquenessConstraint( label, propertyKey );
        commit();
        SchemaStateCheck schemaState = new SchemaStateCheck().setUp();
        newTransaction();

        // when
        statement.dropConstraint( constraint );
        statement.addUniquenessConstraint( label, propertyKey );
        commit();
        newTransaction();

        // then
        assertEquals( singletonList( constraint ), asCollection( statement.getConstraints( label, propertyKey ) ) );
        schemaState.assertNotCleared();
    }

    @Test
    public void shouldClearSchemaStateWhenConstraintIsCreated() throws Exception
    {
        // given
        SchemaStateCheck schemaState = new SchemaStateCheck().setUp();

        newTransaction();

        // when
        statement.addUniquenessConstraint( label, propertyKey );
        commit();

        // then
        newTransaction();
        schemaState.assertCleared();
    }

    @Test
    public void shouldClearSchemaStateWhenConstraintIsDropped() throws Exception
    {
        // given
        newTransaction();
        UniquenessConstraint constraint = statement.addUniquenessConstraint( label, propertyKey );
        commit();

        SchemaStateCheck schemaState = new SchemaStateCheck().setUp();

        newTransaction();

        // when
        statement.dropConstraint( constraint );
        commit();

        // then
        newTransaction();
        schemaState.assertCleared();
    }

    @Test
    public void shouldCreateAnIndexToGoAlongWithAUniquenessConstraint() throws Exception
    {
        // when
        newTransaction();
        statement.addUniquenessConstraint( label, propertyKey );
        commit();

        // then
        newTransaction();
        assertEquals( asSet( new IndexDescriptor( label, propertyKey ) ), asSet( statement.getConstraintIndexes() ) );
    }

    @Test
    public void shouldDropCreatedConstraintIndexWhenRollingBackConstraintCreation() throws Exception
    {
        // given
        newTransaction();
        statement.addUniquenessConstraint( label, propertyKey );
        assertEquals( asSet( new IndexDescriptor( label, propertyKey ) ), asSet( statement.getConstraintIndexes() ) );

        // when
        rollback();

        // then
        assertEquals( emptySetOf( IndexDescriptor.class ), asSet( readOnlyContext().getConstraintIndexes() ) );
    }

    @Test
    public void shouldDropConstraintIndexWhenDroppingConstraint() throws Exception
    {
        // given
        newTransaction();
        UniquenessConstraint constraint = statement.addUniquenessConstraint( label, propertyKey );
        assertEquals( asSet( new IndexDescriptor( label, propertyKey ) ), asSet( statement.getConstraintIndexes() ) );
        commit();

        // when
        newTransaction();
        statement.dropConstraint( constraint );
        commit();

        // then
        assertEquals( emptySetOf( IndexDescriptor.class ), asSet( readOnlyContext().getConstraintIndexes() ) );
    }

    @Test
    public void committedConstraintRuleShouldCrossReferenceTheCorrespondingIndexRule() throws Exception
    {
        // when
        newTransaction();
        statement.addUniquenessConstraint( label, propertyKey );
        commit();

        // then
        SchemaStorage schema = new SchemaStorage( neoStore().getSchemaStore() );
        IndexRule indexRule = schema.indexRule( label, propertyKey );
        UniquenessConstraintRule constraintRule = schema.uniquenessConstraint( label, propertyKey );
        assertEquals( constraintRule.getId(), indexRule.getOwningConstraint().longValue() );
        assertEquals( indexRule.getId(), constraintRule.getOwnedIndex() );
    }

    private long label, propertyKey;

    @Before
    public void createKeys() throws DataIntegrityKernelException
    {
        newTransaction();
        this.label = statement.getOrCreateLabelId( "Foo" );
        this.propertyKey = statement.getOrCreatePropertyKeyId( "bar" );
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

        public SchemaStateCheck setUp()
        {
            newTransaction();
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
            assertEquals( Integer.valueOf( 7 ), statement.getOrCreateFromSchemaState( "7", this ) );
            return this;
        }
    }
}
