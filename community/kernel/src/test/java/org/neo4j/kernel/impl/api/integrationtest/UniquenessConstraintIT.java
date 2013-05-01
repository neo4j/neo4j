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
import org.neo4j.kernel.api.ConstraintViolationKernelException;
import org.neo4j.kernel.api.constraints.UniquenessConstraint;

import static java.util.Collections.singleton;
import static java.util.Collections.singletonList;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.neo4j.helpers.collection.IteratorUtil.asCollection;
import static org.neo4j.helpers.collection.IteratorUtil.asSet;

public class UniquenessConstraintIT extends KernelIntegrationTest
{
    @Test
    public void shouldBeAbleToStoreAndRetrieveUniquenessConstraintRule() throws Exception
    {
        // given
        newTransaction();

        // when
        UniquenessConstraint constraint = statement.addUniquenessConstraint( label, propertyKey );

        // then
        Iterator<UniquenessConstraint> constraints = statement.getConstraints( label, propertyKey );
        assertTrue( "should have at least one constraint", constraints.hasNext() );
        assertEquals( "should have the constraint we created", constraint, constraints.next() );
        assertFalse( "should have no more than one constraint", constraints.hasNext() );

        // given
        commit();
        newTransaction();

        // when
        constraints = statement.getConstraints( label, propertyKey );

        // then
        assertTrue( "should have at least one constraint", constraints.hasNext() );
        assertEquals( "should have the constraint we created", constraint, constraints.next() );
        assertFalse( "should have no more than one constraint", constraints.hasNext() );
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
        SchemaStateCheck schemaState = new SchemaStateCheck().setUp();

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
        schemaState.assertNotCleared();
    }

    @Test
    public void shouldNotCreateUniquenessConstraintThatAlreadyExists() throws Exception
    {
        // given
        newTransaction();
        statement.addUniquenessConstraint( label, propertyKey );
        commit();
        newTransaction();

        // when
        UniquenessConstraint constraint = statement.addUniquenessConstraint( label, propertyKey );

        // then
        assertEquals( singleton( constraint ), asSet( statement.getConstraints( label, propertyKey ) ) );

        // when
        commit();
        newTransaction();

        // then
        assertEquals( singletonList( constraint ), asCollection( statement.getConstraints( label, propertyKey ) ) );
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

    private long label, propertyKey;

    @Before
    public void createKeys() throws ConstraintViolationKernelException
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
