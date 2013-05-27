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
import org.neo4j.kernel.api.constraints.UniquenessConstraint;
import org.neo4j.kernel.api.exceptions.schema.AlreadyConstrainedException;
import org.neo4j.kernel.api.exceptions.schema.SchemaKernelException;
import org.neo4j.kernel.impl.api.SchemaStorage;
import org.neo4j.kernel.impl.api.index.IndexDescriptor;
import org.neo4j.kernel.impl.nioneo.store.IndexRule;
import org.neo4j.kernel.impl.nioneo.store.UniquenessConstraintRule;

import static java.util.Collections.singletonList;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
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
        UniquenessConstraint constraint = statement.uniquenessConstraintCreate( label, propertyKey );

        // then
        assertEquals( constraint, single( statement.constraintsGetForLabelAndPropertyKey( label, propertyKey ) ) );
        assertEquals( constraint, single( statement.constraintsGetForLabel( label ) ) );

        // given
        commit();
        newTransaction();

        // when
        Iterator<UniquenessConstraint> constraints = statement.constraintsGetForLabelAndPropertyKey( label, propertyKey );

        // then
        assertEquals( constraint, single( constraints ) );
    }

    @Test
    public void shouldNotPersistUniquenessConstraintsCreatedInAbortedTransaction() throws Exception
    {
        // given
        newTransaction();

        statement.uniquenessConstraintCreate( label, propertyKey );

        // when
        rollback();
        newTransaction();

        // then
        Iterator<UniquenessConstraint> constraints = statement.constraintsGetForLabelAndPropertyKey( label, propertyKey );
        assertFalse( "should not have any constraints", constraints.hasNext() );
    }

    @Test
    public void shouldNotStoreUniquenessConstraintThatIsRemovedInTheSameTransaction() throws Exception
    {
        // given
        newTransaction();

        UniquenessConstraint constraint = statement.uniquenessConstraintCreate( label, propertyKey );

        // when
        statement.constraintDrop( constraint );

        // then
        assertFalse( "should not have any constraints", statement.constraintsGetForLabelAndPropertyKey( label, propertyKey ).hasNext() );

        // when
        commit();
        newTransaction();

        // then
        assertFalse( "should not have any constraints", statement.constraintsGetForLabelAndPropertyKey( label, propertyKey ).hasNext() );
    }

    @Test
    public void shouldNotCreateUniquenessConstraintThatAlreadyExists() throws Exception
    {
        // given
        newTransaction();
        statement.uniquenessConstraintCreate( label, propertyKey );
        commit();

        // when
        newTransaction();
        try
        {
            statement.uniquenessConstraintCreate( label, propertyKey );
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
        newTransaction();
        UniquenessConstraint constraint = statement.uniquenessConstraintCreate( label, propertyKey );
        commit();
        SchemaStateCheck schemaState = new SchemaStateCheck().setUp();
        newTransaction();

        // when
        statement.constraintDrop( constraint );
        statement.uniquenessConstraintCreate( label, propertyKey );
        commit();
        newTransaction();

        // then
        assertEquals( singletonList( constraint ), asCollection( statement.constraintsGetForLabelAndPropertyKey( label, propertyKey ) ) );
        schemaState.assertNotCleared();
    }

    @Test
    public void shouldClearSchemaStateWhenConstraintIsCreated() throws Exception
    {
        // given
        SchemaStateCheck schemaState = new SchemaStateCheck().setUp();

        newTransaction();

        // when
        statement.uniquenessConstraintCreate( label, propertyKey );
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
        UniquenessConstraint constraint = statement.uniquenessConstraintCreate( label, propertyKey );
        commit();

        SchemaStateCheck schemaState = new SchemaStateCheck().setUp();

        newTransaction();

        // when
        statement.constraintDrop( constraint );
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
        statement.uniquenessConstraintCreate( label, propertyKey );
        commit();

        // then
        newTransaction();
        assertEquals( asSet( new IndexDescriptor( label, propertyKey ) ), asSet( statement.uniqueIndexesGetAll() ) );
    }

    @Test
    public void shouldDropCreatedConstraintIndexWhenRollingBackConstraintCreation() throws Exception
    {
        // given
        newTransaction();
        statement.uniquenessConstraintCreate( label, propertyKey );
        assertEquals( asSet( new IndexDescriptor( label, propertyKey ) ), asSet( statement.uniqueIndexesGetAll() ) );

        // when
        rollback();

        // then
        assertEquals( emptySetOf( IndexDescriptor.class ), asSet( readOnlyContext().uniqueIndexesGetAll() ) );
    }

    @Test
    public void shouldDropConstraintIndexWhenDroppingConstraint() throws Exception
    {
        // given
        newTransaction();
        UniquenessConstraint constraint = statement.uniquenessConstraintCreate( label, propertyKey );
        assertEquals( asSet( new IndexDescriptor( label, propertyKey ) ), asSet( statement.uniqueIndexesGetAll() ) );
        commit();

        // when
        newTransaction();
        statement.constraintDrop( constraint );
        commit();

        // then
        assertEquals( emptySetOf( IndexDescriptor.class ), asSet( readOnlyContext().uniqueIndexesGetAll() ) );
    }

    @Test
    public void committedConstraintRuleShouldCrossReferenceTheCorrespondingIndexRule() throws Exception
    {
        // when
        newTransaction();
        statement.uniquenessConstraintCreate( label, propertyKey );
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
    public void createKeys() throws SchemaKernelException
    {
        newTransaction();
        this.label = statement.labelGetOrCreateForName( "Foo" );
        this.propertyKey = statement.propertyKeyGetOrCreateForName( "bar" );
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
            assertEquals( Integer.valueOf( 7 ), statement.schemaStateGetOrCreate( "7", this ) );
            return this;
        }
    }
}
