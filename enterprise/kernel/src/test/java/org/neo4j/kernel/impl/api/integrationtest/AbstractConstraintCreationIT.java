/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 *
 * This file is part of Neo4j Enterprise Edition. The included source
 * code can be redistributed and/or modified under the terms of the
 * GNU AFFERO GENERAL PUBLIC LICENSE Version 3
 * (http://www.fsf.org/licensing/licenses/agpl-3.0.html) with the
 * Commons Clause, as found in the associated LICENSE.txt file.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * Neo4j object code can be licensed independently from the source
 * under separate terms from the AGPL. Inquiries can be directed to:
 * licensing@neo4j.com
 *
 * More information is also available at:
 * https://neo4j.com/licensing/
 */
package org.neo4j.kernel.impl.api.integrationtest;

import org.junit.Before;
import org.junit.Test;

import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.QueryExecutionException;
import org.neo4j.graphdb.TransientTransactionFailureException;
import org.neo4j.graphdb.schema.ConstraintDefinition;
import org.neo4j.graphdb.schema.IndexDefinition;
import org.neo4j.graphdb.schema.Schema;
import org.neo4j.helpers.collection.Iterables;
import org.neo4j.internal.kernel.api.SchemaWrite;
import org.neo4j.internal.kernel.api.TokenWrite;
import org.neo4j.internal.kernel.api.Transaction;
import org.neo4j.internal.kernel.api.exceptions.KernelException;
import org.neo4j.internal.kernel.api.exceptions.TransactionFailureException;
import org.neo4j.internal.kernel.api.schema.SchemaDescriptor;
import org.neo4j.internal.kernel.api.schema.constraints.ConstraintDescriptor;
import org.neo4j.kernel.api.exceptions.Status;
import org.neo4j.kernel.api.exceptions.schema.AlreadyConstrainedException;
import org.neo4j.kernel.api.exceptions.schema.DropConstraintFailureException;
import org.neo4j.kernel.api.exceptions.schema.NoSuchConstraintException;
import org.neo4j.test.TestEnterpriseGraphDatabaseFactory;
import org.neo4j.test.TestGraphDatabaseFactory;

import static java.util.Collections.emptySet;
import static java.util.Collections.singletonList;
import static org.hamcrest.CoreMatchers.instanceOf;
import static org.hamcrest.CoreMatchers.startsWith;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;
import static org.neo4j.helpers.collection.Iterators.asCollection;
import static org.neo4j.helpers.collection.Iterators.asSet;
import static org.neo4j.helpers.collection.Iterators.single;
import static org.neo4j.internal.kernel.api.security.LoginContext.AUTH_DISABLED;

public abstract class AbstractConstraintCreationIT<Constraint extends ConstraintDescriptor, DESCRIPTOR extends SchemaDescriptor>
        extends KernelIntegrationTest
{
    static final String KEY = "Foo";
    static final String PROP = "bar";

    int typeId;
    int propertyKeyId;
    DESCRIPTOR descriptor;

    abstract int initializeLabelOrRelType( TokenWrite tokenWrite, String name )
            throws KernelException;

    abstract Constraint createConstraint( SchemaWrite writeOps, DESCRIPTOR descriptor ) throws Exception;

    abstract void createConstraintInRunningTx( GraphDatabaseService db, String type, String property );

    abstract Constraint newConstraintObject( DESCRIPTOR descriptor );

    abstract void dropConstraint( SchemaWrite writeOps, Constraint constraint ) throws Exception;

    abstract void createOffendingDataInRunningTx( GraphDatabaseService db );

    abstract void removeOffendingDataInRunningTx( GraphDatabaseService db );

    abstract DESCRIPTOR makeDescriptor( int typeId, int propertyKeyId );

    @Before
    public void createKeys() throws Exception
    {
        TokenWrite tokenWrite = tokenWriteInNewTransaction();
        this.typeId = initializeLabelOrRelType( tokenWrite, KEY );
        this.propertyKeyId = tokenWrite.propertyKeyGetOrCreateForName( PROP );
        this.descriptor = makeDescriptor( typeId, propertyKeyId );
        commit();
    }

    @Override
    protected TestGraphDatabaseFactory createGraphDatabaseFactory()
    {
        return new TestEnterpriseGraphDatabaseFactory();
    }

    @Test
    public void shouldBeAbleToStoreAndRetrieveConstraint() throws Exception
    {
        // given
        Transaction transaction = newTransaction( AUTH_DISABLED );

        // when
        ConstraintDescriptor constraint = createConstraint( transaction.schemaWrite(), descriptor );

        // then
        assertEquals( constraint, single( transaction.schemaRead().constraintsGetAll() ) );

        // given
        commit();
        transaction = newTransaction();

        // when
        Iterator<?> constraints = transaction.schemaRead().constraintsGetAll();

        // then
        assertEquals( constraint, single( constraints ) );
        commit();
    }

    @Test
    public void shouldBeAbleToStoreAndRetrieveConstraintAfterRestart() throws Exception
    {
        // given
        Transaction transaction = newTransaction( AUTH_DISABLED );

        // when
        ConstraintDescriptor constraint = createConstraint( transaction.schemaWrite(), descriptor );

        // then
        assertEquals( constraint, single( transaction.schemaRead().constraintsGetAll() ) );

        // given
        commit();
        restartDb();
        transaction = newTransaction();

        // when
        Iterator<?> constraints = transaction.schemaRead().constraintsGetAll();

        // then
        assertEquals( constraint, single( constraints ) );

        commit();
    }

    @Test
    public void shouldNotPersistConstraintCreatedInAbortedTransaction() throws Exception
    {
        // given
        SchemaWrite schemaWriteOperations = schemaWriteInNewTransaction();

        createConstraint( schemaWriteOperations, descriptor );

        // when
        rollback();

       Transaction transaction = newTransaction();

        // then
        Iterator<?> constraints = transaction.schemaRead().constraintsGetAll();
        assertFalse( "should not have any constraints", constraints.hasNext() );
        commit();
    }

    @Test
    public void shouldNotStoreConstraintThatIsRemovedInTheSameTransaction() throws Exception
    {
        // given
        Transaction transaction = newTransaction( AUTH_DISABLED );

        Constraint constraint = createConstraint( transaction.schemaWrite(), descriptor );

        // when
        dropConstraint( transaction.schemaWrite(), constraint );

        // then
        assertFalse( "should not have any constraints", transaction.schemaRead().constraintsGetAll().hasNext() );

        // when
        commit();

       transaction = newTransaction();

        // then
        assertFalse( "should not have any constraints", transaction.schemaRead().constraintsGetAll().hasNext() );
        commit();
    }

    @Test
    public void shouldDropConstraint() throws Exception
    {
        // given
        Constraint constraint;
        {
            SchemaWrite statement = schemaWriteInNewTransaction();
            constraint = createConstraint( statement, descriptor );
            commit();
        }

        // when
        {
            SchemaWrite statement = schemaWriteInNewTransaction();
            dropConstraint( statement, constraint );
            commit();
        }

        // then
        {
            Transaction transaction = newTransaction();

            // then
            assertFalse( "should not have any constraints", transaction.schemaRead().constraintsGetAll().hasNext() );
            commit();
        }
    }

    @Test
    public void shouldNotCreateConstraintThatAlreadyExists() throws Exception
    {
        // given
        {
            SchemaWrite statement = schemaWriteInNewTransaction();
            createConstraint( statement, descriptor );
            commit();
        }

        // when
        try
        {
            SchemaWrite statement = schemaWriteInNewTransaction();

            createConstraint( statement, descriptor );

            fail( "Should not have validated" );
        }
        // then
        catch ( AlreadyConstrainedException e )
        {
            // good
        }
        commit();
    }

    @Test
    public void shouldNotRemoveConstraintThatGetsReAdded() throws Exception
    {
        // given
        Constraint constraint;
        {
            SchemaWrite statement = schemaWriteInNewTransaction();
            constraint = createConstraint( statement, descriptor );
            commit();
        }
        try ( org.neo4j.graphdb.Transaction tx = db.beginTx() )
        {
            // Make sure all schema changes are stable, to avoid any synchronous schema state invalidation
            db.schema().awaitIndexesOnline( 10, TimeUnit.SECONDS );
        }
        SchemaStateCheck schemaState = new SchemaStateCheck().setUp();
        {
            SchemaWrite statement = schemaWriteInNewTransaction();

            // when
            dropConstraint( statement, constraint );
            createConstraint( statement, descriptor );
            commit();
        }
        {
           Transaction transaction = newTransaction();

            // then
            assertEquals( singletonList( constraint ), asCollection( transaction.schemaRead().constraintsGetAll() ) );
            schemaState.assertNotCleared( transaction );
            commit();
        }
    }

    @Test
    public void shouldClearSchemaStateWhenConstraintIsCreated() throws Exception
    {
        // given
        SchemaStateCheck schemaState = new SchemaStateCheck().setUp();

        SchemaWrite statement = schemaWriteInNewTransaction();

        // when
        createConstraint( statement, descriptor );
        commit();

        // then
        schemaState.assertCleared( newTransaction() );
        rollback();
    }

    @Test
    public void shouldClearSchemaStateWhenConstraintIsDropped() throws Exception
    {
        // given
        Constraint constraint;
        SchemaStateCheck schemaState;
        {
            SchemaWrite statement = schemaWriteInNewTransaction();
            constraint = createConstraint( statement, descriptor );
            commit();

            schemaState = new SchemaStateCheck().setUp();
        }

        {
            SchemaWrite statement = schemaWriteInNewTransaction();

            // when
            dropConstraint( statement, constraint );
            commit();
        }

        // then
        schemaState.assertCleared( newTransaction() );
        rollback();
    }

    @Test
    public void shouldNotDropConstraintThatDoesNotExist() throws Exception
    {
        // given
        Constraint constraint = newConstraintObject( descriptor );

        // when
        {
            SchemaWrite statement = schemaWriteInNewTransaction();

            try
            {
                dropConstraint( statement, constraint );
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
            Transaction transaction = newTransaction();
            assertEquals( emptySet(), asSet( transaction.schemaRead().indexesGetAll() ) );
            commit();
        }
    }

    @Test
    public void shouldNotLeaveAnyStateBehindAfterFailingToCreateConstraint()
    {
        // given
        try ( org.neo4j.graphdb.Transaction tx = db.beginTx() )
        {
            createOffendingDataInRunningTx( db );
            tx.success();
        }

        // when
        try ( org.neo4j.graphdb.Transaction tx = db.beginTx() )
        {
            createConstraintInRunningTx( db, KEY, PROP );

            tx.success();
            fail( "expected failure" );
        }
        catch ( QueryExecutionException e )
        {
            assertThat( e.getMessage(), startsWith( "Unable to create CONSTRAINT" ) );
        }

        // then
        try ( org.neo4j.graphdb.Transaction tx = db.beginTx() )
        {
            assertEquals( Collections.<ConstraintDefinition>emptyList(), Iterables.asList( db.schema().getConstraints() ) );
            assertEquals( Collections.<IndexDefinition,Schema.IndexState>emptyMap(),
                    indexesWithState( db.schema() ) );
            tx.success();
        }
    }

    @Test
    public void shouldBeAbleToResolveConflictsAndRecreateConstraintAfterFailingToCreateItDueToConflict()
            throws Exception
    {
        // given
        try ( org.neo4j.graphdb.Transaction tx = db.beginTx() )
        {
            createOffendingDataInRunningTx( db );
            tx.success();
        }

        // when
        try ( org.neo4j.graphdb.Transaction tx = db.beginTx() )
        {
            createConstraintInRunningTx( db, KEY, PROP );

            tx.success();
            fail( "expected failure" );
        }
        catch ( QueryExecutionException e )
        {
            assertThat( e.getMessage(), startsWith( "Unable to create CONSTRAINT" ) );
        }

        try ( org.neo4j.graphdb.Transaction tx = db.beginTx() )
        {
            removeOffendingDataInRunningTx( db );
            tx.success();
        }

        // then - this should not fail
        SchemaWrite statement = schemaWriteInNewTransaction();
        createConstraint( statement, descriptor );
        commit();
    }

    @Test
    public void changedConstraintsShouldResultInTransientFailure()
    {
        // Given
        Runnable constraintCreation = () ->
        {
            try ( org.neo4j.graphdb.Transaction tx = db.beginTx() )
            {
                createConstraintInRunningTx( db, KEY, PROP );
                tx.success();
            }
        };

        // When
        try
        {
            try ( org.neo4j.graphdb.Transaction tx = db.beginTx() )
            {
                Executors.newSingleThreadExecutor().submit( constraintCreation ).get();
                db.createNode();
                tx.success();
            }
            fail( "Exception expected" );
        }
        catch ( Exception e )
        {
            // Then
            assertThat( e, instanceOf( TransientTransactionFailureException.class ) );
            assertThat( e.getCause(), instanceOf( TransactionFailureException.class ) );
            TransactionFailureException cause = (TransactionFailureException) e.getCause();
            assertEquals( Status.Transaction.ConstraintsChanged, cause.status() );
        }
    }

    private static Map<IndexDefinition,Schema.IndexState> indexesWithState( Schema schema )
    {
        Map<IndexDefinition,Schema.IndexState> result = new HashMap<>();
        for ( IndexDefinition definition : schema.getIndexes() )
        {
            result.put( definition, schema.getIndexState( definition ) );
        }
        return result;
    }

    private class SchemaStateCheck implements Function<String,Integer>
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
            Transaction transaction = newTransaction();
            checkState( transaction );
            commit();
            return this;
        }

        void assertCleared( Transaction transaction )
        {
            int count = invocationCount;
            checkState( transaction );
            assertEquals( "schema state should have been cleared.", count + 1, invocationCount );
        }

        void assertNotCleared( Transaction transaction )
        {
            int count = invocationCount;
            checkState( transaction );
            assertEquals( "schema state should not have been cleared.", count, invocationCount );
        }

        private SchemaStateCheck checkState( Transaction transaction )
        {
            assertEquals( Integer.valueOf( 7 ), transaction.schemaRead().schemaStateGetOrCreate( "7", this ) );
            return this;
        }
    }
}
