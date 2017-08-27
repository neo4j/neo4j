/*
 * Copyright (c) 2002-2017 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
 *
 * This file is part of Neo4j.
 *
 * Neo4j is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program. If not, see <http://www.gnu.org/licenses/>.
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
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.TransientTransactionFailureException;
import org.neo4j.graphdb.schema.ConstraintDefinition;
import org.neo4j.graphdb.schema.IndexDefinition;
import org.neo4j.graphdb.schema.Schema;
import org.neo4j.helpers.collection.Iterables;
import org.neo4j.kernel.api.ReadOperations;
import org.neo4j.kernel.api.SchemaWriteOperations;
import org.neo4j.kernel.api.Statement;
import org.neo4j.kernel.api.TokenWriteOperations;
import org.neo4j.kernel.api.exceptions.KernelException;
import org.neo4j.kernel.api.exceptions.Status;
import org.neo4j.kernel.api.exceptions.TransactionFailureException;
import org.neo4j.kernel.api.exceptions.schema.AlreadyConstrainedException;
import org.neo4j.kernel.api.exceptions.schema.DropConstraintFailureException;
import org.neo4j.kernel.api.exceptions.schema.NoSuchConstraintException;
import org.neo4j.kernel.api.schema.SchemaDescriptor;
import org.neo4j.kernel.api.schema.constaints.ConstraintDescriptor;
import org.neo4j.kernel.api.security.SecurityContext;
import org.neo4j.test.TestEnterpriseGraphDatabaseFactory;

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

public abstract class AbstractConstraintCreationIT<Constraint extends ConstraintDescriptor, DESCRIPTOR extends SchemaDescriptor>
        extends KernelIntegrationTest
{
    static final String KEY = "Foo";
    static final String PROP = "bar";

    int typeId;
    int propertyKeyId;
    DESCRIPTOR descriptor;

    abstract int initializeLabelOrRelType( TokenWriteOperations tokenWriteOperations, String name )
            throws KernelException;

    abstract Constraint createConstraint( SchemaWriteOperations writeOps, DESCRIPTOR descriptor ) throws Exception;

    abstract void createConstraintInRunningTx( GraphDatabaseService db, String type, String property );

    abstract Constraint newConstraintObject( DESCRIPTOR descriptor );

    abstract void dropConstraint( SchemaWriteOperations writeOps, Constraint constraint ) throws Exception;

    abstract void createOffendingDataInRunningTx( GraphDatabaseService db );

    abstract void removeOffendingDataInRunningTx( GraphDatabaseService db );

    abstract DESCRIPTOR makeDescriptor( int typeId, int propertyKeyId );

    @Before
    public void createKeys() throws Exception
    {
        TokenWriteOperations tokenWriteOperations = tokenWriteOperationsInNewTransaction();
        this.typeId = initializeLabelOrRelType( tokenWriteOperations, KEY );
        this.propertyKeyId = tokenWriteOperations.propertyKeyGetOrCreateForName( PROP );
        this.descriptor = makeDescriptor( typeId, propertyKeyId );
        commit();
    }

    @Override
    protected GraphDatabaseService createGraphDatabase()
    {
        return new TestEnterpriseGraphDatabaseFactory().setFileSystem( fileSystemRule.get() )
                .newEmbeddedDatabase( testDir.graphDbDir() );
    }

    @Test
    public void shouldBeAbleToStoreAndRetrieveConstraint() throws Exception
    {
        // given
        Statement statement = statementInNewTransaction( SecurityContext.AUTH_DISABLED );

        // when
        ConstraintDescriptor constraint = createConstraint( statement.schemaWriteOperations(), descriptor );

        // then
        assertEquals( constraint, single( statement.readOperations().constraintsGetAll() ) );

        // given
        commit();
        ReadOperations readOperations = readOperationsInNewTransaction();

        // when
        Iterator<?> constraints = readOperations.constraintsGetAll();

        // then
        assertEquals( constraint, single( constraints ) );
        commit();
    }

    @Test
    public void shouldBeAbleToStoreAndRetrieveConstraintAfterRestart() throws Exception
    {
        // given
        Statement statement = statementInNewTransaction( SecurityContext.AUTH_DISABLED );

        // when
        ConstraintDescriptor constraint = createConstraint( statement.schemaWriteOperations(), descriptor );

        // then
        assertEquals( constraint, single( statement.readOperations().constraintsGetAll() ) );

        // given
        commit();
        restartDb();
        ReadOperations readOperations = readOperationsInNewTransaction();

        // when
        Iterator<?> constraints = readOperations.constraintsGetAll();

        // then
        assertEquals( constraint, single( constraints ) );

        commit();
    }

    @Test
    public void shouldNotPersistConstraintCreatedInAbortedTransaction() throws Exception
    {
        // given
        SchemaWriteOperations schemaWriteOperations = schemaWriteOperationsInNewTransaction();

        createConstraint( schemaWriteOperations, descriptor );

        // when
        rollback();

        ReadOperations readOperations = readOperationsInNewTransaction();

        // then
        Iterator<?> constraints = readOperations.constraintsGetAll();
        assertFalse( "should not have any constraints", constraints.hasNext() );
        commit();
    }

    @Test
    public void shouldNotStoreConstraintThatIsRemovedInTheSameTransaction() throws Exception
    {
        // given
        try ( Statement statement = statementInNewTransaction( SecurityContext.AUTH_DISABLED ) )
        {

            Constraint constraint = createConstraint( statement.schemaWriteOperations(), descriptor );

            // when
            dropConstraint( statement.schemaWriteOperations(), constraint );

            // then
            assertFalse( "should not have any constraints", statement.readOperations().constraintsGetAll().hasNext() );
        }

        // when
        commit();

        ReadOperations readOperations = readOperationsInNewTransaction();

        // then
        assertFalse( "should not have any constraints", readOperations.constraintsGetAll().hasNext() );
        commit();
    }

    @Test
    public void shouldDropConstraint() throws Exception
    {
        // given
        Constraint constraint;
        {
            SchemaWriteOperations statement = schemaWriteOperationsInNewTransaction();
            constraint = createConstraint( statement, descriptor );
            commit();
        }

        // when
        {
            SchemaWriteOperations statement = schemaWriteOperationsInNewTransaction();
            dropConstraint( statement, constraint );
            commit();
        }

        // then
        {
            ReadOperations statement = readOperationsInNewTransaction();

            // then
            assertFalse( "should not have any constraints", statement.constraintsGetAll().hasNext() );
            commit();
        }
    }

    @Test
    public void shouldNotCreateConstraintThatAlreadyExists() throws Exception
    {
        // given
        {
            SchemaWriteOperations statement = schemaWriteOperationsInNewTransaction();
            createConstraint( statement, descriptor );
            commit();
        }

        // when
        try
        {
            SchemaWriteOperations statement = schemaWriteOperationsInNewTransaction();

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
            SchemaWriteOperations statement = schemaWriteOperationsInNewTransaction();
            constraint = createConstraint( statement, descriptor );
            commit();
        }
        try ( Transaction tx = db.beginTx() )
        {
            // Make sure all schema changes are stable, to avoid any synchronous schema state invalidation
            db.schema().awaitIndexesOnline( 10, TimeUnit.SECONDS );
        }
        SchemaStateCheck schemaState = new SchemaStateCheck().setUp();
        {
            SchemaWriteOperations statement = schemaWriteOperationsInNewTransaction();

            // when
            dropConstraint( statement, constraint );
            createConstraint( statement, descriptor );
            commit();
        }
        {
            ReadOperations statement = readOperationsInNewTransaction();

            // then
            assertEquals( singletonList( constraint ), asCollection( statement.constraintsGetAll() ) );
            schemaState.assertNotCleared( statement );
            commit();
        }
    }

    @Test
    public void shouldClearSchemaStateWhenConstraintIsCreated() throws Exception
    {
        // given
        SchemaStateCheck schemaState = new SchemaStateCheck().setUp();

        SchemaWriteOperations statement = schemaWriteOperationsInNewTransaction();

        // when
        createConstraint( statement, descriptor );
        commit();

        // then
        schemaState.assertCleared( readOperationsInNewTransaction() );
        rollback();
    }

    @Test
    public void shouldClearSchemaStateWhenConstraintIsDropped() throws Exception
    {
        // given
        Constraint constraint;
        SchemaStateCheck schemaState;
        {
            SchemaWriteOperations statement = schemaWriteOperationsInNewTransaction();
            constraint = createConstraint( statement, descriptor );
            commit();

            schemaState = new SchemaStateCheck().setUp();
        }

        {
            SchemaWriteOperations statement = schemaWriteOperationsInNewTransaction();

            // when
            dropConstraint( statement, constraint );
            commit();
        }

        // then
        schemaState.assertCleared( readOperationsInNewTransaction() );
        rollback();
    }

    @Test
    public void shouldNotDropConstraintThatDoesNotExist() throws Exception
    {
        // given
        Constraint constraint = newConstraintObject( descriptor );

        // when
        {
            SchemaWriteOperations statement = schemaWriteOperationsInNewTransaction();

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
            ReadOperations statement = readOperationsInNewTransaction();
            assertEquals( emptySet(), asSet( statement.indexesGetAll() ) );
            commit();
        }
    }

    @Test
    public void shouldNotLeaveAnyStateBehindAfterFailingToCreateConstraint() throws Exception
    {
        // given
        try ( Transaction tx = db.beginTx() )
        {
            createOffendingDataInRunningTx( db );
            tx.success();
        }

        // when
        try ( Transaction tx = db.beginTx() )
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
        try ( Transaction tx = db.beginTx() )
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
        try ( Transaction tx = db.beginTx() )
        {
            createOffendingDataInRunningTx( db );
            tx.success();
        }

        // when
        try ( Transaction tx = db.beginTx() )
        {
            createConstraintInRunningTx( db, KEY, PROP );

            tx.success();
            fail( "expected failure" );
        }
        catch ( QueryExecutionException e )
        {
            assertThat( e.getMessage(), startsWith( "Unable to create CONSTRAINT" ) );
        }

        try ( Transaction tx = db.beginTx() )
        {
            removeOffendingDataInRunningTx( db );
            tx.success();
        }

        // then - this should not fail
        SchemaWriteOperations statement = schemaWriteOperationsInNewTransaction();
        createConstraint( statement, descriptor );
        commit();
    }

    @Test
    public void changedConstraintsShouldResultInTransientFailure() throws InterruptedException
    {
        // Given
        Runnable constraintCreation = () ->
        {
            try ( Transaction tx = db.beginTx() )
            {
                createConstraintInRunningTx( db, KEY, PROP );
                tx.success();
            }
        };

        // When
        try
        {
            try ( Transaction tx = db.beginTx() )
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
            ReadOperations readOperations = readOperationsInNewTransaction();
            checkState( readOperations );
            commit();
            return this;
        }

        void assertCleared( ReadOperations readOperations )
        {
            int count = invocationCount;
            checkState( readOperations );
            assertEquals( "schema state should have been cleared.", count + 1, invocationCount );
        }

        void assertNotCleared( ReadOperations readOperations )
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
