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

import java.util.ArrayList;
import java.util.Collection;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import javax.transaction.HeuristicRollbackException;
import javax.transaction.xa.XAException;

import org.junit.Rule;
import org.junit.Test;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.TransactionFailureException;
import org.neo4j.kernel.GraphDatabaseAPI;
import org.neo4j.kernel.api.DataWriteOperations;
import org.neo4j.kernel.api.SchemaWriteOperations;
import org.neo4j.kernel.api.constraints.UniquenessConstraint;
import org.neo4j.kernel.api.exceptions.schema.CreateConstraintFailureException;
import org.neo4j.kernel.api.index.IndexConfiguration;
import org.neo4j.kernel.api.index.IndexPopulator;
import org.neo4j.kernel.api.index.PreexistingIndexEntryConflictException;
import org.neo4j.kernel.extension.KernelExtensionFactory;
import org.neo4j.kernel.impl.api.constraints.ConstraintVerificationFailedKernelException;
import org.neo4j.kernel.impl.api.index.SwallowingIndexUpdater;
import org.neo4j.kernel.impl.util.TestIndexProviderFactory;
import org.neo4j.test.OtherThreadRule;
import org.neo4j.test.TargetDirectory;
import org.neo4j.test.TestGraphDatabaseFactory;
import org.neo4j.tooling.GlobalGraphOperations;

import static org.hamcrest.CoreMatchers.instanceOf;
import static org.hamcrest.core.IsEqual.equalTo;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyLong;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;
import static org.powermock.api.mockito.PowerMockito.mock;

import static org.neo4j.graphdb.DynamicLabel.label;
import static org.neo4j.helpers.collection.Iterables.count;
import static org.neo4j.helpers.collection.IteratorUtil.asSet;
import static org.neo4j.kernel.impl.api.integrationtest.UniquenessConstraintValidationConcurrencyIT.createNode;

public class UniquenessConstraintCreationIT extends KernelIntegrationTest
{

    public @Rule
    TargetDirectory.TestDirectory testDir = TargetDirectory.cleanTestDirForTest( getClass() );

    public @Rule
    OtherThreadRule<Void> otherThread = new OtherThreadRule<>();

    @Test
    public void shouldAbortConstraintCreationWhenDuplicatesExist() throws Exception
    {
        // given
        long node1, node2;
        int foo, name;
        {
            DataWriteOperations statement = dataWriteOperationsInNewTransaction();
            // name is not unique for Foo in the existing data
            Node node = db.createNode( label( "Foo" ) );
            node1 = node.getId();
            node.setProperty( "name", "foo" );
            node = db.createNode( label( "Foo" ) );
            node2 = node.getId();
            node.setProperty( "name", "foo" );
            foo = statement.labelGetForName( "Foo" );
            name = statement.propertyKeyGetForName( "name" );
            commit();
        }

        // when
        try
        {
            SchemaWriteOperations statement = schemaWriteOperationsInNewTransaction();
            statement.uniquenessConstraintCreate( foo, name );

            fail( "expected exception" );
        }
        // then
        catch ( CreateConstraintFailureException ex )
        {
            assertEquals( new UniquenessConstraint( foo, name ), ex.constraint() );
            Throwable cause = ex.getCause();
            assertThat( cause, instanceOf( ConstraintVerificationFailedKernelException.class ) );
            assertEquals( asSet( new ConstraintVerificationFailedKernelException.Evidence(
                    new PreexistingIndexEntryConflictException( "foo", node1, node2 ) ) ),
                    ((ConstraintVerificationFailedKernelException) cause).evidence() );
        }
    }

    @Test
    public void shouldFailOnCommitIfConstraintIsBrokenAfterConstraintAddedButBeforeConstraintCommitted()
            throws Exception
    {
        // given
        long node1;
        int foo, name;
        {
            DataWriteOperations statement = dataWriteOperationsInNewTransaction();
            Node node = db.createNode( label( "Foo" ) );
            node1 = node.getId();
            node.setProperty( "name", "foo" );
            foo = statement.labelGetForName( "Foo" );
            name = statement.propertyKeyGetForName( "name" );
            commit();
        }

        SchemaWriteOperations statement = schemaWriteOperationsInNewTransaction();
        statement.uniquenessConstraintCreate(  foo, name );

        ExecutorService executor = Executors.newSingleThreadExecutor();
        long node2 = executor.submit( new Callable<Long>()
        {
            @Override
            public Long call()
            {
                try ( Transaction tx = db.beginTx() )
                {
                    Node node = db.createNode( label( "Foo" ) );
                    node.setProperty( "name", "foo" );
                    tx.success();
                    return node.getId();
                }
            }
        } ).get();
        executor.shutdown();

        // when
        try
        {
            commit();

            fail( "expected exception" );
        }
        // then
        catch ( TransactionFailureException ex )
        {
            HeuristicRollbackException rollbackEx = (HeuristicRollbackException)ex.getCause();

            XAException xaEx = (XAException)rollbackEx.getCause();
            assertThat( xaEx.errorCode, equalTo( XAException.XA_RBINTEGRITY ));

            ConstraintVerificationFailedKernelException verificationEx = (ConstraintVerificationFailedKernelException) xaEx.getCause();

            assertEquals( asSet( new ConstraintVerificationFailedKernelException.Evidence(
                    new PreexistingIndexEntryConflictException( "foo", node1, node2 ) ) ),
                    verificationEx.evidence() );
        }
    }

    @Test
    public void shouldNotBlockOtherWritersWhilePopulatingConstraintIndex()
            throws Exception
    {
        // This tests that when background population is started for a constraint index, a separate thread can still
        // perform writes to the database. This test would deadlock if this were not the case.

        // given
        TestIndexProviderFactory.TestIndexProvider provider = spy(new TestIndexProviderFactory.TestIndexProvider());

        Collection<KernelExtensionFactory<?>> extensions = new ArrayList<>();
        extensions.add( new TestIndexProviderFactory(provider) );

        final GraphDatabaseAPI gdb = (GraphDatabaseAPI) new TestGraphDatabaseFactory()
                .addKernelExtensions( extensions )
                .newEmbeddedDatabase( testDir.absolutePath() );

        try
        {
            IndexPopulator populator = mock(IndexPopulator.class);
            doAnswer( triggerConcurrentNodeCreation( gdb ) ).when( populator ).create();
            when( populator.newPopulatingUpdater() ).thenReturn( new SwallowingIndexUpdater() );

            when( provider.getPopulator( anyLong(), any( IndexConfiguration.class ) ) ).thenReturn( populator );

            // when
            try( Transaction tx = gdb.beginTx() )
            {
                gdb.schema().constraintFor( label("User") ).on( "name" ).unique().create();
                tx.success();
            }

            // then both the node I created, and the constraint, should be online
            try( Transaction ignored = gdb.beginTx() )
            {
                assertThat( count( GlobalGraphOperations.at( gdb ).getAllNodes() ), equalTo(1l));
                assertThat( count( gdb.schema().getConstraints() ), equalTo(1l));
            }
        }
        finally
        {
            gdb.shutdown();
        }
    }

    private Answer triggerConcurrentNodeCreation( final GraphDatabaseAPI gdb )
    {
        return new Answer()
        {
            @Override
            public Object answer( InvocationOnMock invocationOnMock ) throws Throwable
            {
                otherThread.execute( createNode( gdb, "User", "name", "Bob" ) ).get();
                return null;
            }
        };
    }
}
