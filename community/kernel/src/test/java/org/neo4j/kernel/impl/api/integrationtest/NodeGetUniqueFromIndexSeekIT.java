/*
 * Copyright (c) 2002-2018 "Neo Technology,"
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

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import org.neo4j.graphdb.Transaction;
import org.neo4j.internal.kernel.api.exceptions.KernelException;
import org.neo4j.internal.kernel.api.schema.LabelSchemaDescriptor;
import org.neo4j.internal.kernel.api.security.LoginContext;
import org.neo4j.kernel.api.DataWriteOperations;
import org.neo4j.kernel.api.ReadOperations;
import org.neo4j.kernel.api.Statement;
import org.neo4j.kernel.api.TokenWriteOperations;
import org.neo4j.kernel.api.exceptions.index.IndexNotApplicableKernelException;
import org.neo4j.kernel.api.exceptions.index.IndexNotFoundKernelException;
import org.neo4j.kernel.api.exceptions.schema.IndexBrokenKernelException;
import org.neo4j.kernel.api.schema.SchemaDescriptorFactory;
import org.neo4j.kernel.api.schema.index.IndexDescriptor;
import org.neo4j.test.DoubleLatch;
import org.neo4j.values.storable.Value;

import static java.lang.Thread.State.TIMED_WAITING;
import static java.lang.Thread.State.WAITING;
import static java.lang.Thread.yield;
import static java.time.Duration.ofMillis;
import static org.junit.jupiter.api.Assertions.assertTimeout;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.neo4j.internal.kernel.api.IndexQuery.exact;
import static org.neo4j.internal.kernel.api.security.SecurityContext.AUTH_DISABLED;
import static org.neo4j.kernel.api.StatementConstants.NO_SUCH_NODE;
import static org.neo4j.kernel.api.schema.SchemaDescriptorFactory.forLabel;
import static org.neo4j.values.storable.Values.of;

public class NodeGetUniqueFromIndexSeekIT extends KernelIntegrationTest
{
    private int labelId;
    private int propertyId1;
    private int propertyId2;

    @BeforeEach
    public void createKeys() throws Exception
    {
        TokenWriteOperations tokenWriteOperations = tokenWriteOperationsInNewTransaction();
        this.labelId = tokenWriteOperations.labelGetOrCreateForName( "Person" );
        this.propertyId1 = tokenWriteOperations.propertyKeyGetOrCreateForName( "foo" );
        this.propertyId2 = tokenWriteOperations.propertyKeyGetOrCreateForName( "bar" );
        commit();
    }

    // nodeGetUniqueWithLabelAndProperty(statement, :Person, foo=val)
    //
    // Given we have a unique constraint on :Person(foo)
    // (If not, throw)
    //
    // If there is a node n with n:Person and n.foo == val, return it
    // If there is no such node, return ?
    //
    // Ensure that if that method is called again with the same argument from some other transaction,
    // that transaction blocks until this transaction has finished
    //

    // [X] must return node from the unique index with the given property
    // [X] must return NO_SUCH_NODE if it is not in the index for the given property
    //
    // must block other transactions that try to call it with the same arguments

    @Test
    public void shouldFindMatchingNode() throws Exception
    {
        // given
        IndexDescriptor index = createUniquenessConstraint( labelId, propertyId1 );
        Value value = of( "value" );
        long nodeId = createNodeWithValue( value );

        // when looking for it
        ReadOperations readOperations = readOperationsInNewTransaction();
        int propertyId = index.schema().getPropertyId();
        long foundId = readOperations.nodeGetFromUniqueIndexSeek( index, exact( propertyId, value ) );
        commit();

        // then
        assertTrue( nodeId == foundId, "Created node was not found" );
    }

    @Test
    public void shouldNotFindNonMatchingNode() throws Exception
    {
        // given
        IndexDescriptor index = createUniquenessConstraint( labelId, propertyId1 );
        Value value = of( "value" );
        createNodeWithValue( of( "other_" + value ) );

        // when looking for it
        ReadOperations readOperations = readOperationsInNewTransaction();
        long foundId = readOperations.nodeGetFromUniqueIndexSeek( index, exact( propertyId1, value ) );
        commit();

        // then
        assertTrue( isNoSuchNode( foundId ), "Non-matching created node was found" );
    }

    @Test
    public void shouldCompositeFindMatchingNode() throws Exception
    {
        // given
        IndexDescriptor index = createUniquenessConstraint( labelId, propertyId1, propertyId2 );
        Value value1 = of( "value1" );
        Value value2 = of( "value2" );
        long nodeId = createNodeWithValues( value1, value2 );

        // when looking for it
        ReadOperations readOperations = readOperationsInNewTransaction();
        long foundId = readOperations
                .nodeGetFromUniqueIndexSeek( index, exact( propertyId1, value1 ), exact( propertyId2, value2 ) );
        commit();

        // then
        assertTrue( nodeId == foundId, "Created node was not found" );
    }

    @Test
    public void shouldNotCompositeFindNonMatchingNode() throws Exception
    {
        // given
        IndexDescriptor index = createUniquenessConstraint( labelId, propertyId1, propertyId2 );
        Value value1 = of( "value1" );
        Value value2 = of( "value2" );
        createNodeWithValues( of( "other_" + value1 ), of( "other_" + value2 ) );

        // when looking for it
        ReadOperations readOperations = readOperationsInNewTransaction();
        long foundId = readOperations
                .nodeGetFromUniqueIndexSeek( index, exact( propertyId1, value1 ), exact( propertyId2, value2 ) );
        commit();

        // then
        assertTrue( isNoSuchNode( foundId ), "Non-matching created node was found" );
    }

    @Test
    public void shouldBlockUniqueIndexSeekFromCompetingTransaction()
    {
        assertTimeout( ofMillis( 10_000 ), () -> {
            //  This is the interleaving that we are trying to verify works correctly:

            // ----------------------------------------------------------------------
            // Thread1 (main)        : Thread2
            // create unique node    :
            // lookup(node)          :
            // open start latch ----->
            //    |                  : lookup(node)
            // wait for T2 to block  :      |
            //                       :    *block*
            // commit --------------->   *unblock*
            // wait for T2 end latch :      |
            //                       : finish transaction
            //                       : open end latch
            // *unblock* <-------------‘
            // assert that we complete before timeout
            final DoubleLatch latch = new DoubleLatch();

            final IndexDescriptor index = createUniquenessConstraint( labelId, propertyId1 );
            final Value value = of( "value" );

            DataWriteOperations dataStatement = dataWriteOperationsInNewTransaction();
            long nodeId = dataStatement.nodeCreate();
            dataStatement.nodeAddLabel( nodeId, labelId );

            // This adds the node to the unique index and should take an index write lock
            dataStatement.nodeSetProperty( nodeId, propertyId1, value );

            Runnable runnableForThread2 = () -> {
                latch.waitForAllToStart();
                try ( Transaction tx = db.beginTx() )
                {
                    try ( Statement statement1 = statementContextSupplier.get() )
                    {
                        statement1.readOperations().nodeGetFromUniqueIndexSeek( index, exact( propertyId1, value ) );
                    }
                    tx.success();
                }
                catch ( IndexNotFoundKernelException | IndexNotApplicableKernelException | IndexBrokenKernelException e )
                {
                    throw new RuntimeException( e );
                }
                finally
                {
                    latch.finish();
                }
            };
            Thread thread2 = new Thread( runnableForThread2, "Transaction Thread 2" );
            thread2.start();
            latch.startAndWaitForAllToStart();

            //noinspection UnusedLabel
            spinUntilBlocking:
            for ( ; ; )
            {
                if ( thread2.getState() == TIMED_WAITING || thread2.getState() == WAITING )
                {
                    break;
                }
                yield();
            }

            commit();
            latch.waitForAllToFinish();
        } );
    }

    private boolean isNoSuchNode( long foundId )
    {
        return NO_SUCH_NODE == foundId;
    }

    private long createNodeWithValue( Value value ) throws KernelException
    {
        DataWriteOperations dataStatement = dataWriteOperationsInNewTransaction();
        long nodeId = dataStatement.nodeCreate();
        dataStatement.nodeAddLabel( nodeId, labelId );
        dataStatement.nodeSetProperty( nodeId, propertyId1, value );
        commit();
        return nodeId;
    }

    private long createNodeWithValues( Value value1, Value value2 ) throws KernelException
    {
        DataWriteOperations dataStatement = dataWriteOperationsInNewTransaction();
        long nodeId = dataStatement.nodeCreate();
        dataStatement.nodeAddLabel( nodeId, labelId );
        dataStatement.nodeSetProperty( nodeId, propertyId1, value1 );
        dataStatement.nodeSetProperty( nodeId, propertyId2, value2 );
        commit();
        return nodeId;
    }

    private IndexDescriptor createUniquenessConstraint( int labelId, int... propertyIds ) throws Exception
    {
        Statement statement = statementInNewTransaction( LoginContext.AUTH_DISABLED );
        LabelSchemaDescriptor descriptor = SchemaDescriptorFactory.forLabel( labelId, propertyIds );
        statement.schemaWriteOperations().uniquePropertyConstraintCreate( descriptor );
        IndexDescriptor result = statement.readOperations().indexGetForSchema( descriptor );
        commit();
        return result;
    }
}
