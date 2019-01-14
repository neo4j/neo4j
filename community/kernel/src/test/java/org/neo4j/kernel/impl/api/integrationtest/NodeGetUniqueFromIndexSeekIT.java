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
package org.neo4j.kernel.impl.api.integrationtest;

import org.junit.Before;
import org.junit.Test;

import org.neo4j.internal.kernel.api.CapableIndexReference;
import org.neo4j.internal.kernel.api.Read;
import org.neo4j.internal.kernel.api.TokenWrite;
import org.neo4j.internal.kernel.api.Transaction;
import org.neo4j.internal.kernel.api.Write;
import org.neo4j.internal.kernel.api.exceptions.KernelException;
import org.neo4j.internal.kernel.api.schema.LabelSchemaDescriptor;
import org.neo4j.internal.kernel.api.security.LoginContext;
import org.neo4j.kernel.api.StatementConstants;
import org.neo4j.kernel.api.schema.SchemaDescriptorFactory;
import org.neo4j.test.DoubleLatch;
import org.neo4j.values.storable.Value;
import org.neo4j.values.storable.Values;

import static org.junit.Assert.assertTrue;
import static org.neo4j.internal.kernel.api.IndexQuery.exact;

public class NodeGetUniqueFromIndexSeekIT extends KernelIntegrationTest
{
    private int labelId;
    private int propertyId1;
    private int propertyId2;

    @Before
    public void createKeys() throws Exception
    {
        TokenWrite tokenWrite = tokenWriteInNewTransaction();
        this.labelId = tokenWrite.labelGetOrCreateForName( "Person" );
        this.propertyId1 = tokenWrite.propertyKeyGetOrCreateForName( "foo" );
        this.propertyId2 = tokenWrite.propertyKeyGetOrCreateForName( "bar" );
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
        CapableIndexReference index = createUniquenessConstraint( labelId, propertyId1 );
        Value value = Values.of( "value" );
        long nodeId = createNodeWithValue( value );

        // when looking for it
        Read read = newTransaction().dataRead();
        int propertyId = index.properties()[0];
        long foundId = read.lockingNodeUniqueIndexSeek( index, exact( propertyId, value ) );
        commit();

        // then
        assertTrue( "Created node was not found", nodeId == foundId );
    }

    @Test
    public void shouldNotFindNonMatchingNode() throws Exception
    {
        // given
        CapableIndexReference index = createUniquenessConstraint( labelId, propertyId1 );
        Value value = Values.of( "value" );
        createNodeWithValue( Values.of( "other_" + value ) );

        // when looking for it
        Transaction transaction = newTransaction();
        long foundId = transaction.dataRead().lockingNodeUniqueIndexSeek( index, exact( propertyId1, value ) );
        commit();

        // then
        assertTrue( "Non-matching created node was found", isNoSuchNode( foundId ) );
    }

    @Test
    public void shouldCompositeFindMatchingNode() throws Exception
    {
        // given
        CapableIndexReference index = createUniquenessConstraint( labelId, propertyId1, propertyId2 );
        Value value1 = Values.of( "value1" );
        Value value2 = Values.of( "value2" );
        long nodeId = createNodeWithValues( value1, value2 );

        // when looking for it
        Transaction transaction = newTransaction();
        long foundId = transaction.dataRead().lockingNodeUniqueIndexSeek( index,
                exact( propertyId1, value1 ),
                                                                exact( propertyId2, value2 ) );
        commit();

        // then
        assertTrue( "Created node was not found", nodeId == foundId );
    }

    @Test
    public void shouldNotCompositeFindNonMatchingNode() throws Exception
    {
        // given
        CapableIndexReference index = createUniquenessConstraint( labelId, propertyId1, propertyId2 );
        Value value1 = Values.of( "value1" );
        Value value2 = Values.of( "value2" );
        createNodeWithValues( Values.of( "other_" + value1 ), Values.of( "other_" + value2 ) );

        // when looking for it
        Transaction transaction = newTransaction();
        long foundId =  transaction.dataRead().lockingNodeUniqueIndexSeek( index,
                exact( propertyId1, value1 ),
                                                                exact( propertyId2, value2 ) );
        commit();

        // then
        assertTrue( "Non-matching created node was found", isNoSuchNode( foundId ) );
    }

    @Test( timeout = 10_000 )
    public void shouldBlockUniqueIndexSeekFromCompetingTransaction() throws Exception
    {
        // This is the interleaving that we are trying to verify works correctly:
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

        final CapableIndexReference index = createUniquenessConstraint( labelId, propertyId1 );
        final Value value = Values.of( "value" );

        Write write = dataWriteInNewTransaction();
        long nodeId = write.nodeCreate();
        write.nodeAddLabel( nodeId, labelId );

        // This adds the node to the unique index and should take an index write lock
        write.nodeSetProperty( nodeId, propertyId1, value );

        Runnable runnableForThread2 = () ->
        {
            latch.waitForAllToStart();
            try ( Transaction tx = session.beginTransaction() )
            {
                tx.dataRead().lockingNodeUniqueIndexSeek( index, exact( propertyId1, value ) );
                tx.success();
            }
            catch ( KernelException e )
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
        for (; ; )
        {
            if ( thread2.getState() == Thread.State.TIMED_WAITING || thread2.getState() == Thread.State.WAITING )
            {
                break;
            }
            Thread.yield();
        }

        commit();
        latch.waitForAllToFinish();
    }

    private boolean isNoSuchNode( long foundId )
    {
        return StatementConstants.NO_SUCH_NODE == foundId;
    }

    private long createNodeWithValue( Value value ) throws KernelException
    {
        Write write = dataWriteInNewTransaction();
        long nodeId = write.nodeCreate();
        write.nodeAddLabel( nodeId, labelId );
        write.nodeSetProperty( nodeId, propertyId1, value );
        commit();
        return nodeId;
    }

    private long createNodeWithValues( Value value1, Value value2 ) throws KernelException
    {
        Write write = dataWriteInNewTransaction();
        long nodeId = write.nodeCreate();
        write.nodeAddLabel( nodeId, labelId );
        write.nodeSetProperty( nodeId, propertyId1, value1 );
        write.nodeSetProperty( nodeId, propertyId2, value2 );
        commit();
        return nodeId;
    }

    private CapableIndexReference createUniquenessConstraint( int labelId, int... propertyIds ) throws Exception
    {
        Transaction transaction = newTransaction( LoginContext.AUTH_DISABLED );
        LabelSchemaDescriptor descriptor = SchemaDescriptorFactory.forLabel( labelId, propertyIds );
        transaction.schemaWrite().uniquePropertyConstraintCreate( descriptor );
        CapableIndexReference result = transaction.schemaRead().index( descriptor.getLabelId(), descriptor.getPropertyIds() );
        commit();
        return result;
    }
}
