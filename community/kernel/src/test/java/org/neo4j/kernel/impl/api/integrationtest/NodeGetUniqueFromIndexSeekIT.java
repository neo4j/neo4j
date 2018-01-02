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

import org.junit.Before;
import org.junit.Test;
import org.neo4j.graphdb.DependencyResolver;
import org.neo4j.graphdb.Transaction;
import org.neo4j.kernel.api.DataWriteOperations;
import org.neo4j.kernel.api.SchemaWriteOperations;
import org.neo4j.kernel.api.Statement;
import org.neo4j.kernel.api.StatementConstants;
import org.neo4j.kernel.api.exceptions.KernelException;
import org.neo4j.kernel.api.exceptions.index.IndexNotFoundKernelException;
import org.neo4j.kernel.api.exceptions.schema.IndexBrokenKernelException;
import org.neo4j.kernel.api.index.IndexDescriptor;
import org.neo4j.kernel.api.properties.Property;
import org.neo4j.kernel.impl.locking.Locks;
import org.neo4j.test.DoubleLatch;

import static org.junit.Assert.*;

public class NodeGetUniqueFromIndexSeekIT extends KernelIntegrationTest
{
    private int labelId, propertyKeyId;

    @Before
    public void createKeys() throws Exception
    {
        SchemaWriteOperations statement = schemaWriteOperationsInNewTransaction();
        this.labelId = statement.labelGetOrCreateForName( "Person" );
        this.propertyKeyId = statement.propertyKeyGetOrCreateForName( "foo" );
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
        IndexDescriptor index = createUniquenessConstraint();
        String value = "value";
        long nodeId = createNodeWithValue( value );

        // when looking for it
        DataWriteOperations statement = dataWriteOperationsInNewTransaction();
        long foundId = statement.nodeGetFromUniqueIndexSeek( index, value );
        commit();

        // then
        assertTrue( "Created node was not found", nodeId == foundId );
    }

    @Test
    public void shouldNotFindNonMatchingNode() throws Exception
    {
        // given
        IndexDescriptor index = createUniquenessConstraint();
        String value = "value";
        createNodeWithValue( "other_" + value );

        // when looking for it
        DataWriteOperations statement = dataWriteOperationsInNewTransaction();
        long foundId = statement.nodeGetFromUniqueIndexSeek( index, value );
        commit();

        // then
        assertTrue( "Non-matching created node was found", isNoSuchNode( foundId ) );
    }

    @Test(timeout = 1000)
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
        //                       : finish failed transaction
        //                       : open end latch
        // *unblock* <-------------‘
        // assert that we complete before timeout
        final DoubleLatch latch = new DoubleLatch();

        DependencyResolver resolver = db.getDependencyResolver();
        Locks manager = resolver.resolveDependency( Locks.class );

        final IndexDescriptor index = createUniquenessConstraint();
        final String value = "value";

        DataWriteOperations dataStatement = dataWriteOperationsInNewTransaction();
        long nodeId = dataStatement.nodeCreate();
        dataStatement.nodeAddLabel( nodeId, labelId );

        // This adds the node to the unique index and should take an index write lock
        dataStatement.nodeSetProperty( nodeId, Property.stringProperty( propertyKeyId, value ) );

        Runnable runnableForThread2 = new Runnable()
        {
            @Override
            public void run()
            {
                latch.awaitStart();
                try ( Transaction tx = db.beginTx() )
                {
                    try ( Statement statement = statementContextSupplier.get() )
                    {
                        statement.readOperations().nodeGetFromUniqueIndexSeek( index, value );
                    }
                    tx.success();
                }
                catch ( IndexNotFoundKernelException | IndexBrokenKernelException e )
                {
                    throw new RuntimeException( e );
                }
                finally
                {
                    latch.finish();
                }
            }
        };
        Thread thread2 = new Thread( runnableForThread2, "Transaction Thread 2" );
        thread2.start();
        latch.start();

        spinUntilBlocking:
        for (; ; )
        {
            if(thread2.getState() == Thread.State.TIMED_WAITING || thread2.getState() == Thread.State.WAITING)
            {
                break;
            }
            Thread.yield();
        }

        commit();
        latch.awaitFinish();
    }

    private boolean isNoSuchNode( long foundId )
    {
        return StatementConstants.NO_SUCH_NODE == foundId;
    }

    private long createNodeWithValue( String value ) throws KernelException
    {
        DataWriteOperations dataStatement = dataWriteOperationsInNewTransaction();
        long nodeId = dataStatement.nodeCreate();
        dataStatement.nodeAddLabel( nodeId, labelId );
        dataStatement.nodeSetProperty( nodeId, Property.stringProperty( propertyKeyId, value ) );
        commit();
        return nodeId;
    }

    private IndexDescriptor createUniquenessConstraint() throws Exception
    {
        SchemaWriteOperations schemaStatement = schemaWriteOperationsInNewTransaction();
        schemaStatement.uniquePropertyConstraintCreate( labelId, propertyKeyId );
        IndexDescriptor result = schemaStatement.uniqueIndexGetForLabelAndPropertyKey( labelId, propertyKeyId );
        commit();
        return result;
    }
}
