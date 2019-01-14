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
package org.neo4j.internal.kernel.api;

import org.junit.Test;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import org.neo4j.internal.kernel.api.exceptions.KernelException;
import org.neo4j.internal.kernel.api.schema.LabelSchemaDescriptor;
import org.neo4j.kernel.api.exceptions.Status;
import org.neo4j.values.storable.Values;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public abstract class LockingTestBase<G extends KernelAPIWriteTestSupport>
        extends KernelAPIWriteTestBase<G>
{
    @Test
    public void shouldNotBlockConstraintCreationOnUnrelatedPropertyWrite() throws Throwable
    {
        int nodeProp;
        int constraintProp;
        int label;

        // Given
        try ( Transaction tx = session.beginTransaction() )
        {
            nodeProp = tx.tokenWrite().propertyKeyGetOrCreateForName( "nodeProp" );
            constraintProp = tx.tokenWrite().propertyKeyGetOrCreateForName( "constraintProp" );
            label = tx.tokenWrite().labelGetOrCreateForName( "label" );
            tx.success();
        }

        try ( Transaction tx = session.beginTransaction() )
        {
            tx.schemaWrite().uniquePropertyConstraintCreate( labelDescriptor( label, constraintProp ) );
            tx.success();
        }

        CountDownLatch createNodeLatch = new CountDownLatch( 1 );
        CountDownLatch createConstraintLatch = new CountDownLatch( 1 );

        // When & Then
        ExecutorService executor = Executors.newFixedThreadPool( 2 );
        Future<?> f1 = executor.submit( () -> {
            try ( Transaction tx = session.beginTransaction() )
            {
                createNodeWithProperty( tx, nodeProp );

                createNodeLatch.countDown();
                assertTrue( createConstraintLatch.await( 5, TimeUnit.MINUTES) );

                tx.success();
            }
            catch ( Exception e )
            {
                fail( "Create node failed: " + e );
            }
            finally
            {
                createNodeLatch.countDown();
            }
        } );

        Future<?> f2 = executor.submit( () -> {

            try ( Transaction tx = session.beginTransaction() )
            {
                assertTrue( createNodeLatch.await( 5, TimeUnit.MINUTES) );
                tx.schemaWrite().uniquePropertyConstraintCreate( labelDescriptor( label, constraintProp ) );
                tx.success();
            }
            catch ( KernelException e )
            {
                // constraint already exists, so should fail!
                assertEquals( Status.Schema.ConstraintAlreadyExists, e.status() );
            }
            catch ( InterruptedException e )
            {
                fail( "Interrupted during create constraint" );
            }
            finally
            {
                createConstraintLatch.countDown();
            }
        } );

        try
        {
            f1.get();
            f2.get();
        }
        finally
        {
            executor.shutdown();
        }
    }

    private void createNodeWithProperty( Transaction tx, int propId1 ) throws KernelException
    {
        long node = tx.dataWrite().nodeCreate();
        tx.dataWrite().nodeSetProperty( node, propId1, Values.intValue( 42 ) );
    }

    protected abstract LabelSchemaDescriptor labelDescriptor( int label, int... props );
}
