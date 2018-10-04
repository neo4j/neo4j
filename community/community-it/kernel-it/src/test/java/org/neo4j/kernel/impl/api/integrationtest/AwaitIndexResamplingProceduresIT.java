/*
 * Copyright (c) 2002-2018 "Neo4j,"
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

import org.hamcrest.Matchers;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.util.concurrent.atomic.AtomicReference;

import static org.hamcrest.Matchers.is;

import org.neo4j.internal.kernel.api.IndexReference;
import org.neo4j.internal.kernel.api.Transaction;
import org.neo4j.internal.kernel.api.exceptions.KernelException;
import org.neo4j.internal.kernel.api.exceptions.ProcedureException;
import org.neo4j.internal.kernel.api.exceptions.schema.IndexNotFoundKernelException;
import org.neo4j.kernel.api.exceptions.Status;
import org.neo4j.register.Register;
import org.neo4j.register.Registers;
import org.neo4j.values.storable.Values;

import static java.util.concurrent.TimeUnit.MINUTES;
import static org.hamcrest.MatcherAssert.assertThat;

import static org.neo4j.internal.kernel.api.procs.ProcedureSignature.procedureName;
import static org.neo4j.internal.kernel.api.security.LoginContext.AUTH_DISABLED;
import static org.neo4j.kernel.api.schema.SchemaDescriptorFactory.forLabel;

public class AwaitIndexResamplingProceduresIT extends KernelIntegrationTest
{
    @Rule
    public ExpectedException exception = ExpectedException.none();

    private final Register.DoubleLongRegister register = Registers.newDoubleLongRegister();

    @Test
    public void shouldNotTimeOutIfNoIndexes() throws Throwable
    {
        try ( Transaction transaction = kernel.beginTransaction( Transaction.Type.explicit, AUTH_DISABLED ) )
        {
            waitForIndexResampling( 0 );
        }

    }

    @Test
    public void shouldNotTimeOutIfNoUpdates() throws Throwable
    {
        // Given
        try ( Transaction transaction = kernel.beginTransaction( Transaction.Type.explicit, AUTH_DISABLED ) )
        {
            final int labelId = transaction.tokenWrite().labelGetOrCreateForName( "Person" );
            final int propertyKeyId = transaction.tokenWrite().propertyKeyGetOrCreateForName( "foo" );
            transaction.schemaWrite().indexCreate( forLabel( labelId, propertyKeyId ) );
            transaction.success();
        }

        waitForIndexToComeOnline();

        try ( Transaction transaction = kernel.beginTransaction( Transaction.Type.explicit, AUTH_DISABLED ) )
        {
            resampleIndex();
            waitForIndexResampling( 0 );
        }
    }

    @Test
    public void shouldTimeOutIfUpdatesAndToShortTimeout() throws Throwable
    {
        final int labelId;
        final int propertyKeyId;
        final IndexReference indexReference;

        // Given
        try ( Transaction transaction = kernel.beginTransaction( Transaction.Type.explicit, AUTH_DISABLED ) )
        {
            labelId = transaction.tokenWrite().labelGetOrCreateForName( "Person" );
            propertyKeyId = transaction.tokenWrite().propertyKeyGetOrCreateForName( "foo" );
            indexReference = transaction.schemaWrite().indexCreate( forLabel( labelId, propertyKeyId ) );
            transaction.success();
        }

        waitForIndexToComeOnline();

        // Create a (:Person {foo: 0}) node
        createNode( labelId, propertyKeyId, 0 );

        AtomicReference<ProcedureException> exception = new AtomicReference<>();

        try ( Transaction transaction = kernel.beginTransaction( Transaction.Type.explicit, AUTH_DISABLED ) )
        {
            assertThat( nbrOfUpdates( indexReference, transaction ), Matchers.equalTo( 1L ) );
            waitForIndexResampling( 0 );
        }
        catch ( ProcedureException e )
        {
            exception.set( e );
        }

        assertThat( exception.get().status(), is( Status.Procedure.ProcedureTimedOut) );
    }

    @Test
    public void shouldAwaitIndexResampling() throws Throwable
    {
        final int labelId;
        final int propertyKeyId;
        final IndexReference indexReference;
        int nonResampledUpdates = 0;

        // Given
        try ( Transaction transaction = kernel.beginTransaction( Transaction.Type.explicit, AUTH_DISABLED ) )
        {
            labelId = transaction.tokenWrite().labelGetOrCreateForName( "Person" );
            propertyKeyId = transaction.tokenWrite().propertyKeyGetOrCreateForName( "foo" );
            indexReference = transaction.schemaWrite().indexCreate( forLabel( labelId, propertyKeyId ) );
            transaction.success();
        }

        waitForIndexToComeOnline();

        for ( int i = 0; i < 1000; i++ )
        {
            // Create a (:Person {foo: i}) node
            createNode( labelId, propertyKeyId, i );

            try ( Transaction transaction = kernel.beginTransaction( Transaction.Type.explicit, AUTH_DISABLED ) )
            {
                assertThat( nbrOfUpdates( indexReference, transaction ), Matchers.equalTo( 1L ) );

                // Trigger a resampling of the index and measure how many updates were not resampled directly
                // (will vary between 0 and 1 )
                resampleIndex();

                nonResampledUpdates += nbrOfUpdates( indexReference, transaction );

                waitForIndexResampling( 120 );

                // Ensure no updates are left after resampling
                assertThat( nbrOfUpdates( indexReference, transaction ), Matchers.equalTo( 0L ) );
            }
        }

        // Check that we had to wait for resampling at least once
        assertThat( nonResampledUpdates, Matchers.greaterThan( 0 ) );
    }

    private void createNode( int labelId, int propertyKeyId, int propertyValue ) throws KernelException
    {
        try ( Transaction transaction = kernel.beginTransaction( Transaction.Type.explicit, AUTH_DISABLED ) )
        {
            long nodeId = transaction.dataWrite().nodeCreate();
            transaction.dataWrite().nodeAddLabel( nodeId, labelId );
            transaction.dataWrite().nodeSetProperty( nodeId, propertyKeyId, Values.of( propertyValue ) );
            transaction.success();
        }
    }

    private void waitForIndexToComeOnline()
    {
        try ( org.neo4j.graphdb.Transaction tx = db.beginTx() )
        {
            db.schema().awaitIndexesOnline( 2, MINUTES );
            tx.success();
        }
    }

    private void waitForIndexResampling( long timeout ) throws Throwable
    {
        procs().procedureCallRead( procs().procedureGet(
                procedureName( "db", "awaitIndexResampling" ) ).id(), new Object[]{ timeout } );
    }

    private void resampleIndex() throws Throwable
    {
        procs().procedureCallRead( procs().procedureGet(
                procedureName( "db", "resampleIndex" ) ).id(),
                new Object[]{":Person(foo)"} );
    }

    private long nbrOfUpdates( IndexReference indexReference, Transaction transaction )
            throws IndexNotFoundKernelException
    {
        transaction.schemaRead().indexUpdatesAndSize( indexReference, register );
        return register.readFirst();
    }
}
