/*
 * Copyright (c) 2002-2018 "Neo Technology,"
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
package org.neo4j.kernel.ha;

import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.Collections;
import java.util.concurrent.atomic.AtomicInteger;

import org.neo4j.com.ComException;
import org.neo4j.com.RequestContext;
import org.neo4j.com.Response;
import org.neo4j.graphdb.TransientTransactionFailureException;
import org.neo4j.kernel.api.exceptions.Status;
import org.neo4j.kernel.api.exceptions.TransactionFailureException;
import org.neo4j.kernel.ha.com.RequestContextFactory;
import org.neo4j.kernel.ha.com.master.Master;
import org.neo4j.kernel.impl.api.TransactionApplicationMode;
import org.neo4j.kernel.impl.locking.LockGroup;
import org.neo4j.kernel.impl.transaction.command.Command;
import org.neo4j.kernel.impl.transaction.log.PhysicalTransactionRepresentation;
import org.neo4j.kernel.impl.transaction.tracing.CommitEvent;
import org.neo4j.test.ConstantRequestContextFactory;
import org.neo4j.test.LongResponse;

import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class SlaveTransactionCommitProcessTest
{
    private AtomicInteger lastSeenEventIdentifier;
    private Master master;
    private RequestContext requestContext;
    private RequestContextFactory reqFactory;
    private Response<Long> response;
    private PhysicalTransactionRepresentation tx;
    private SlaveTransactionCommitProcess commitProcess;

    @Before
    public void setUp()
    {
        lastSeenEventIdentifier = new AtomicInteger( -1 );
        master = mock( Master.class );
        requestContext = new RequestContext( 10, 11, 12, 13, 14 );
        reqFactory = new ConstantRequestContextFactory( requestContext )
        {
            @Override
            public RequestContext newRequestContext( int eventIdentifier )
            {
                lastSeenEventIdentifier.set( eventIdentifier );
                return super.newRequestContext( eventIdentifier );
            }
        };
        response = new LongResponse( 42L );
        tx = new PhysicalTransactionRepresentation(
                Collections.<Command>emptyList() );
        tx.setHeader( new byte[]{}, 1, 1, 1, 1, 1, 1337 );

        commitProcess = new SlaveTransactionCommitProcess( master, reqFactory );
    }

    @Test
    public void shouldForwardLockIdentifierToMaster() throws Exception
    {
        // Given
        when( master.commit( requestContext, tx ) ).thenReturn( response );

        // When
        commitProcess.commit( tx , new LockGroup(), CommitEvent.NULL, TransactionApplicationMode.INTERNAL );

        // Then
        assertThat( lastSeenEventIdentifier.get(), is( 1337 ) );
    }

    @Test( expected = TransientTransactionFailureException.class )
    public void mustTranslateComExceptionsToTransientTransactionFailures() throws Exception
    {
        when( master.commit( requestContext, tx ) ).thenThrow( new ComException() );

        commitProcess.commit( tx , new LockGroup(), CommitEvent.NULL, TransactionApplicationMode.INTERNAL );
        // Then we assert that the right exception is thrown
    }

    @Test
    public void mustTranslateIOExceptionsToKernelTransactionFailures() throws Exception
    {
        when( master.commit( requestContext, tx ) ).thenThrow( new IOException() );

        try
        {
            commitProcess.commit( tx , new LockGroup(), CommitEvent.NULL, TransactionApplicationMode.INTERNAL );
            fail( "commit should have thrown" );
        }
        catch ( TransactionFailureException e )
        {
            assertThat( e.status(), is( (Status) Status.Transaction.CouldNotCommit ) );
        }
    }
}
