/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 *
 * This file is part of Neo4j Enterprise Edition. The included source
 * code can be redistributed and/or modified under the terms of the
 * Neo4j Sweden Software License, as found in the associated LICENSE.txt
 * file.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * Neo4j Sweden Software License for more details.
 */
package org.neo4j.kernel.ha;

import org.junit.Before;
import org.junit.Test;

import java.util.Collections;
import java.util.concurrent.atomic.AtomicInteger;

import org.neo4j.com.ComException;
import org.neo4j.com.RequestContext;
import org.neo4j.com.Response;
import org.neo4j.graphdb.TransientTransactionFailureException;
import org.neo4j.kernel.ha.com.RequestContextFactory;
import org.neo4j.kernel.ha.com.master.Master;
import org.neo4j.kernel.impl.api.TransactionToApply;
import org.neo4j.kernel.impl.transaction.log.PhysicalTransactionRepresentation;
import org.neo4j.kernel.impl.transaction.tracing.CommitEvent;
import org.neo4j.storageengine.api.TransactionApplicationMode;
import org.neo4j.test.ConstantRequestContextFactory;
import org.neo4j.test.LongResponse;

import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class SlaveTransactionCommitProcessTest
{
    private AtomicInteger lastSeenEventIdentifier;
    private Master master;
    private RequestContext requestContext;
    private Response<Long> response;
    private PhysicalTransactionRepresentation tx;
    private SlaveTransactionCommitProcess commitProcess;

    @Before
    public void setUp()
    {
        lastSeenEventIdentifier = new AtomicInteger( -1 );
        master = mock( Master.class );
        requestContext = new RequestContext( 10, 11, 12, 13, 14 );
        RequestContextFactory reqFactory = new ConstantRequestContextFactory( requestContext )
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
                Collections.emptyList() );
        tx.setHeader(new byte[]{}, 1, 1, 1, 1, 1, 1337);

        commitProcess = new SlaveTransactionCommitProcess( master, reqFactory );
    }

    @Test
    public void shouldForwardLockIdentifierToMaster() throws Exception
    {
        // Given
        when( master.commit( requestContext, tx ) ).thenReturn( response );

        // When
        commitProcess.commit( new TransactionToApply( tx ), CommitEvent.NULL, TransactionApplicationMode.INTERNAL );

        // Then
        assertThat( lastSeenEventIdentifier.get(), is( 1337 ) );
    }

    @Test( expected = TransientTransactionFailureException.class )
    public void mustTranslateComExceptionsToTransientTransactionFailures() throws Exception
    {
        when( master.commit( requestContext, tx ) ).thenThrow( new ComException() );

        // When
        commitProcess.commit( new TransactionToApply( tx ), CommitEvent.NULL, TransactionApplicationMode.INTERNAL );
        // Then we assert that the right exception is thrown
    }
}
