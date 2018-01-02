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
package org.neo4j.server.rest.transactional;

import java.net.URI;

import org.junit.Test;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;
import org.neo4j.helpers.Clock;
import org.neo4j.logging.NullLogProvider;
import org.neo4j.server.rest.transactional.error.InvalidConcurrentTransactionAccess;
import org.neo4j.server.rest.web.TransactionUriScheme;
import org.neo4j.test.DoubleLatch;

import javax.servlet.http.HttpServletRequest;

import static javax.xml.bind.DatatypeConverter.parseLong;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class ConcurrentTransactionAccessTest
{
    @Test
    public void shouldThrowSpecificExceptionOnConcurrentTransactionAccess() throws Exception
    {
        // given
        TransactionRegistry registry =
                new TransactionHandleRegistry( mock( Clock.class), 0, NullLogProvider.getInstance() );
        TransitionalPeriodTransactionMessContainer kernel = mock( TransitionalPeriodTransactionMessContainer.class );
        when(kernel.newTransaction()).thenReturn( mock(TransitionalTxManagementKernelTransaction.class) );
        TransactionFacade actions = new TransactionFacade( kernel, null, registry, NullLogProvider.getInstance() );

        final TransactionHandle transactionHandle = actions.newTransactionHandle( new DisgustingUriScheme() );

        final DoubleLatch latch = new DoubleLatch();

        final StatementDeserializer statements = mock( StatementDeserializer.class );
        when( statements.hasNext() ).thenAnswer( new Answer<Boolean>()
        {
            @Override
            public Boolean answer( InvocationOnMock invocation ) throws Throwable
            {
                latch.startAndAwaitFinish();
                return false;
            }
        } );

        new Thread( new Runnable()
        {
            @Override
            public void run()
            {
                // start and block until finish
                transactionHandle.execute( statements, mock( ExecutionResultSerializer.class ), mock(
                        HttpServletRequest.class ) );
            }
        } ).start();

        latch.awaitStart();

        try
        {
            // when
            actions.findTransactionHandle( DisgustingUriScheme.parseTxId( transactionHandle.uri() ) );
            fail( "should have thrown exception" );
        }
        catch ( InvalidConcurrentTransactionAccess neo4jError )
        {
            // then we get here
        }
        finally
        {
            latch.finish();
        }
    }

    private static class DisgustingUriScheme implements TransactionUriScheme
    {
        private static long parseTxId( URI txUri )
        {
            return parseLong( txUri.toString() );
        }

        @Override
        public URI txUri( long id )
        {
            return URI.create( String.valueOf( id ) );
        }

        @Override
        public URI txCommitUri( long id )
        {
            return txUri( id );
        }
    }
}
