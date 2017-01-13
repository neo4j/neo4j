/*
 * Copyright (c) 2002-2017 "Neo Technology,"
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
package org.neo4j.kernel.impl.api;

import org.junit.Test;

import org.neo4j.kernel.api.exceptions.Status;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.neo4j.kernel.impl.api.KernelTransactionImplementation.TransactionStatus;

public class KernelTransactionStatusTest
{

    @Test
    public void openStatusAfterInitialization() throws Exception
    {
        TransactionStatus status = getOpenStatus();
        assertTrue( "Status should be open after initialization.", status.isOpen() );
    }

    @Test
    public void closedStatusAfterCreation()
    {
        TransactionStatus status = createStatus();
        assertTrue( "Status should be closed just after creation", status.isClosed() );
    }

    @Test
    public void terminateOpenTransaction()
    {
        TransactionStatus status = getOpenStatus();

        assertTrue( status.terminate( Status.Transaction.Terminated ) );
        assertTrue( status.isTerminated() );
        assertEquals( Status.Transaction.Terminated, status.getTerminationReason().get() );
    }

    @Test
    public void terminateClosingTransaction()
    {
        TransactionStatus status = getOpenStatus();

        assertTrue( status.closing() );
        assertTrue( status.terminate( Status.Transaction.Terminated ) );
        assertTrue( status.isTerminated() );
        assertEquals( Status.Transaction.Terminated, status.getTerminationReason().get() );
    }

    @Test
    public void closedTransactionIsNotTerminatable()
    {
        TransactionStatus status = getOpenStatus();

        status.close();
        assertFalse( status.terminate( Status.Transaction.Terminated ) );
        assertFalse( status.getTerminationReason().isPresent() );
    }

    @Test
    public void shutdownTransactionIsNotTerminatable()
    {
        TransactionStatus status = getOpenStatus();

        assertTrue( status.shutdown() );
        assertFalse( status.terminate( Status.Transaction.Terminated ) );
    }

    @Test
    public void closingTransactionNotPossibleWhenTransactionIsClosedAlready()
    {
        TransactionStatus status = getOpenStatus();

        status.close();
        assertFalse( status.closing() );
        assertTrue( status.isClosed() );
    }

    @Test
    public void closingTransactionInClosingState()
    {
        TransactionStatus closingStatus = getOpenStatus();
        closingStatus.closing();
        closingStatus.close();
        assertTrue( closingStatus.isClosed() );
    }

    @Test
    public void closingTransactionCanBeTerminated()
    {
        TransactionStatus openStatus = getOpenStatus();
        assertTrue( openStatus.closing() );
        assertTrue( openStatus.terminate( Status.Transaction.Terminated ) );

        assertTrue( openStatus.isTerminated() );
        assertTrue( openStatus.isClosing() );
    }

    @Test
    public void transactionStatusLifeCycle()
    {
        TransactionStatus status = getOpenStatus();

        assertTrue( status.closing() );
        assertTrue( status.isClosing() );

        status.close();
        assertTrue( status.isClosed() );
    }

    @Test
    public void closeResetTerminationReason()
    {
        TransactionStatus openStatus = getOpenStatus();

        assertTrue( openStatus.closing() );
        assertTrue( openStatus.terminate( Status.Transaction.Terminated ) );
        assertTrue( openStatus.getTerminationReason().isPresent() );
        openStatus.close();

        assertFalse( openStatus.getTerminationReason().isPresent() );
    }

    private TransactionStatus getOpenStatus()
    {
        TransactionStatus status = createStatus();
        status.init();
        return status;
    }

    private TransactionStatus createStatus()
    {
        return new TransactionStatus();
    }
}
