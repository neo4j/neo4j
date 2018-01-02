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
package org.neo4j.kernel.impl.api;

import org.junit.Test;

import org.neo4j.graphdb.TransactionTerminatedException;
import org.neo4j.kernel.api.exceptions.Status;
import org.neo4j.kernel.api.labelscan.LabelScanReader;
import org.neo4j.kernel.api.labelscan.LabelScanStore;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

public class KernelStatementTest
{
    @Test
    public void shouldCloseOpenedLabelScanReader() throws Exception
    {
        // given
        LabelScanStore scanStore = mock( LabelScanStore.class );
        LabelScanReader scanReader = mock( LabelScanReader.class );

        when( scanStore.newReader() ).thenReturn( scanReader );
        KernelStatement statement =
            new KernelStatement(
                mock( KernelTransactionImplementation.class ),
                mock( IndexReaderFactory.class ), scanStore, null, null, null, null );

        statement.acquire();

        // when
        LabelScanReader actualReader = statement.getLabelScanReader();

        // then
        assertEquals( scanReader, actualReader );

        // when
        statement.close();

        // then
        verify( scanStore ).newReader();
        verifyNoMoreInteractions( scanStore );

        verify( scanReader ).close();
        verifyNoMoreInteractions( scanReader );
    }

    @Test(expected = TransactionTerminatedException.class)
    public void shouldThrowTerminateExceptionWhenTransactionTerminated() throws Exception
    {
        KernelTransactionImplementation transaction = mock( KernelTransactionImplementation.class );
        when( transaction.getReasonIfTerminated() ).thenReturn( Status.General.UnknownFailure );

        KernelStatement statement = new KernelStatement(
            transaction, mock( IndexReaderFactory.class ),
                mock( LabelScanStore.class ), null, null, null, null );

        statement.readOperations().nodeExists( 0 );
    }

    @Test
    public void shouldCloseOpenedLabelScanReaderWhenForceClosed() throws Exception
    {
        // given
        LabelScanStore scanStore = mock( LabelScanStore.class );
        LabelScanReader scanReader = mock( LabelScanReader.class );

        when( scanStore.newReader() ).thenReturn( scanReader );
        KernelStatement statement =
                new KernelStatement( mock( KernelTransactionImplementation.class ), mock( IndexReaderFactory.class ),
                        scanStore, null, null, null, null );

        statement.acquire();

        // when
        LabelScanReader actualReader = statement.getLabelScanReader();

        // then
        assertEquals( scanReader, actualReader );

        // when
        statement.forceClose();

        // then
        verify( scanStore ).newReader();
        verifyNoMoreInteractions( scanStore );

        verify( scanReader ).close();
        verifyNoMoreInteractions( scanReader );
    }
}
