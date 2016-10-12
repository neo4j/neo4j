/*
 * Copyright (c) 2002-2016 "Neo Technology,"
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

import org.neo4j.kernel.impl.store.TransactionId;
import org.neo4j.kernel.impl.transaction.log.LogPosition;
import org.neo4j.kernel.impl.transaction.log.LogicalTransactionStore;
import org.neo4j.kernel.impl.transaction.log.TransactionIdStore;
import org.neo4j.kernel.impl.transaction.log.TransactionMetadataCache;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class TransactionChecksumLookupTest
{

    private TransactionIdStore transactionIdStore = mock( TransactionIdStore.class );
    private LogicalTransactionStore transactionStore = mock( LogicalTransactionStore.class );

    @Before
    public void setUp() throws IOException
    {
        when( transactionIdStore.getLastCommittedTransaction() ).thenReturn( new TransactionId( 1, 1, 1 ) );
        when( transactionIdStore.getUpgradeTransaction() ).thenReturn( new TransactionId( 2, 2, 2 ) );
        when( transactionStore.getMetadataFor( 3 ) ).thenReturn(
                new TransactionMetadataCache.TransactionMetadata( 1, 1, mock( LogPosition.class ), 3, 3 ) );
    }

    @Test
    public void lookupChecksumUsingUpgradeTransaction() throws Exception
    {
        TransactionChecksumLookup checksumLookup = new TransactionChecksumLookup( transactionIdStore, transactionStore );
        assertEquals(2, checksumLookup.applyAsLong( 2 ));
    }

    @Test
    public void lookupChecksumUsingCommittedTransaction() throws Exception
    {
        TransactionChecksumLookup checksumLookup = new TransactionChecksumLookup( transactionIdStore, transactionStore );
        assertEquals(1, checksumLookup.applyAsLong( 1 ));
    }

    @Test
    public void lookupChecksumUsingTransactionStore() throws Exception
    {
        TransactionChecksumLookup checksumLookup = new TransactionChecksumLookup( transactionIdStore, transactionStore );
        assertEquals(3, checksumLookup.applyAsLong( 3 ));
    }
}
