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
package org.neo4j.kernel.impl.api;

import org.junit.Test;

import org.neo4j.kernel.impl.api.TransactionQueue.Applier;
import org.neo4j.kernel.impl.transaction.TransactionRepresentation;

import static org.junit.Assert.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.reset;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;

public class TransactionQueueTest
{
    @Test
    public void shouldEmptyIfTooMany() throws Exception
    {
        // GIVEN
        Applier applier = mock( Applier.class );
        int batchSize = 10;
        TransactionQueue queue = new TransactionQueue( batchSize, applier );

        // WHEN
        for ( int i = 0; i < 9; i++ )
        {
            queue.queue( mock( TransactionToApply.class ) );
            verifyNoMoreInteractions( applier );
        }
        queue.queue( mock( TransactionToApply.class ) );
        verify( applier, times( 1 ) ).apply( any(), any() );
        reset( applier );

        // THEN
        queue.queue( mock( TransactionToApply.class ) );

        // and WHEN emptying in the end
        for ( int i = 0; i < 2; i++ )
        {
            queue.queue( mock( TransactionToApply.class ) );
            verifyNoMoreInteractions( applier );
        }
        queue.empty();
        verify( applier, times( 1 ) ).apply( any(), any() );
    }

    @Test
    public void shouldLinkTogetherTransactions() throws Exception
    {
        // GIVEN
        Applier applier = mock( Applier.class );
        int batchSize = 10;
        TransactionQueue queue = new TransactionQueue( batchSize, applier );

        // WHEN
        TransactionToApply[] txs = new TransactionToApply[batchSize];
        for ( int i = 0; i < batchSize; i++ )
        {
            queue.queue( txs[i] = new TransactionToApply( mock( TransactionRepresentation.class ) ) );
        }

        // THEN
        verify( applier, times( 1 ) ).apply( any(), any() );
        for ( int i = 0; i < txs.length - 1; i++ )
        {
            assertEquals( txs[i + 1], txs[i].next() );
        }
    }
}
