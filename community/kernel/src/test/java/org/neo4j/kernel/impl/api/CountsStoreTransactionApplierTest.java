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

import java.util.Optional;

import org.neo4j.kernel.api.StatementConstants;
import org.neo4j.kernel.impl.store.counts.CountsTracker;
import org.neo4j.kernel.impl.transaction.command.Command;
import org.neo4j.storageengine.api.TransactionApplicationMode;

import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class CountsStoreTransactionApplierTest
{
    @Test
    public void shouldNotifyCacheAccessOnHowManyUpdatesOnCountsWeHadSoFar() throws Exception
    {
        // GIVEN
        final CountsTracker tracker = mock( CountsTracker.class );
        final CountsAccessor.Updater updater = mock( CountsAccessor.Updater.class );
        when( tracker.apply( anyLong() ) ).thenReturn( Optional.of( updater ) );
        final CountsStoreBatchTransactionApplier applier = new CountsStoreBatchTransactionApplier( tracker,
                TransactionApplicationMode.INTERNAL );

        // WHEN
        try ( TransactionApplier txApplier = applier.startTx( new TransactionToApply( null, 2L ) ) )
        {
            txApplier.visitNodeCountsCommand( new Command.NodeCountsCommand( StatementConstants.ANY_LABEL, 1 ) );
        }

        // THEN
        verify( updater, times( 1 ) ).incrementNodeCount( StatementConstants.ANY_LABEL, 1 );
    }
}
