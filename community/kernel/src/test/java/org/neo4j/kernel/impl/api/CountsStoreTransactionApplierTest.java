/*
 * Copyright (c) 2002-2016 "Neo Technology,"
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

import java.util.Optional;

import org.neo4j.kernel.api.ReadOperations;
import org.neo4j.kernel.impl.store.counts.CountsSnapshot;
import org.neo4j.kernel.impl.store.counts.CountsStorageService;
import org.neo4j.kernel.impl.store.counts.CountsStorageServiceImpl;
import org.neo4j.kernel.impl.store.counts.CountsTracker;
import org.neo4j.kernel.impl.transaction.command.Command;
import org.neo4j.storageengine.api.TransactionApplicationMode;

import static org.mockito.Matchers.anyLong;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.neo4j.kernel.impl.transaction.log.TransactionIdStore.BASE_TX_ID;

public class CountsStoreTransactionApplierTest
{
    @Test
    public void shouldNotifyCacheAccessOnHowManyUpdatesOnCountsWeHadSoFar() throws Exception
    {
        // GIVEN
        final CountsTracker tracker = mock( CountsTracker.class );
        final CountsStorageService countsStorageService = new CountsStorageServiceImpl();
        countsStorageService.initialize( new CountsSnapshot( BASE_TX_ID) );

        final CountsAccessor.Updater updater = mock( CountsAccessor.Updater.class );
        when( tracker.apply( anyLong() ) ).thenReturn( Optional.of( updater ) );
        final CountsStoreBatchTransactionApplier applier =
                new CountsStoreBatchTransactionApplier( tracker, countsStorageService,
                        TransactionApplicationMode.INTERNAL );

        // WHEN
        try ( TransactionApplier txApplier = applier.startTx( new TransactionToApply( null, 2L ) ) )
        {
            txApplier.visitNodeCountsCommand( new Command.NodeCountsCommand( ReadOperations.ANY_LABEL, 1 ) );
        }

        // THEN
        verify( updater, times( 1 ) ).incrementNodeCount( ReadOperations.ANY_LABEL, 1 );
    }
}
