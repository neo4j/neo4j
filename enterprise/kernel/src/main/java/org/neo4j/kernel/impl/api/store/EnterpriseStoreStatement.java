/*
 * Copyright (c) 2002-2017 "Neo Technology,"
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
package org.neo4j.kernel.impl.api.store;

import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Supplier;

import org.neo4j.cursor.Cursor;
import org.neo4j.kernel.impl.api.IndexReaderFactory;
import org.neo4j.kernel.impl.locking.LockService;
import org.neo4j.kernel.impl.store.NeoStores;
import org.neo4j.kernel.impl.store.NodeStore;
import org.neo4j.storageengine.api.NodeItem;
import org.neo4j.storageengine.api.schema.LabelScanReader;
import org.neo4j.storageengine.api.txstate.ReadableTransactionState;

import static java.lang.Math.max;

public class EnterpriseStoreStatement extends StoreStatement
{
    private final NodeStore nodeStore;

    public EnterpriseStoreStatement( NeoStores neoStores, Supplier<IndexReaderFactory> indexReaderFactory,
            Supplier<LabelScanReader> labelScanReaderSupplier, LockService lockService )
    {
        super( neoStores, indexReaderFactory, labelScanReaderSupplier, lockService );
        this.nodeStore = neoStores.getNodeStore();
    }

    @Override
    public NodeProgression parallelNodeScanProgression()
    {
        return new NodeProgression()
        {
            private final int reservedLowIds = nodeStore.getNumberOfReservedLowIds();
            private final int recordsPerPage = nodeStore.getRecordsPerPage();
            private final long lastPageId = nodeStore.getHighestPossibleIdInUse() / recordsPerPage;
            private final AtomicLong pageIds = new AtomicLong();
            private final AtomicBoolean done = new AtomicBoolean();

            private final ThreadLocal<TransactionStateAccessMode> state =
                    ThreadLocal.withInitial( () -> TransactionStateAccessMode.NONE );
            private final AtomicBoolean append = new AtomicBoolean( true );

            @Override
            public boolean nextBatch( Batch batch )
            {
                while ( true )
                {
                    if ( done.get() )
                    {
                        batch.nothing();
                        return false;
                    }

                    long pageId = pageIds.getAndIncrement();
                    if ( pageId < lastPageId )
                    {
                        long first = firstId( pageId );
                        long last = first + recordsPerPage - 1;
                        batch.init( first, last );
                        return true;
                    }
                    else if ( !done.get() && done.compareAndSet( false, true ) )
                    {
                        long first = firstId( lastPageId );
                        long last = nodeStore.getHighestPossibleIdInUse();
                        batch.init( first, last );
                        return true;
                    }
                }
            }

            private long firstId( long pageId )
            {
                return max( reservedLowIds, pageId * recordsPerPage );
            }

            @Override
            public TransactionStateAccessMode mode()
            {
                if ( append.get() && append.compareAndSet( true, false ) )
                {
                    state.set( TransactionStateAccessMode.APPEND );
                }
                return state.get();
            }
        };
    }

    @Override
    public Cursor<NodeItem> acquireParallelScanNodeCursor( NodeProgression nodeProgression,
            ReadableTransactionState state )
    {
        return nodeCursor.get().init( nodeProgression, state );
    }
}
