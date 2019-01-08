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
package org.neo4j.kernel.impl.context;

import java.util.function.LongSupplier;

import org.neo4j.io.pagecache.tracing.cursor.context.VersionContext;

import static org.neo4j.kernel.impl.transaction.log.TransactionIdStore.BASE_TX_ID;

/**
 * Transactional version context that used by read transaction to read data of specific version.
 * Or perform versioned data modification.
 */
public class TransactionVersionContext implements VersionContext
{
    private final LongSupplier lastClosedTxIdSupplier;
    private long transactionId = BASE_TX_ID;
    private long lastClosedTxId = Long.MAX_VALUE;
    private boolean dirty;

    public TransactionVersionContext( LongSupplier lastClosedTxIdSupplier )
    {
        this.lastClosedTxIdSupplier = lastClosedTxIdSupplier;
    }

    @Override
    public void initRead()
    {
        long txId = lastClosedTxIdSupplier.getAsLong();
        assert txId >= BASE_TX_ID;
        lastClosedTxId = txId;
        dirty = false;
    }

    @Override
    public void initWrite( long committingTxId )
    {
        assert committingTxId >= BASE_TX_ID;
        transactionId = committingTxId;
    }

    @Override
    public long committingTransactionId()
    {
        return transactionId;
    }

    @Override
    public long lastClosedTransactionId()
    {
        return lastClosedTxId;
    }

    @Override
    public void markAsDirty()
    {
        dirty = true;
    }

    @Override
    public boolean isDirty()
    {
        return dirty;
    }
}
