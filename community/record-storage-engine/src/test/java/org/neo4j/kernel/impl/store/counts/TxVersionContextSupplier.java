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
package org.neo4j.kernel.impl.store.counts;

import java.util.function.LongSupplier;

import org.neo4j.io.pagecache.tracing.cursor.context.VersionContext;
import org.neo4j.io.pagecache.tracing.cursor.context.VersionContextSupplier;

public class TxVersionContextSupplier implements VersionContextSupplier, VersionContext
{
    private LongSupplier supplier;
    private long lastClosedTxId;
    private long transactionId;
    private boolean dirty;

    @Override
    public void init( LongSupplier lastClosedTransactionIdSupplier )
    {
        this.supplier = lastClosedTransactionIdSupplier;
    }

    @Override
    public VersionContext getVersionContext()
    {
        return this;
    }

    @Override
    public void initRead()
    {
        lastClosedTxId = supplier.getAsLong();
    }

    @Override
    public void initWrite( long committingTxId )
    {
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
