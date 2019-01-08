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

import java.io.IOException;

import org.neo4j.kernel.impl.locking.LockGroup;
import org.neo4j.storageengine.api.CommandsToApply;

/**
 * This class wraps several {@link BatchTransactionApplier}s which will do their work sequentially. See also {@link
 * TransactionApplierFacade} which is used to wrap the {@link #startTx(CommandsToApply)} and {@link
 * #startTx(CommandsToApply, LockGroup)} methods.
 */
public class BatchTransactionApplierFacade implements BatchTransactionApplier
{

    private final BatchTransactionApplier[] appliers;

    public BatchTransactionApplierFacade( BatchTransactionApplier... appliers )
    {
        this.appliers = appliers;
    }

    @Override
    public TransactionApplier startTx( CommandsToApply transaction ) throws IOException
    {
        TransactionApplier[] txAppliers = new TransactionApplier[appliers.length];
        for ( int i = 0; i < appliers.length; i++ )
        {
            txAppliers[i] = appliers[i].startTx( transaction );
        }
        return new TransactionApplierFacade( txAppliers );
    }

    @Override
    public TransactionApplier startTx( CommandsToApply transaction, LockGroup lockGroup ) throws IOException
    {
        TransactionApplier[] txAppliers = new TransactionApplier[appliers.length];
        for ( int i = 0; i < appliers.length; i++ )
        {
            txAppliers[i] = appliers[i].startTx( transaction, lockGroup );
        }
        return new TransactionApplierFacade( txAppliers );
    }

    @Override
    public void close() throws Exception
    {
        // Not sure why it is necessary to close them in reverse order
        for ( int i = appliers.length; i-- > 0; )
        {
            appliers[i].close();
        }
    }
}
