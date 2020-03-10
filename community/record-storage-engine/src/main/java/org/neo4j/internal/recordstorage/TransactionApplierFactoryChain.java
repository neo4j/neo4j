/*
 * Copyright (c) 2002-2020 "Neo4j,"
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
package org.neo4j.internal.recordstorage;

import java.io.IOException;
import java.util.function.Supplier;

import org.neo4j.kernel.impl.store.IdUpdateListener;
import org.neo4j.storageengine.api.CommandsToApply;

/**
 * This class wraps several {@link TransactionApplierFactory}s which will do their work sequentially. See also {@link
 * TransactionApplierFacade} which is used to wrap the {@link TransactionApplierFactory#startTx(CommandsToApply, BatchContext)} and {@link
 * TransactionApplierFactory#startTx(CommandsToApply, BatchContext)} methods.
 * Chains are reused between the batches of transactions as a consequence they should be stateless.
 */
public class TransactionApplierFactoryChain implements TransactionApplierFactory
{
    private final Supplier<IdUpdateListener> idUpdateListenerSupplier;
    private final TransactionApplierFactory[] appliers;

    public TransactionApplierFactoryChain( Supplier<IdUpdateListener> idUpdateListenerSupplier, TransactionApplierFactory... appliers )
    {
        this.idUpdateListenerSupplier = idUpdateListenerSupplier;
        this.appliers = appliers;
    }

    public Supplier<IdUpdateListener> getIdUpdateListenerSupplier()
    {
        return idUpdateListenerSupplier;
    }

    @Override
    public TransactionApplier startTx( CommandsToApply transaction, BatchContext batchContext ) throws IOException
    {
        TransactionApplier[] txAppliers = new TransactionApplier[appliers.length];
        for ( int i = 0; i < appliers.length; i++ )
        {
            txAppliers[i] = appliers[i].startTx( transaction, batchContext );
        }
        return new TransactionApplierFacade( txAppliers );
    }
}
