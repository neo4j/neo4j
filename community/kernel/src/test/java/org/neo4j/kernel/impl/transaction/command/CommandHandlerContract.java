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
package org.neo4j.kernel.impl.transaction.command;

import org.neo4j.helpers.collection.Visitor;
import org.neo4j.kernel.impl.api.BatchTransactionApplier;
import org.neo4j.kernel.impl.api.TransactionApplier;
import org.neo4j.kernel.impl.api.TransactionToApply;
import org.neo4j.kernel.impl.locking.LockGroup;
import org.neo4j.kernel.impl.transaction.TransactionRepresentation;
import org.neo4j.storageengine.api.CommandsToApply;

/**
 * Serves as executor of transactions, i.e. the visit... methods and will invoke the other lifecycle methods like {@link
 * BatchTransactionApplier#startTx(CommandsToApply, LockGroup)}, {@link TransactionApplier#close()} ()} a.s.o
 * correctly. Note that {@link BatchTransactionApplier#close()} is also called at the end.
 */
public class CommandHandlerContract
{
    private CommandHandlerContract()
    {
    }

    @FunctionalInterface
    public interface ApplyFunction
    {
        boolean apply( TransactionApplier applier ) throws Exception;
    }

    /**
     * Simply calls through to the {@link TransactionRepresentation#accept(Visitor)} method for each {@link
     * TransactionToApply} given. This assumes that the {@link BatchTransactionApplier} will return {@link
     * TransactionApplier}s which actually do the work and that the transaction has all the relevant data.
     *
     * @param applier to use
     * @param transactions to apply
     */
    public static void apply( BatchTransactionApplier applier, TransactionToApply... transactions ) throws Exception
    {
        for ( TransactionToApply tx : transactions )
        {
            try ( TransactionApplier txApplier = applier.startTx( tx, new LockGroup() ) )
            {
                tx.transactionRepresentation().accept( txApplier );
            }
        }
        applier.close();
    }

    /**
     * In case the transactions do not have the commands to apply, use this method to apply any commands you want with a
     * given {@link ApplyFunction} instead.
     *
     * @param applier to use
     * @param function which knows what to do with the {@link TransactionApplier}.
     * @param transactions are only used to create {@link TransactionApplier}s. The actual work is delegated to the
     * function.
     * @return the boolean-and result of all function operations.
     */
    public static boolean apply( BatchTransactionApplier applier, ApplyFunction function,
            TransactionToApply... transactions ) throws Exception
    {
        boolean result = true;
        for ( TransactionToApply tx : transactions )
        {
            try ( TransactionApplier txApplier = applier.startTx( tx, new LockGroup() ) )
            {
                result &= function.apply( txApplier );
            }
        }
        applier.close();
        return result;
    }
}
