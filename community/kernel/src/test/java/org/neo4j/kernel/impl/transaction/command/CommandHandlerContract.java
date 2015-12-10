/*
 * Copyright (c) 2002-2015 "Neo Technology,"
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
package org.neo4j.kernel.impl.transaction.command;

import java.io.IOException;
import java.util.function.Function;

import org.neo4j.helpers.collection.Visitor;
import org.neo4j.kernel.impl.api.BatchTransactionApplier;
import org.neo4j.kernel.impl.api.TransactionApplier;
import org.neo4j.kernel.impl.api.TransactionToApply;
import org.neo4j.kernel.impl.locking.LockGroup;
import org.neo4j.kernel.impl.transaction.TransactionRepresentation;

/**
 * Serves as executor of transactions, i.e. the visit... methods and will invoke the other lifecycle methods
 * like {@link CommandHandler#begin(TransactionToApply, LockGroup)}, {@link CommandHandler#end()} a.s.o correctly.
 */
public class CommandHandlerContract
{
    @FunctionalInterface
    public interface ApplyFunction
    {
        boolean apply( TransactionApplier applier ) throws Exception;
    }

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

    /*public static boolean apply( BatchTransactionApplier applier, TransactionToApply... transactions )
            throws Exception
    {
        for ( TransactionToApply tx : transactions )
        {
            try (TransactionApplier txApplier = applier.startTx( tx, new LockGroup() ))
            {
                tx.transactionRepresentation().accept( txApplier );
                        //function.apply( applier, tx.transactionRepresentation() );
            }
        }

        if ( !(applier instanceof CommandApplierFacade) )
        {
            // This is really odd... the whole apply/close bit. CommandApplierFacade is apparently
            // owning the call of apply itself. We'll just have to figure out why this is and then
            // merge apply/close. We can't have it like this.
            applier.apply();
        }

        applier.close();
        return false;
    }*/
}
