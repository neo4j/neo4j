/**
 * Copyright (c) 2002-2014 "Neo Technology,"
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
package org.neo4j.kernel.impl.core;

import org.neo4j.helpers.Provider;
import org.neo4j.kernel.IdGeneratorFactory;
import org.neo4j.kernel.api.KernelAPI;
import org.neo4j.kernel.api.KernelTransaction;
import org.neo4j.kernel.api.exceptions.TransactionFailureException;
import org.neo4j.kernel.impl.nioneo.xa.TransactionRecordState;

/**
 * Creates a key within its own transaction, such that the command(s) for creating the key
 * will be alone in a transaction. If there is a running a transaction while calling this
 * it will be temporarily suspended meanwhile.
 */
public abstract class IsolatedTransactionTokenCreator implements TokenCreator
{
    protected final IdGeneratorFactory idGeneratorFactory;
    private final Provider<KernelAPI> kernelProvider;

    public IsolatedTransactionTokenCreator( Provider<KernelAPI> kernelProvider,
            IdGeneratorFactory idGeneratorFactory )
    {
        this.kernelProvider = kernelProvider;
        this.idGeneratorFactory = idGeneratorFactory;
    }

    @Override
    public synchronized int getOrCreate( String name )
    {
        KernelAPI kernel = kernelProvider.instance();
        KernelTransaction transaction = kernel.newTransaction();
        boolean success = false;
        try
        {
            int id = createKey( transaction.getTransactionRecordState(), name );
            success = true;
            return id;
        }
        finally
        {
            if ( !success )
            {
                try
                {
                    kernel.finish( transaction, success );
                }
                catch ( TransactionFailureException e )
                {
                    throw new org.neo4j.graphdb.TransactionFailureException(
                            "Failure to rollback after creating token failed", e );
                }
            }
        }
    }

    protected abstract int createKey( TransactionRecordState transactionRecordState, String name );
}
