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
package org.neo4j.kernel.impl.core;

import java.util.function.IntPredicate;
import java.util.function.Supplier;

import org.neo4j.exceptions.KernelException;
import org.neo4j.kernel.api.Kernel;
import org.neo4j.kernel.api.KernelTransaction;
import org.neo4j.kernel.api.KernelTransaction.Type;
import org.neo4j.token.TokenCreator;

import static org.neo4j.internal.kernel.api.security.LoginContext.AUTH_DISABLED;

/**
 * Creates a key within its own transaction, such that the command(s) for creating the key
 * will be alone in a transaction. If there is a running a transaction while calling this
 * it will be temporarily suspended meanwhile.
 */
abstract class IsolatedTransactionTokenCreator implements TokenCreator
{
    private final Supplier<Kernel> kernelSupplier;

    IsolatedTransactionTokenCreator( Supplier<Kernel> kernelSupplier )
    {
        this.kernelSupplier = kernelSupplier;
    }

    @Override
    public synchronized int createToken( String name, boolean internal ) throws KernelException
    {
        Kernel kernel = kernelSupplier.get();
        try ( KernelTransaction tx = kernel.beginTransaction( Type.IMPLICIT, AUTH_DISABLED ) )
        {
            int id = createKey( tx, name, internal );
            tx.commit();
            return id;
        }
    }

    @Override
    public synchronized void createTokens( String[] names, int[] ids, boolean internal, IntPredicate filter ) throws KernelException
    {
        Kernel kernel = kernelSupplier.get();
        try ( KernelTransaction tx = kernel.beginTransaction( Type.IMPLICIT, AUTH_DISABLED ) )
        {
            for ( int i = 0; i < ids.length; i++ )
            {
                if ( filter.test( i ) )
                {
                    ids[i] = createKey( tx, names[i], internal );
                }
            }
            tx.commit();
        }
    }

    abstract int createKey( KernelTransaction transaction, String name, boolean internal ) throws KernelException;
}
