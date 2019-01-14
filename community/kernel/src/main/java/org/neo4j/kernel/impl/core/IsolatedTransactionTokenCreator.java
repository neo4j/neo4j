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

import java.util.function.Supplier;

import org.neo4j.internal.kernel.api.Kernel;
import org.neo4j.internal.kernel.api.Session;
import org.neo4j.internal.kernel.api.Transaction;
import org.neo4j.internal.kernel.api.Transaction.Type;
import org.neo4j.internal.kernel.api.exceptions.KernelException;
import org.neo4j.internal.kernel.api.exceptions.schema.IllegalTokenNameException;
import org.neo4j.internal.kernel.api.exceptions.schema.TooManyLabelsException;
import org.neo4j.internal.kernel.api.security.LoginContext;
import org.neo4j.kernel.impl.store.id.IdGeneratorFactory;

/**
 * Creates a key within its own transaction, such that the command(s) for creating the key
 * will be alone in a transaction. If there is a running a transaction while calling this
 * it will be temporarily suspended meanwhile.
 */
abstract class IsolatedTransactionTokenCreator implements TokenCreator
{
    protected final IdGeneratorFactory idGeneratorFactory;
    private final Supplier<Kernel> kernelSupplier;

    IsolatedTransactionTokenCreator( Supplier<Kernel> kernelSupplier,
            IdGeneratorFactory idGeneratorFactory )
    {
        this.kernelSupplier = kernelSupplier;
        this.idGeneratorFactory = idGeneratorFactory;
    }

    @Override
    public synchronized int getOrCreate( String name ) throws KernelException
    {
        Kernel kernel = kernelSupplier.get();
        try ( Session session = kernel.beginSession( LoginContext.AUTH_DISABLED ) )
        {
            try ( Transaction transaction = session.beginTransaction( Type.implicit ) )
            {
                int id = createKey( transaction, name );
                transaction.success();
                return id;
            }
        }
    }

    abstract int createKey( Transaction transaction, String name )
            throws IllegalTokenNameException, TooManyLabelsException;
}
