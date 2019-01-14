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
package org.neo4j.kernel.impl.newapi;

import org.neo4j.internal.kernel.api.Session;
import org.neo4j.internal.kernel.api.Transaction;
import org.neo4j.internal.kernel.api.exceptions.TransactionFailureException;
import org.neo4j.internal.kernel.api.security.LoginContext;
import org.neo4j.kernel.api.InwardKernel;
import org.neo4j.kernel.api.KernelTransaction;

/**
 * Implements a Kernel API Session.
 */
class KernelSession implements Session
{
    private final InwardKernel kernel;
    private final LoginContext loginContext;

    KernelSession( InwardKernel kernel, LoginContext loginContext )
    {
        this.kernel = kernel;
        this.loginContext = loginContext;
    }

    @Override
    public Transaction beginTransaction() throws TransactionFailureException
    {
        return beginTransaction( Transaction.Type.explicit );
    }

    @Override
    public Transaction beginTransaction( KernelTransaction.Type type ) throws TransactionFailureException
    {
        return kernel.newTransaction( type, loginContext );
    }

    @Override
    public void close()
    {
    }
}
