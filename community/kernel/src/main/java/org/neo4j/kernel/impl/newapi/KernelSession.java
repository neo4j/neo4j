/*
 * Copyright (c) 2002-2017 "Neo Technology,"
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
package org.neo4j.kernel.impl.newapi;

import org.neo4j.internal.kernel.api.Session;
import org.neo4j.internal.kernel.api.Transaction;
import org.neo4j.internal.kernel.api.exceptions.KernelException;
import org.neo4j.internal.kernel.api.security.SecurityContext;
import org.neo4j.kernel.api.InwardKernel;
import org.neo4j.kernel.api.KernelTransaction;
import org.neo4j.storageengine.api.StorageEngine;

/**
 * Implements a Kernel API Session.
 */
class KernelSession implements Session
{
    private final InwardKernel kernel;
    private final SecurityContext securityContext;
    private final KernelToken token;

    KernelSession( StorageEngine engine, InwardKernel kernel, SecurityContext securityContext )
    {
        this.kernel = kernel;
        this.securityContext = securityContext;
        this.token = new KernelToken( engine );
    }

    @Override
    public Transaction beginTransaction() throws KernelException
    {
        return beginTransaction( Transaction.Type.explicit );
    }

    @Override
    public Transaction beginTransaction( KernelTransaction.Type type ) throws KernelException
    {
        return kernel.newTransaction( type, securityContext );
    }

    @Override
    public org.neo4j.internal.kernel.api.Token token()
    {
        return token;
    }

    @Override
    public void close()
    {
    }
}
