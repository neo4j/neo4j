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

import org.neo4j.internal.kernel.api.CursorFactory;
import org.neo4j.internal.kernel.api.Kernel;
import org.neo4j.internal.kernel.api.security.SecurityContext;
import org.neo4j.kernel.api.InwardKernel;
import org.neo4j.kernel.impl.api.KernelTransactions;
import org.neo4j.storageengine.api.StorageEngine;

/**
 * This is a temporary implementation of the Kernel API, used to enable early testing. The plan is to merge this
 * class with org.neo4j.kernel.impl.api.Kernel.
 */
public class NewKernel implements Kernel
{
    private final Cursors cursors;
    private final StorageEngine engine;
    private final InwardKernel kernel;
    private final Read read;

    public NewKernel( StorageEngine engine, KernelTransactions ktxs, InwardKernel kernel )
    {
        this.engine = engine;
        this.kernel = kernel;
        // This extra statement will be remove once we start adding tx-state. That is because
        // by then we cannot use a global Read in the CursorFactory, but need to use transaction
        // specific Read instances which are given to the Cursors on initialization.
        this.read = new AllStoreHolder( engine, engine.storeReadLayer().newStatement(), ktxs.explicitIndexTxStateSupplier() );
        this.cursors = new Cursors( read );
    }

    @Override
    public CursorFactory cursors()
    {
        return cursors;
    }

    @Override
    public org.neo4j.internal.kernel.api.Session beginSession( SecurityContext securityContext )
    {
        return new KernelSession( engine, kernel, securityContext );
    }
}
