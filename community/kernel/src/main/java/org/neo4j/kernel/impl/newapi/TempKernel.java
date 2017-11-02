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

import org.neo4j.graphdb.DependencyResolver;
import org.neo4j.internal.kernel.api.CursorFactory;
import org.neo4j.internal.kernel.api.ExplicitIndexRead;
import org.neo4j.internal.kernel.api.ExplicitIndexWrite;
import org.neo4j.internal.kernel.api.Kernel;
import org.neo4j.internal.kernel.api.Locks;
import org.neo4j.internal.kernel.api.Permissions;
import org.neo4j.internal.kernel.api.Read;
import org.neo4j.internal.kernel.api.SchemaRead;
import org.neo4j.internal.kernel.api.SchemaWrite;
import org.neo4j.internal.kernel.api.Session;
import org.neo4j.internal.kernel.api.Token;
import org.neo4j.internal.kernel.api.Write;
import org.neo4j.kernel.impl.api.KernelTransactions;
import org.neo4j.kernel.impl.storageengine.impl.recordstorage.RecordStorageEngine;
import org.neo4j.kernel.internal.GraphDatabaseAPI;

class TempKernel implements Kernel, Session
{
    private final Transaction tx;
    private final Cursors cursors;

    TempKernel( GraphDatabaseAPI db )
    {
        DependencyResolver resolver = db.getDependencyResolver();
        RecordStorageEngine engine = resolver.resolveDependency( RecordStorageEngine.class );
        KernelTransactions ktx = resolver.resolveDependency( KernelTransactions.class );
        this.tx = new Transaction( engine, ktx );
        this.cursors = new Cursors( tx );
    }

    @Override
    public Transaction beginTransaction()
    {
        return tx;
    }

    @Override
    public CursorFactory cursors()
    {
        return cursors;
    }

    @Override
    public Session beginSession( Permissions permissions )
    {
        return this;
    }

    @Override
    public Token token()
    {
        return tx;
    }

    @Override
    public void close()
    {
    }

    private static class Transaction extends AllStoreHolder implements org.neo4j.internal.kernel.api.Transaction
    {
        Transaction( RecordStorageEngine engine, KernelTransactions ktx )
        {
            super( engine, ktx.explicitIndexTxStateSupplier() );
        }

        @Override
        public long commit()
        {
            return READ_ONLY_TRANSACTION;
        }

        @Override
        public void rollback()
        {
        }

        @Override
        public Read dataRead()
        {
            return this;
        }

        @Override
        public Write dataWrite()
        {
            throw new UnsupportedOperationException( "not implemented" );
        }

        @Override
        public ExplicitIndexRead indexRead()
        {
            return this;
        }

        @Override
        public ExplicitIndexWrite indexWrite()
        {
            throw new UnsupportedOperationException( "not implemented" );
        }

        @Override
        public SchemaRead schemaRead()
        {
            return this;
        }

        @Override
        public SchemaWrite schemaWrite()
        {
            throw new UnsupportedOperationException( "not implemented" );
        }

        @Override
        public Locks locks()
        {
            throw new UnsupportedOperationException( "not implemented" );
        }
    }
}
