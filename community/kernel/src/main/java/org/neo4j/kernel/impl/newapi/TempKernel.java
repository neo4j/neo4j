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
import org.neo4j.internal.kernel.api.KernelAPI;
import org.neo4j.internal.kernel.api.Token;
import org.neo4j.kernel.impl.storageengine.impl.recordstorage.RecordStorageEngine;
import org.neo4j.kernel.impl.store.NeoStores;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.values.storable.Value;

class TempKernel implements KernelAPI
{
    private final Transaction tx;
    private final Cursors cursors;

    TempKernel( GraphDatabaseAPI db )
    {
        RecordStorageEngine engine = db.getDependencyResolver().resolveDependency( RecordStorageEngine.class );
        this.tx = new Transaction( engine.testAccessNeoStores() );
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
    public Token token()
    {
        throw new UnsupportedOperationException( "not implemented" );
    }

    private static class Transaction extends Read implements org.neo4j.internal.kernel.api.Transaction
    {
        Transaction( NeoStores stores )
        {
            super( stores );
        }

        @Override
        public void success()
        {
        }

        @Override
        public void failure()
        {
        }

        @Override
        public void close() throws Exception
        {
        }

        @Override
        public long nodeCreate()
        {
            throw new UnsupportedOperationException( "not implemented" );
        }

        @Override
        public void nodeDelete( long node )
        {
            throw new UnsupportedOperationException( "not implemented" );
        }

        @Override
        public long relationshipCreate( long sourceNode, int relationshipLabel, long targetNode )
        {
            throw new UnsupportedOperationException( "not implemented" );
        }

        @Override
        public void relationshipDelete( long relationship )
        {
            throw new UnsupportedOperationException( "not implemented" );
        }

        @Override
        public void nodeAddLabel( long node, int nodeLabel )
        {
            throw new UnsupportedOperationException( "not implemented" );
        }

        @Override
        public void nodeRemoveLabel( long node, int nodeLabel )
        {
            throw new UnsupportedOperationException( "not implemented" );
        }

        @Override
        public void nodeSetProperty( long node, int propertyKey, Object value )
        {
            throw new UnsupportedOperationException( "not implemented" );
        }

        @Override
        public void nodeRemoveProperty( long node, int propertyKey )
        {
            throw new UnsupportedOperationException( "not implemented" );
        }

        @Override
        public void relationshipSetProperty( long relationship, int propertyKey, Value value )
        {
            throw new UnsupportedOperationException( "not implemented" );
        }

        @Override
        public void relationshipRemoveProperty( long node, int propertyKey )
        {
            throw new UnsupportedOperationException( "not implemented" );
        }
    }
}
