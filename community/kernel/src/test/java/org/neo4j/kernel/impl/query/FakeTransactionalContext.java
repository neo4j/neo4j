/*
 * Copyright (c) 2002-2016 "Neo Technology,"
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
package org.neo4j.kernel.impl.query;

import org.neo4j.graphdb.Lock;
import org.neo4j.graphdb.PropertyContainer;
import org.neo4j.kernel.GraphDatabaseQueryService;
import org.neo4j.kernel.api.KernelTransaction;
import org.neo4j.kernel.api.ReadOperations;
import org.neo4j.kernel.api.Statement;
import org.neo4j.kernel.api.dbms.DbmsOperations;
import org.neo4j.kernel.api.security.AccessMode;
import org.neo4j.kernel.api.txstate.TxStateHolder;

public class FakeTransactionalContext implements TransactionalContext
{
    private final AccessMode mode;

    public FakeTransactionalContext( AccessMode mode ) {
        this.mode = mode;
    }

    @Override
    public ReadOperations readOperations()
    {
        throw new UnsupportedOperationException( "fake test class" );
    }

    @Override
    public DbmsOperations dbmsOperations()
    {
        throw new UnsupportedOperationException( "fake test class" );
    }

    @Override
    public boolean isTopLevelTx()
    {
        throw new UnsupportedOperationException( "fake test class" );
    }

    @Override
    public void close( boolean success )
    {
        throw new UnsupportedOperationException( "fake test class" );
    }

    @Override
    public void commitAndRestartTx()
    {
        throw new UnsupportedOperationException( "fake test class" );
    }

    @Override
    public void cleanForReuse()
    {
        throw new UnsupportedOperationException( "fake test class" );
    }

    @Override
    public TransactionalContext provideContext()
    {
        throw new UnsupportedOperationException( "fake test class" );
    }

    @Override
    public boolean isOpen()
    {
        throw new UnsupportedOperationException( "fake test class" );
    }

    @Override
    public GraphDatabaseQueryService graph()
    {
        throw new UnsupportedOperationException( "fake test class" );
    }

    @Override
    public Statement statement()
    {
        throw new UnsupportedOperationException( "fake test class" );
    }

    @Override
    public TxStateHolder stateView()
    {
        throw new UnsupportedOperationException( "fake test class" );
    }

    @Override
    public Lock acquireWriteLock( PropertyContainer p )
    {
        throw new UnsupportedOperationException( "fake test class" );
    }

    @Override
    public AccessMode accessMode()
    {
        return mode;
    }

    @Override
    public KernelTransaction.Revertable restrictCurrentTransaction( AccessMode accessMode )
    {
        throw new UnsupportedOperationException( "fake test class" );
    }
}
