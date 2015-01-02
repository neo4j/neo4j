/**
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
package org.neo4j.kernel.impl.index;

import javax.transaction.TransactionManager;

import org.neo4j.kernel.impl.core.ReadOnlyDbException;
import org.neo4j.kernel.impl.transaction.xaframework.XaConnection;

public class ReadOnlyIndexConnectionBroker<T extends XaConnection> extends IndexConnectionBroker<T>
{
    public ReadOnlyIndexConnectionBroker( TransactionManager transactionManager )
    {
        super( transactionManager );
    }
    
    @Override
    public T acquireResourceConnection()
    {
        throw new ReadOnlyDbException();
    }
    
    @Override
    public T acquireReadOnlyResourceConnection()
    {
        return null;
    }
    
    @Override
    protected T newConnection()
    {
        throw new ReadOnlyDbException();
    }
}
