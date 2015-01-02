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
package org.neo4j.kernel.impl.nioneo.xa;

import org.neo4j.kernel.impl.persistence.EntityIdGenerator;
import org.neo4j.kernel.impl.persistence.PersistenceSource;
import org.neo4j.kernel.impl.transaction.XaDataSourceManager;
import org.neo4j.kernel.impl.transaction.xaframework.XaConnection;
import org.neo4j.kernel.impl.transaction.xaframework.XaDataSource;
import org.neo4j.kernel.lifecycle.LifecycleAdapter;

/**
 * The NioNeo persistence source implementation. If this class is registered as
 * persistence source for Neo4j kernel operations that are performed on the graph
 * will be forwarded to this class {@link NeoStoreTransaction} implementation.
 */
public class NioNeoDbPersistenceSource extends LifecycleAdapter implements PersistenceSource, EntityIdGenerator
{
    private final String dataSourceName = null;
    private final XaDataSourceManager xaDataSourceManager;

    public NioNeoDbPersistenceSource(XaDataSourceManager xaDataSourceManager)
    {
        assert(xaDataSourceManager != null);
        this.xaDataSourceManager = xaDataSourceManager;
    }

    @Override
    public NeoStoreTransaction createTransaction( XaConnection connection )
    {
        return ((NeoStoreXaConnection) connection).createTransaction();
    }

    @Override
    public String toString()
    {
        return "A persistence source to [" + dataSourceName + "]";
    }

    @Override
    public long nextId( Class<?> clazz )
    {
        return  xaDataSourceManager.getNeoStoreDataSource().nextId( clazz );
    }

    @Override
    public long getHighestPossibleIdInUse( Class<?> clazz )
    {
        return  xaDataSourceManager.getNeoStoreDataSource().getHighestPossibleIdInUse( clazz );
    }

    @Override
    public long getNumberOfIdsInUse( Class<?> clazz )
    {
        return  xaDataSourceManager.getNeoStoreDataSource().getNumberOfIdsInUse( clazz );
    }
    
    @Override
    public XaDataSource getXaDataSource()
    {
        return  xaDataSourceManager.getNeoStoreDataSource();
    }
}