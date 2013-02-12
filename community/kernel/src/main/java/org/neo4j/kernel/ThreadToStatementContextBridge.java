/**
 * Copyright (c) 2002-2013 "Neo Technology,"
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
package org.neo4j.kernel;

import org.neo4j.graphdb.NotInTransactionException;
import org.neo4j.kernel.api.KernelAPI;
import org.neo4j.kernel.api.StatementContext;
import org.neo4j.kernel.impl.nioneo.xa.NeoStoreXaDataSource;
import org.neo4j.kernel.impl.transaction.AbstractTransactionManager;
import org.neo4j.kernel.impl.transaction.DataSourceRegistrationListener;
import org.neo4j.kernel.impl.transaction.XaDataSourceManager;
import org.neo4j.kernel.impl.transaction.xaframework.XaDataSource;
import org.neo4j.kernel.lifecycle.LifecycleAdapter;

/**
 * This is meant to serve as the bridge that makes the Beans API tie transactions to threads. The Beans API
 * will use this to get the appropriate {@link StatementContext} when it performs operations.
 */
public class ThreadToStatementContextBridge extends LifecycleAdapter
{
    private final KernelAPI kernelAPI;
    private StatementContext readOnlyStatementCtx;
    private final AbstractTransactionManager txManager;
    private final XaDataSourceManager xaDataSourceManager;

    public ThreadToStatementContextBridge( KernelAPI kernelAPI,
            AbstractTransactionManager txManager, XaDataSourceManager xaDataSourceManager )
    {
        this.kernelAPI = kernelAPI;
        this.txManager = txManager;
        this.xaDataSourceManager = xaDataSourceManager;
    }
    
    @Override
    public void start()
    {
        xaDataSourceManager.addDataSourceRegistrationListener( new DataSourceRegistrationListener.Adapter()
        {
            @Override
            public void registeredDataSource( XaDataSource ds )
            {
                if ( ds.getName().equals( NeoStoreXaDataSource.DEFAULT_DATA_SOURCE_NAME ) )
                {
                    readOnlyStatementCtx = kernelAPI.newReadOnlyStatementContext();
                }
            }
        } );
    }

    public StatementContext getCtxForReading()
    {
        StatementContext ctx = getStatementContext();
        if(ctx != null)
        {
            return ctx;
        }

        return readOnlyStatementCtx;
    }

    public StatementContext getCtxForWriting()
    {
        StatementContext ctx = getStatementContext();
        if(ctx != null)
        {
            return ctx;
        }

        throw new NotInTransactionException( "You have to start a transaction to perform write operations." );
    }

    private StatementContext getStatementContext()
    {
        return txManager.getStatementContext();
    }
}
