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

import org.neo4j.graphdb.DatabaseShutdownException;
import org.neo4j.graphdb.NotInTransactionException;
import org.neo4j.kernel.api.KernelAPI;
import org.neo4j.kernel.api.StatementOperationParts;
import org.neo4j.kernel.api.StatementOperations;
import org.neo4j.kernel.api.operations.ReadOnlyStatementState;
import org.neo4j.kernel.api.operations.StatementState;
import org.neo4j.kernel.impl.api.IndexReaderFactory;
import org.neo4j.kernel.impl.api.index.IndexingService;
import org.neo4j.kernel.impl.nioneo.xa.NeoStoreXaDataSource;
import org.neo4j.kernel.impl.transaction.AbstractTransactionManager;
import org.neo4j.kernel.impl.transaction.DataSourceRegistrationListener;
import org.neo4j.kernel.impl.transaction.XaDataSourceManager;
import org.neo4j.kernel.impl.transaction.xaframework.XaDataSource;
import org.neo4j.kernel.lifecycle.LifecycleAdapter;

import static org.neo4j.kernel.impl.transaction.XaDataSourceManager.neoStoreListener;

/**
 * This is meant to serve as the bridge that makes the Beans API tie transactions to threads. The Beans API
 * will use this to get the appropriate {@link StatementOperations} when it performs operations.
 */
public class ThreadToStatementContextBridge extends LifecycleAdapter
{
    final KernelAPI kernelAPI;
    private final AbstractTransactionManager txManager;
    private boolean isShutdown = false;
    private final XaDataSourceManager xaDataSourceManager;
    private IndexingService indexingService;

    public ThreadToStatementContextBridge( KernelAPI kernelAPI, AbstractTransactionManager txManager,
            XaDataSourceManager xaDataSourceManager )
    {
        this.kernelAPI = kernelAPI;
        this.txManager = txManager;
        this.xaDataSourceManager = xaDataSourceManager;
    }
    
    @Override
    public void start() throws Throwable
    {
        xaDataSourceManager.addDataSourceRegistrationListener( neoStoreListener( new DataSourceRegistrationListener.Adapter()
        {
            @Override
            public void registeredDataSource( XaDataSource ds )
            {
                indexingService = ((NeoStoreXaDataSource)ds).getIndexService();
            }
        } ) );
    }
    
    public StatementOperationParts getCtxForReading()
    {
        return kernelAPI.readOnlyStatementOperations();
    }

    public StatementOperationParts getCtxForWriting()
    {
        return kernelAPI.statementOperations();
    }
    
    public StatementState statementForReading()
    {
        StatementState statement = newStatementIfInTx();
        if ( statement != null )
        {
            return statement;
        }
        return new ReadOnlyStatementState( new IndexReaderFactory.Caching( indexingService ) );
    }
    
    public StatementState statementForWriting()
    {
        StatementState statement = newStatementIfInTx();
        if ( statement != null )
        {
            return statement;
        }
        throw new NotInTransactionException();
    }

    private StatementState newStatementIfInTx()
    {
        checkIfShutdown();
        return txManager.newStatement();
    }

    @Override
    public void shutdown() throws Throwable
    {
        isShutdown = true;
    }

    private void checkIfShutdown()
    {
        if ( isShutdown )
        {
            throw new DatabaseShutdownException();
        }
    }
    
    public static class ReadOnly extends ThreadToStatementContextBridge
    {
        public ReadOnly( KernelAPI kernelAPI, AbstractTransactionManager txManager,
                XaDataSourceManager xaDataSourceManager )
        {
            super( kernelAPI, txManager, xaDataSourceManager );
        }
        
        @Override
        public StatementOperationParts getCtxForWriting()
        {
            return kernelAPI.readOnlyStatementOperations();
        }

        @Override
        public StatementOperationParts getCtxForReading()
        {
            return kernelAPI.readOnlyStatementOperations();
        }
    }
}
