/**
 * Copyright (c) 2002-2015 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
 *
 * This file is part of Neo4j.
 *
 * Neo4j is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program. If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.kernel.ha.transaction;

import java.io.IOException;
import java.nio.channels.ReadableByteChannel;

import javax.transaction.xa.XAException;

import org.neo4j.com.ComException;
import org.neo4j.com.Response;
import org.neo4j.com.TxExtractor;
import org.neo4j.helpers.Exceptions;
import org.neo4j.kernel.ha.HaXaDataSourceManager;
import org.neo4j.kernel.ha.com.RequestContextFactory;
import org.neo4j.kernel.ha.com.master.Master;
import org.neo4j.kernel.impl.core.TransactionState;
import org.neo4j.kernel.impl.transaction.AbstractTransactionManager;
import org.neo4j.kernel.impl.transaction.xaframework.LogBuffer;
import org.neo4j.kernel.impl.transaction.xaframework.TxIdGenerator;
import org.neo4j.kernel.impl.transaction.xaframework.XaDataSource;

public class SlaveTxIdGenerator implements TxIdGenerator
{
    private final int serverId;
    private final Master master;
    private final int masterId;
    private final RequestContextFactory requestContextFactory;
    private final HaXaDataSourceManager xaDsm;
    private final AbstractTransactionManager txManager;

    public SlaveTxIdGenerator( int serverId, Master master, int masterId, RequestContextFactory requestContextFactory,
                               HaXaDataSourceManager xaDsm, AbstractTransactionManager txManager )
    {
        this.serverId = serverId;
        this.masterId = masterId;
        this.requestContextFactory = requestContextFactory;
        this.master = master;
        this.xaDsm = xaDsm;
        this.txManager = txManager;
    }

    @Override
    public long generate( XaDataSource dataSource, int identifier ) throws XAException
    {
        try
        {
            // For the first resource to commit against, make sure the master tx is initialized. This is sub
            // optimal to do here, since we are under a synchronized block, but writing to master from slaves
            // is discouraged in any case. For details of the background for this call, see TransactionState
            // and its isRemoteInitialized method.
            TransactionState txState = txManager.getTransactionState();
            txState.getTxHook().remotelyInitializeTransaction( txManager.getEventIdentifier(), txState );

            Response<Long> response = master.commitSingleResourceTransaction(
                    requestContextFactory.newRequestContext( dataSource ), dataSource.getName(),
                    myPreparedTransactionToCommit( dataSource, identifier ) );
            xaDsm.applyTransactions( response );
            return response.response().longValue();
        }
        catch ( ComException e )
        {
            throw Exceptions.withCause( new XAException( XAException.XA_HEURCOM ), e );
        }
        catch (RuntimeException e)
        {
            // If the original issue was caused by an XAException, wrap the whole thing in an XA exception with
            // the same error code and message.
            Throwable currentException = e.getCause();
            while(currentException != null)
            {
                if( currentException instanceof XAException )
                {
                    throw Exceptions.withCause( new XAException( ((XAException) currentException).errorCode ), e );
                }
                currentException = currentException.getCause();
            }

            // If no XAException involved, just throw the runtime exception.
            throw e;
        }
    }

    @Override
    public void committed( XaDataSource dataSource, int identifier, long txId, Integer externalAuthorServerId )
    {
        master.pushTransaction(
                requestContextFactory.newRequestContext( identifier ), dataSource.getName(), txId ).close();
    }

    @Override
    public int getCurrentMasterId()
    {
        return masterId;
    }

    @Override
    public int getMyId()
    {
        return serverId;
    }

    private TxExtractor myPreparedTransactionToCommit( final XaDataSource dataSource, final int identifier )
    {
        return new TxExtractor()
        {
            @Override
            public ReadableByteChannel extract()
            {
                throw new UnsupportedOperationException();
            }
            
            @Override
            public void extract( LogBuffer buffer )
            {
                try
                {
                    dataSource.getPreparedTransaction( identifier, buffer );
                }
                catch ( IOException e )
                {
                    throw new RuntimeException( e );
                }
            }
        };
    }
}
