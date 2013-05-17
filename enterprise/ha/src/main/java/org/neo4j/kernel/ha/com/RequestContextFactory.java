/**
 * Copyright (c) 2002-2013 "Neo Technology,"
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
package org.neo4j.kernel.ha.com;

import java.io.IOException;
import java.util.Collection;

import org.neo4j.com.RequestContext;
import org.neo4j.graphdb.DependencyResolver;
import org.neo4j.helpers.Pair;
import org.neo4j.kernel.impl.nioneo.xa.NeoStoreXaDataSource;
import org.neo4j.kernel.impl.transaction.AbstractTransactionManager;
import org.neo4j.kernel.impl.transaction.XaDataSourceManager;
import org.neo4j.kernel.impl.transaction.xaframework.XaDataSource;
import org.neo4j.kernel.impl.transaction.xaframework.XaLogicalLog;

public class RequestContextFactory
{
    private final long startupTime;
    private final int serverId;
    private final XaDataSourceManager xaDsm;
    private final DependencyResolver resolver;

    public RequestContextFactory( int serverId, XaDataSourceManager xaDsm, DependencyResolver resolver )
    {
        this.resolver = resolver;
        this.startupTime = System.currentTimeMillis();
        this.serverId = serverId;
        this.xaDsm = xaDsm;
    }

    public RequestContext newRequestContext( int eventIdentifier )
    {
        // Constructs a slave context from scratch.
        try
        {
            Collection<XaDataSource> dataSources = xaDsm.getAllRegisteredDataSources();
            RequestContext.Tx[] txs = new RequestContext.Tx[dataSources.size()];
            int i = 0;
            Pair<Integer,Long> master = null;
            for ( XaDataSource dataSource : dataSources )
            {
                long txId = dataSource.getLastCommittedTxId();
                if( dataSource.getName().equals( NeoStoreXaDataSource.DEFAULT_DATA_SOURCE_NAME ) )
                {
                    master = dataSource.getMasterForCommittedTx( txId );
                }
                txs[i++] = RequestContext.lastAppliedTx( dataSource.getName(), txId );
            }
            assert master != null : "master should not be null, since we should have found " + NeoStoreXaDataSource.DEFAULT_DATA_SOURCE_NAME;
            return new RequestContext( startupTime, serverId, eventIdentifier, txs, master.first(), master.other() );
        }
        catch ( IOException e )
        {
            throw new RuntimeException( e );
        }
    }

    public RequestContext newRequestContext( long sessionId, int machineId, int eventIdentifier )
    {
        try
        {
            Collection<XaDataSource> dataSources = xaDsm.getAllRegisteredDataSources();
            RequestContext.Tx[] txs = new RequestContext.Tx[dataSources.size()];
            int i = 0;
            Pair<Integer,Long> master = null;
            for ( XaDataSource dataSource : dataSources )
            {
                long txId = dataSource.getLastCommittedTxId();
                if( dataSource.getName().equals( NeoStoreXaDataSource.DEFAULT_DATA_SOURCE_NAME ) )
                {
                    master = dataSource.getMasterForCommittedTx( txId );
                }
                txs[i++] = RequestContext.lastAppliedTx( dataSource.getName(), txId );
            }
            assert master != null : "master should not be null, since we should have found " + NeoStoreXaDataSource.DEFAULT_DATA_SOURCE_NAME;
            return new RequestContext( sessionId, machineId, eventIdentifier, txs, master.first(), master.other() );
        }
        catch ( IOException e )
        {
            throw new RuntimeException( e );
        }
    }

    public RequestContext newRequestContext( XaDataSource dataSource, long sessionId, int machineId, int eventIdentifier )
    {
        try
        {
            long txId = dataSource.getLastCommittedTxId();
            RequestContext.Tx[] txs = new RequestContext.Tx[] { RequestContext.lastAppliedTx( dataSource.getName(), txId ) };
            Pair<Integer,Long> master = dataSource.getName().equals( NeoStoreXaDataSource.DEFAULT_DATA_SOURCE_NAME ) ?
                    dataSource.getMasterForCommittedTx( txId ) : Pair.of( XaLogicalLog.MASTER_ID_REPRESENTING_NO_MASTER, 0L );
            return new RequestContext( sessionId, machineId, eventIdentifier, txs, master.first(), master.other() );
        }
        catch ( IOException e )
        {
            throw new RuntimeException( e );
        }
    }

    public RequestContext newRequestContext( XaDataSource dataSource )
    {
        return newRequestContext( dataSource, startupTime, serverId,
                resolver.resolveDependency( AbstractTransactionManager.class ).getEventIdentifier() );
    }

    public RequestContext newRequestContext()
    {
        return newRequestContext( startupTime, serverId,
                resolver.resolveDependency( AbstractTransactionManager.class ).getEventIdentifier() );
    }
}
