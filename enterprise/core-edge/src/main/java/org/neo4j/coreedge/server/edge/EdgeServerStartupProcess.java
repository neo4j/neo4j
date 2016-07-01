/*
 * Copyright (c) 2002-2016 "Neo Technology,"
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
package org.neo4j.coreedge.server.edge;

import java.util.concurrent.locks.LockSupport;

import org.neo4j.coreedge.catchup.storecopy.LocalDatabase;
import org.neo4j.coreedge.catchup.storecopy.edge.StoreFetcher;
import org.neo4j.coreedge.discovery.CoreServerSelectionException;
import org.neo4j.coreedge.discovery.EdgeTopologyService;
import org.neo4j.coreedge.raft.replication.tx.RetryStrategy;
import org.neo4j.coreedge.server.CoreMember;
import org.neo4j.coreedge.server.StoreId;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.impl.transaction.state.DataSourceManager;
import org.neo4j.kernel.lifecycle.Lifecycle;
import org.neo4j.logging.Log;
import org.neo4j.logging.LogProvider;

import static java.lang.String.format;
import static org.neo4j.coreedge.server.edge.EnterpriseEdgeEditionModule.extractBoltAddress;

public class EdgeServerStartupProcess implements Lifecycle
{
    private final StoreFetcher storeFetcher;
    private final LocalDatabase localDatabase;
    private final Lifecycle txPulling;
    private final DataSourceManager dataSourceManager;
    private final CoreServerSelectionStrategy connectionStrategy;
    private final Log log;
    private final EdgeTopologyService discoveryService;
    private final Config config;
    private final RetryStrategy.Timeout timeout;

    public EdgeServerStartupProcess(
            StoreFetcher storeFetcher,
            LocalDatabase localDatabase,
            Lifecycle txPulling,
            DataSourceManager dataSourceManager,
            CoreServerSelectionStrategy connectionStrategy,
            RetryStrategy retryStrategy,
            LogProvider logProvider, EdgeTopologyService discoveryService, Config config )
    {
        this.storeFetcher = storeFetcher;
        this.localDatabase = localDatabase;
        this.txPulling = txPulling;
        this.dataSourceManager = dataSourceManager;
        this.connectionStrategy = connectionStrategy;
        this.timeout = retryStrategy.newTimeout();
        this.log = logProvider.getLog( getClass() );
        this.discoveryService = discoveryService;
        this.config = config;
    }

    @Override
    public void init() throws Throwable
    {
        dataSourceManager.init();
        txPulling.init();
    }

    @Override
    public void start() throws Throwable
    {
        dataSourceManager.start();

        CoreMember coreMember = findCoreMemberToCopyFrom();
        if ( localDatabase.isEmpty() )
        {
            localDatabase.stop();
            localDatabase.copyStoreFrom( coreMember, storeFetcher );
            localDatabase.start();
        }
        else
        {
            StoreId localStoreId = localDatabase.storeId();
            StoreId remoteStoreId = storeFetcher.storeId( coreMember );
            if ( !localStoreId.equals( remoteStoreId ) )
            {
                throw new IllegalStateException( format( "This edge machine cannot join the cluster. " +
                                "The local database is not empty and has a mismatching storeId: expected %s actual %s.",
                        remoteStoreId, localStoreId ) );
            }
        }

        txPulling.start();
    }

    private CoreMember findCoreMemberToCopyFrom()
    {
        while ( true )
        {
            try
            {
                CoreMember coreMember = connectionStrategy.coreServer();
                log.info( "Server starting, connecting to core server at %s", coreMember.toString() );

                discoveryService.registerEdgeServer( extractBoltAddress( config ) );
                return coreMember;
            }
            catch ( CoreServerSelectionException ex )
            {
                log.info( "Failed to connect to core server. Retrying in %d ms.", timeout.getMillis() );
                LockSupport.parkUntil( timeout.getMillis() + System.currentTimeMillis() );
                timeout.increment();
            }
        }
    }

    @Override
    public void stop() throws Throwable
    {
        txPulling.stop();
        dataSourceManager.stop();
    }

    @Override
    public void shutdown() throws Throwable
    {
        txPulling.shutdown();
        dataSourceManager.shutdown();
    }
}
