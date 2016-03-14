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

import org.neo4j.coreedge.catchup.storecopy.LocalDatabase;
import org.neo4j.coreedge.catchup.storecopy.edge.StoreFetcher;
import org.neo4j.coreedge.catchup.tx.edge.TxPollingClient;
import org.neo4j.coreedge.discovery.CoreServerSelectionException;
import org.neo4j.coreedge.raft.replication.tx.RetryStrategy;
import org.neo4j.coreedge.server.AdvertisedSocketAddress;
import org.neo4j.kernel.impl.transaction.state.DataSourceManager;
import org.neo4j.kernel.lifecycle.Lifecycle;
import org.neo4j.logging.Log;
import org.neo4j.logging.LogProvider;

public class EdgeServerStartupProcess implements Lifecycle
{
    private final StoreFetcher storeFetcher;
    private final LocalDatabase localDatabase;
    private final TxPollingClient txPuller;
    private final DataSourceManager dataSourceManager;
    private final CoreServerSelectionStrategy connectionStrategy;
    private final Log log;
    private final RetryStrategy.Timeout timeout;

    public EdgeServerStartupProcess( StoreFetcher storeFetcher, LocalDatabase localDatabase,
                                     TxPollingClient txPuller,
                                     DataSourceManager dataSourceManager,
                                     CoreServerSelectionStrategy connectionStrategy,
                                     RetryStrategy retryStrategy,
                                     LogProvider logProvider )
    {
        this.storeFetcher = storeFetcher;
        this.localDatabase = localDatabase;
        this.txPuller = txPuller;
        this.dataSourceManager = dataSourceManager;
        this.connectionStrategy = connectionStrategy;
        this.timeout = retryStrategy.newTimeout();
        this.log = logProvider.getLog( getClass() );
    }

    @Override
    public void init() throws Throwable
    {
        dataSourceManager.init();
    }

    @Override
    public void start() throws Throwable
    {
        boolean copiedStore = false;
        do
        {
            try
            {
                AdvertisedSocketAddress transactionServer = connectionStrategy.coreServer();
                log.info( "Server starting, connecting to core server at %s", transactionServer.toString() );
                localDatabase.copyStoreFrom( transactionServer, storeFetcher );
                copiedStore = true;
            }
            catch ( CoreServerSelectionException ex )
            {
                log.info( "Failed to connect to core server. Retrying in %d ms.", timeout.getMillis() );
                Thread.sleep( timeout.getMillis() );
                timeout.increment();
            }

        } while ( !copiedStore );


        dataSourceManager.start();
        txPuller.startPolling();
    }

    @Override
    public void stop() throws Throwable
    {
        txPuller.stop();
        dataSourceManager.stop();
    }

    @Override
    public void shutdown() throws Throwable
    {
        dataSourceManager.shutdown();
    }

}
