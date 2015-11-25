/*
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
package org.neo4j.coreedge.server.edge;

import org.neo4j.coreedge.discovery.ClusterTopology;
import org.neo4j.coreedge.catchup.storecopy.edge.StoreFetcher;
import org.neo4j.coreedge.server.AdvertisedSocketAddress;
import org.neo4j.coreedge.catchup.tx.edge.TxPollingClient;
import org.neo4j.coreedge.catchup.storecopy.LocalDatabase;
import org.neo4j.coreedge.discovery.EdgeDiscoveryService;
import org.neo4j.kernel.impl.transaction.state.DataSourceManager;
import org.neo4j.kernel.lifecycle.Lifecycle;

public class EdgeServerStartupProcess implements Lifecycle
{
    private final StoreFetcher storeFetcher;
    private final LocalDatabase localDatabase;
    private final TxPollingClient txPuller;
    private final EdgeDiscoveryService discoveryService;
    private final DataSourceManager dataSourceManager;

    public EdgeServerStartupProcess( StoreFetcher storeFetcher, LocalDatabase localDatabase,
                                     TxPollingClient txPuller, EdgeDiscoveryService discoveryService,
                                     DataSourceManager dataSourceManager )


    {
        this.storeFetcher = storeFetcher;
        this.localDatabase = localDatabase;
        this.txPuller = txPuller;
        this.discoveryService = discoveryService;
        this.dataSourceManager = dataSourceManager;
    }

    @Override
    public void init() throws Throwable
    {
        dataSourceManager.init();
    }

    @Override
    public void start() throws Throwable
    {
        ClusterTopology clusterTopology = discoveryService.currentTopology();

        AdvertisedSocketAddress transactionServer = clusterTopology.firstTransactionServer();
        localDatabase.copyStoreFrom( transactionServer, storeFetcher );

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