/*
 * Copyright (c) 2002-2018 "Neo Technology,"
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
package org.neo4j.causalclustering.catchup.storecopy;

import io.netty.channel.ChannelHandler;

import java.util.function.Supplier;

import org.neo4j.causalclustering.StrippedCatchupServer;
import org.neo4j.causalclustering.catchup.CatchupServerProtocol;
import org.neo4j.graphdb.DependencyResolver;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.kernel.NeoStoreDataSource;
import org.neo4j.kernel.impl.transaction.log.checkpoint.CheckPointer;
import org.neo4j.kernel.impl.transaction.log.checkpoint.StoreCopyCheckPointMutex;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.logging.LogProvider;
import org.neo4j.logging.NullLogProvider;

/**
 * A standalone catchup server that uses production dependencies
 * (i.e. only relevant subset of a neo4j cluster instance)
 */
class RealStrippedCatchupServer extends StrippedCatchupServer
{
    private final StoreCopyCheckPointMutex storeCopyCheckPointMutex;
    private final Supplier<CheckPointer> checkPointer;
    private final Supplier<NeoStoreDataSource> neoStoreDataSourceSupplier;
    private final PageCache pageCache;
    private final FileSystemAbstraction fsa;
    private final LogProvider logProvider;

    RealStrippedCatchupServer( FileSystemAbstraction fsa, GraphDatabaseAPI graphDb )
    {
        this.fsa = fsa;
        DependencyResolver dependencyResolver = graphDb.getDependencyResolver();
        storeCopyCheckPointMutex = dependencyResolver.resolveDependency( StoreCopyCheckPointMutex.class );
        checkPointer = () -> getCheckPointer( graphDb );
        neoStoreDataSourceSupplier = () -> getNeoStoreDataSource( graphDb );
        pageCache = graphDb.getDependencyResolver().resolveDependency( PageCache.class );
        logProvider = NullLogProvider.getInstance();
    }

    @Override
    protected ChannelHandler getStoreFileRequestHandler( CatchupServerProtocol protocol )
    {
        return new GetStoreFileRequestHandler( protocol, neoStoreDataSourceSupplier, checkPointer, new StoreFileStreamingProtocol(),
                pageCache, fsa, logProvider );
    }

    @Override
    protected ChannelHandler getStoreListingRequestHandler( CatchupServerProtocol protocol )
    {
        PrepareStoreCopyFilesProvider prepareStoreCopyFilesProvider = new PrepareStoreCopyFilesProvider( pageCache, fsa );
        return new PrepareStoreCopyRequestHandler( protocol, checkPointer, storeCopyCheckPointMutex, neoStoreDataSourceSupplier,
                prepareStoreCopyFilesProvider );
    }

    @Override
    protected ChannelHandler getIndexRequestHandler( CatchupServerProtocol protocol )
    {
        return new GetIndexSnapshotRequestHandler( protocol, neoStoreDataSourceSupplier, checkPointer,
                new StoreFileStreamingProtocol(), pageCache, fsa );
    }

    static CheckPointer getCheckPointer( GraphDatabaseAPI graphDb )
    {
        return graphDb.getDependencyResolver().resolveDependency( CheckPointer.class );
    }

    static NeoStoreDataSource getNeoStoreDataSource( GraphDatabaseAPI graphDb )
    {
        return graphDb.getDependencyResolver().resolveDependency( NeoStoreDataSource.class );
    }
}
