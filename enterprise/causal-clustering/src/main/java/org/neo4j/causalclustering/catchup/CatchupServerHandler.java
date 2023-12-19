/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 *
 * This file is part of Neo4j Enterprise Edition. The included source
 * code can be redistributed and/or modified under the terms of the
 * Neo4j Sweden Software License, as found in the associated LICENSE.txt
 * file.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * Neo4j Sweden Software License for more details.
 */
package org.neo4j.causalclustering.catchup;

import io.netty.channel.ChannelHandler;

import java.util.Optional;

public interface CatchupServerHandler
{
    ChannelHandler txPullRequestHandler( CatchupServerProtocol catchupServerProtocol );

    ChannelHandler getStoreIdRequestHandler( CatchupServerProtocol catchupServerProtocol );

    ChannelHandler storeListingRequestHandler( CatchupServerProtocol catchupServerProtocol );

    ChannelHandler getStoreFileRequestHandler( CatchupServerProtocol catchupServerProtocol );

    ChannelHandler getIndexSnapshotRequestHandler( CatchupServerProtocol catchupServerProtocol );

    Optional<ChannelHandler> snapshotHandler( CatchupServerProtocol catchupServerProtocol );
}
