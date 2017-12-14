/*
 * Copyright (c) 2002-2017 "Neo Technology,"
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

import io.netty.channel.ChannelHandlerContext;
import io.netty.util.concurrent.Future;

import java.io.IOException;

import org.neo4j.causalclustering.catchup.ResponseMessageType;
import org.neo4j.cursor.RawCursor;

public class StoreStreamingProtocol
{
    /**
     * This queues all the file sending operations on the outgoing pipeline, including
     * chunking {@link org.neo4j.causalclustering.catchup.storecopy.FileSender} handlers.
     * <p>
     * Note that we do not block here.
     */
    void stream( ChannelHandlerContext ctx, RawCursor<StoreResource,IOException> resources ) throws IOException
    {
        while ( resources.next() )
        {
            StoreResource resource = resources.get();

            ctx.write( ResponseMessageType.FILE );
            ctx.write( new FileHeader( resource.path(), resource.recordSize() ) );
            ctx.write( new FileSender( resource ) );
        }
    }

    Future<Void> end( ChannelHandlerContext ctx, StoreCopyFinishedResponse.Status status, long lastCommittedTxBeforeStoreCopy )
    {
        ctx.write( ResponseMessageType.STORE_COPY_FINISHED );
        return ctx.writeAndFlush( new StoreCopyFinishedResponse( status, lastCommittedTxBeforeStoreCopy ) );
    }
}
