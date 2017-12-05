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
import java.util.function.Supplier;

import org.neo4j.cursor.RawCursor;
import org.neo4j.graphdb.Resource;
import org.neo4j.kernel.impl.transaction.log.checkpoint.CheckPointer;
import org.neo4j.kernel.impl.transaction.log.checkpoint.SimpleTriggerInfo;
import org.neo4j.kernel.impl.transaction.log.checkpoint.StoreCopyCheckPointMutex;

import static org.neo4j.causalclustering.catchup.storecopy.StoreCopyFinishedResponse.Status.SUCCESS;

public class StoreStreamingProcess
{
    private final StoreStreamingProtocol protocol;
    private final Supplier<CheckPointer> checkPointerSupplier;
    private final StoreCopyCheckPointMutex mutex;
    private final StoreResourceStreamFactory resourceStreamFactory;

    public StoreStreamingProcess( StoreStreamingProtocol protocol, Supplier<CheckPointer> checkPointerSupplier, StoreCopyCheckPointMutex mutex,
            StoreResourceStreamFactory resourceStreamFactory )
    {
        this.protocol = protocol;
        this.checkPointerSupplier = checkPointerSupplier;
        this.mutex = mutex;
        this.resourceStreamFactory = resourceStreamFactory;
    }

    void perform( ChannelHandlerContext ctx ) throws IOException
    {
        CheckPointer checkPointer = checkPointerSupplier.get();
        Resource checkPointLock = mutex.storeCopy( () -> checkPointer.tryCheckPoint( new SimpleTriggerInfo( "Store copy" ) ) );

        Future<Void> completion = null;
        try ( RawCursor<StoreResource,IOException> resources = resourceStreamFactory.create() )
        {
            protocol.stream( ctx, resources );
            completion = protocol.end( ctx, SUCCESS, checkPointer.lastCheckPointedTransactionId() );
        }
        finally
        {
            if ( completion != null )
            {
                completion.addListener( f -> checkPointLock.close() );
            }
            else
            {
                checkPointLock.close();
            }
        }
    }

    public void fail( ChannelHandlerContext ctx, StoreCopyFinishedResponse.Status failureCode )
    {
        protocol.end( ctx, failureCode, -1 );
    }
}
