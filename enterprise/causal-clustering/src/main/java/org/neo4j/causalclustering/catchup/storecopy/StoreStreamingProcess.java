/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 *
 * This file is part of Neo4j Enterprise Edition. The included source
 * code can be redistributed and/or modified under the terms of the
 * GNU AFFERO GENERAL PUBLIC LICENSE Version 3
 * (http://www.fsf.org/licensing/licenses/agpl-3.0.html) with the
 * Commons Clause, as found in the associated LICENSE.txt file.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * Neo4j object code can be licensed independently from the source
 * under separate terms from the AGPL. Inquiries can be directed to:
 * licensing@neo4j.com
 *
 * More information is also available at:
 * https://neo4j.com/licensing/
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
    private final StoreFileStreamingProtocol protocol;
    private final Supplier<CheckPointer> checkPointerSupplier;
    private final StoreCopyCheckPointMutex mutex;
    private final StoreResourceStreamFactory resourceStreamFactory;

    public StoreStreamingProcess( StoreFileStreamingProtocol protocol, Supplier<CheckPointer> checkPointerSupplier, StoreCopyCheckPointMutex mutex,
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
            while ( resources.next() )
            {
                StoreResource resource = resources.get();
                protocol.stream( ctx, resource );
            }
            completion = protocol.end( ctx, SUCCESS );
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
        protocol.end( ctx, failureCode );
    }
}
