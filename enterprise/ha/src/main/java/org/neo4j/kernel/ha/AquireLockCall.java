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
package org.neo4j.kernel.ha;

import org.jboss.netty.buffer.ChannelBuffer;

import org.neo4j.com.RequestContext;
import org.neo4j.com.Response;
import org.neo4j.com.TargetCaller;
import org.neo4j.kernel.ha.com.master.Master;
import org.neo4j.kernel.ha.lock.LockResult;
import org.neo4j.kernel.impl.locking.ResourceTypes;
import org.neo4j.storageengine.api.lock.ResourceType;

abstract class AquireLockCall implements TargetCaller<Master, LockResult>
{
    @Override
    public Response<LockResult> call( Master master, RequestContext context,
                                      ChannelBuffer input, ChannelBuffer target )
    {
        ResourceType type = ResourceTypes.fromId( input.readInt() );
        long[] ids = new long[input.readInt()];
        for ( int i = 0; i < ids.length; i++ )
        {
            ids[i] = input.readLong();
        }
        return lock( master, context, type, ids );
    }

    protected abstract Response<LockResult> lock( Master master, RequestContext context, ResourceType type,
                                                  long... ids );
}
