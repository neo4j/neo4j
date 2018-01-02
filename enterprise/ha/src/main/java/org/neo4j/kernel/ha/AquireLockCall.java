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
