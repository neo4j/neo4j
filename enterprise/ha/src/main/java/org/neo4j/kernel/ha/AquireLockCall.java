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
