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
package org.neo4j.causalclustering.core.state.machines.locks;

import java.io.IOException;

import org.neo4j.causalclustering.identity.MemberId;
import org.neo4j.causalclustering.messaging.EndOfStreamException;
import org.neo4j.storageengine.api.ReadableChannel;
import org.neo4j.storageengine.api.WritableChannel;

public class ReplicatedLockTokenSerializer
{

    private ReplicatedLockTokenSerializer()
    {
    }

    public static void marshal( ReplicatedLockTokenRequest tokenRequest, WritableChannel channel ) throws IOException
    {
        channel.putInt( tokenRequest.id() );
        new MemberId.Marshal().marshal( tokenRequest.owner(), channel );
    }

    public static ReplicatedLockTokenRequest unmarshal( ReadableChannel channel ) throws IOException, EndOfStreamException
    {
        int candidateId = channel.getInt();
        MemberId owner = new MemberId.Marshal().unmarshal( channel );

        return new ReplicatedLockTokenRequest( owner, candidateId );
    }
}
