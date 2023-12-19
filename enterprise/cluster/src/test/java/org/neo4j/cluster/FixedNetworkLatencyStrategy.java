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
package org.neo4j.cluster;

import org.neo4j.cluster.com.message.Message;
import org.neo4j.cluster.com.message.MessageType;

/**
 * Messages never gets lost
 */
public class FixedNetworkLatencyStrategy
    implements NetworkLatencyStrategy
{
    private long delay;

    public FixedNetworkLatencyStrategy()
    {
        this( 0 );
    }

    public FixedNetworkLatencyStrategy( long delay )
    {
        this.delay = delay;
    }

    @Override
    public long messageDelay( Message<? extends MessageType> message, String serverIdTo )
    {
        return delay;
    }
}
