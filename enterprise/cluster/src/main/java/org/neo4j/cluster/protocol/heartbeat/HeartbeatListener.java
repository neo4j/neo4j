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
package org.neo4j.cluster.protocol.heartbeat;

import org.neo4j.cluster.InstanceId;

/**
 * Listener interface for heart beat. Implementations will receive
 * callbacks when instances in the cluster are considered failed or alive.
 */
public interface HeartbeatListener
{
    void failed( InstanceId server );

    void alive( InstanceId server );

    class Adapter implements HeartbeatListener
    {
        @Override
        public void failed( InstanceId server )
        {
        }

        @Override
        public void alive( InstanceId server )
        {
        }
    }
}
