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
package org.neo4j.cluster.protocol.atomicbroadcast.multipaxos;

import org.neo4j.cluster.protocol.ConfigurationContext;
import org.neo4j.cluster.protocol.LoggingContext;
import org.neo4j.cluster.protocol.TimeoutsContext;
import org.neo4j.cluster.protocol.atomicbroadcast.AtomicBroadcastListener;
import org.neo4j.cluster.protocol.atomicbroadcast.Payload;

/**
 * Context for AtomicBroadcast statemachine.
 */
public interface AtomicBroadcastContext
    extends TimeoutsContext, ConfigurationContext, LoggingContext
{
    void addAtomicBroadcastListener( AtomicBroadcastListener listener );
    void removeAtomicBroadcastListener( AtomicBroadcastListener listener );
    void receive( Payload value );
    boolean hasQuorum();
}
