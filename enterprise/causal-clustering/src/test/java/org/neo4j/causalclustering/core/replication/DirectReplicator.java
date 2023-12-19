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
package org.neo4j.causalclustering.core.replication;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicReference;

import org.neo4j.causalclustering.core.state.machines.StateMachine;

public class DirectReplicator<Command extends ReplicatedContent> implements Replicator
{
    private final StateMachine<Command> stateMachine;
    private long commandIndex;

    public DirectReplicator( StateMachine<Command> stateMachine )
    {
        this.stateMachine = stateMachine;
    }

    @Override
    public synchronized Future<Object> replicate( ReplicatedContent content, boolean trackResult )
    {
        AtomicReference<CompletableFuture<Object>> futureResult = new AtomicReference<>( new CompletableFuture<>() );
        stateMachine.applyCommand( (Command) content, commandIndex++, result ->
        {
            if ( trackResult )
            {
                futureResult.getAndUpdate( result::apply );
            }
        } );

        return futureResult.get();
    }
}
