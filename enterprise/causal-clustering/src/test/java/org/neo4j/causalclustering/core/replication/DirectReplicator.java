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
