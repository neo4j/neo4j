/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 *
 * This file is part of Neo4j.
 *
 * Neo4j is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.bolt.v4.runtime;

import org.neo4j.bolt.messaging.ResultConsumer;
import org.neo4j.bolt.runtime.Bookmark;
import org.neo4j.bolt.runtime.statemachine.BoltStateMachineState;
import org.neo4j.bolt.runtime.statemachine.StateMachineContext;
import org.neo4j.memory.HeapEstimator;

/**
 * When AUTOCOMMIT, additionally attach bookmark to PULL, DISCARD result
 */
public class AutoCommitState extends AbstractStreamingState
{
    public static final long SHALLOW_SIZE = HeapEstimator.shallowSizeOfInstance( AutoCommitState.class );

    @Override
    public String name()
    {
        return "AUTOCOMMIT";
    }

    @Override
    protected BoltStateMachineState processStreamPullResultMessage( int statementId, ResultConsumer resultConsumer,
                                                                    StateMachineContext context, long noToPull ) throws Throwable
    {
        Bookmark bookmark = context.getTransactionManager().pullData( context.connectionState().getCurrentTransactionId(), statementId,
                                                                      noToPull, resultConsumer );
        bookmark.attachTo( context.connectionState() );
        if ( resultConsumer.hasMore() )
        {
            return this;
        }
        return readyState;
    }

    @Override
    protected BoltStateMachineState processStreamDiscardResultMessage( int statementId, ResultConsumer resultConsumer,
                                                                       StateMachineContext context, long noToDiscard ) throws Throwable
    {
        var bookmark = context.getTransactionManager().discardData( context.connectionState().getCurrentTransactionId(), statementId,
                                                                    noToDiscard, resultConsumer );
        bookmark.attachTo( context.connectionState() );
        if ( resultConsumer.hasMore() )
        {
            return this;
        }
        return readyState;
    }
}
