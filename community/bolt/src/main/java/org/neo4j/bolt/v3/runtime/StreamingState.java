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
package org.neo4j.bolt.v3.runtime;

import org.neo4j.bolt.runtime.statemachine.BoltStateMachineState;
import org.neo4j.bolt.runtime.statemachine.StateMachineContext;
import org.neo4j.bolt.runtime.statemachine.StatementMetadata;
import org.neo4j.bolt.messaging.ResultConsumer;
import org.neo4j.memory.HeapEstimator;

/**
 * When STREAMING, additionally attach bookmark to PULL_ALL, DISCARD_ALL result
 */
public class StreamingState extends AbstractStreamingState
{
    public static final long SHALLOW_SIZE = HeapEstimator.shallowSizeOfInstance( StreamingState.class );

    @Override
    public String name()
    {
        return "STREAMING";
    }

    @Override
    protected BoltStateMachineState processStreamResultMessage( ResultConsumer resultConsumer, StateMachineContext context ) throws Throwable
    {
        var statementId = StatementMetadata.ABSENT_QUERY_ID;
        var bookmark = context.getTransactionManager()
                              .pullData( context.connectionState().getCurrentTransactionId(), statementId, -1, resultConsumer );
        bookmark.attachTo( context.connectionState() );
        return readyState;
    }
}
