/*
 * Copyright (c) 2002-2020 "Neo4j,"
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

import org.neo4j.bolt.messaging.ResultConsumer;
import org.neo4j.bolt.runtime.BoltResult;
import org.neo4j.bolt.runtime.statemachine.StateMachineContext;

import static org.neo4j.bolt.v4.messaging.AbstractStreamingMessage.STREAM_LIMIT_UNLIMITED;

public class ResultConsumerAdaptor implements ResultConsumer
{
    private final boolean pull;
    private final StateMachineContext context;

    ResultConsumerAdaptor( StateMachineContext context, boolean pull )
    {
        this.pull = pull;
        this.context = context;
    }

    @Override
    public boolean hasMore()
    {
        return false;
    }

    @Override
    public void consume( BoltResult boltResult ) throws Throwable
    {
        if ( pull )
        {
            context.connectionState().getResponseHandler().onPullRecords( boltResult, STREAM_LIMIT_UNLIMITED );
        }
        else
        {
            context.connectionState().getResponseHandler().onDiscardRecords( boltResult, STREAM_LIMIT_UNLIMITED );
        }
    }
}
