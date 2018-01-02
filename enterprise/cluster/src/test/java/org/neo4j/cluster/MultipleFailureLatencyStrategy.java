/*
 * Copyright (c) 2002-2018 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
 *
 * This file is part of Neo4j.
 *
 * Neo4j is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program. If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.cluster;

import org.neo4j.cluster.com.message.Message;
import org.neo4j.cluster.com.message.MessageType;

/**
 * Ask a set of network strategies about a message delay. Anyone says -1, then it is lost.
 */
public class MultipleFailureLatencyStrategy
        implements NetworkLatencyStrategy
{
    private final NetworkLatencyStrategy[] strategies;

    public MultipleFailureLatencyStrategy(NetworkLatencyStrategy... strategies)
    {
        this.strategies = strategies;
    }

    public <T extends NetworkLatencyStrategy> T getStrategy(Class<T> strategyClass)
    {
        for( NetworkLatencyStrategy strategy : strategies )
        {
            if (strategyClass.isInstance( strategy ))
                return (T) strategy;
        }
        throw new IllegalArgumentException( " No strategy of type "+strategyClass.getName()+" found" );
    }

    @Override
    public long messageDelay(Message<? extends MessageType> message, String serverIdTo)
    {
        long totalDelay = 0;
        for (NetworkLatencyStrategy strategy : strategies)
        {
            long delay = strategy.messageDelay(message, serverIdTo);
            if (delay == LOST )
                return delay;
            totalDelay += delay;
        }

        return totalDelay;
    }
}
