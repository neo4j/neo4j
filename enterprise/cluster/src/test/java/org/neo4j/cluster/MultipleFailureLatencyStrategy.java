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

    public MultipleFailureLatencyStrategy( NetworkLatencyStrategy... strategies )
    {
        this.strategies = strategies;
    }

    public <T extends NetworkLatencyStrategy> T getStrategy( Class<T> strategyClass )
    {
        for ( NetworkLatencyStrategy strategy : strategies )
        {
            if ( strategyClass.isInstance( strategy ) )
            {
                return (T) strategy;
            }
        }
        throw new IllegalArgumentException( " No strategy of type " + strategyClass.getName() + " found" );
    }

    @Override
    public long messageDelay( Message<? extends MessageType> message, String serverIdTo )
    {
        long totalDelay = 0;
        for ( NetworkLatencyStrategy strategy : strategies )
        {
            long delay = strategy.messageDelay( message, serverIdTo );
            if ( delay == LOST )
            {
                return delay;
            }
            totalDelay += delay;
        }

        return totalDelay;
    }
}
