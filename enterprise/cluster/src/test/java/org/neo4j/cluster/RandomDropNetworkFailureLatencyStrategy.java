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

import java.util.Random;

import org.neo4j.cluster.com.message.Message;
import org.neo4j.cluster.com.message.MessageType;

/**
 * Randomly drops messages.
 */
public class RandomDropNetworkFailureLatencyStrategy
    implements NetworkLatencyStrategy
{
    Random random;
    private double rate;

    /**
     *
     * @param seed Provide a seed for the Random, in order to produce repeatable tests.
     * @param rate 1.0=no dropped messages, 0.0=all messages are lost
     */
    public RandomDropNetworkFailureLatencyStrategy(long seed, double rate)
    {
        setRate( rate );
        this.random = new Random( seed );
    }

    public void setRate( double rate )
    {
        this.rate = rate;
    }

    @Override
    public long messageDelay(Message<? extends MessageType> message, String serverIdTo)
    {
        return random.nextDouble() > rate ? 0 : LOST;
    }
}
