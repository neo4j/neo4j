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
    public RandomDropNetworkFailureLatencyStrategy( long seed, double rate )
    {
        setRate( rate );
        this.random = new Random( seed );
    }

    public void setRate( double rate )
    {
        this.rate = rate;
    }

    @Override
    public long messageDelay( Message<? extends MessageType> message, String serverIdTo )
    {
        return random.nextDouble() > rate ? 0 : LOST;
    }
}
