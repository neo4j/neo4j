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
package org.neo4j.cluster.timeout;

import org.neo4j.cluster.com.message.Message;

/**
 * A {@link TimeoutStrategy} that sets timeouts to a given fixed value.
 */
public class FixedTimeoutStrategy
    implements TimeoutStrategy
{

    protected final long timeout;

    public FixedTimeoutStrategy( long timeout )
    {
        this.timeout = timeout;
    }

    @Override
    public long timeoutFor( Message message )
    {
        return timeout;
    }

    @Override
    public void timeoutTriggered( Message timeoutMessage )
    {
    }

    @Override
    public void timeoutCancelled( Message timeoutMessage )
    {
    }

    @Override
    public void tick( long now )
    {
    }
}
