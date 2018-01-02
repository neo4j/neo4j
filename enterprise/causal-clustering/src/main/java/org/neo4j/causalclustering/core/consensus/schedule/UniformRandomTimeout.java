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
package org.neo4j.causalclustering.core.consensus.schedule;

import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;

/**
 * A random timeout distributed uniformly in an interval.
 */
public class UniformRandomTimeout implements Timeout
{
    private final long minDelay;
    private final long maxDelay;
    private final TimeUnit unit;

    UniformRandomTimeout( long minDelay, long maxDelay, TimeUnit unit )
    {
        this.minDelay = minDelay;
        this.maxDelay = maxDelay;
        this.unit = unit;
    }

    @Override
    public Delay next()
    {
        long delay = ThreadLocalRandom.current().nextLong( minDelay, maxDelay + 1 );
        return new Delay( delay, unit );
    }
}
