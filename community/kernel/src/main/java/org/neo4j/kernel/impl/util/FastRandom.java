/**
 * Copyright (c) 2002-2016 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
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
package org.neo4j.kernel.impl.util;

import java.security.SecureRandom;

import static java.lang.Math.abs;

// TODO This is only used in "tests" so it should be moved to the test source tree.
public class FastRandom
{
    private long currentSeed;

    public FastRandom()
    {
        this(new SecureRandom().nextLong());
    }

    public FastRandom(long seed)
    {
        this.currentSeed = seed;
    }

    public long next()
    {
        long x = currentSeed;
        x ^= (x << 21);
        x ^= (x >>> 35);
        x ^= (x << 4);
        currentSeed = x;
        return x;
    }

    /**
     * Value between 0 and max.
     */
    public long next( long max )
    {
        return abs( next() ) % max;
    }
}
