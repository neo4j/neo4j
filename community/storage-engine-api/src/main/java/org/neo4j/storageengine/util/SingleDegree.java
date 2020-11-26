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
package org.neo4j.storageengine.util;

import org.neo4j.storageengine.api.Degrees;

public class SingleDegree implements Degrees.Mutator
{
    private int total;
    private final int maxDegree;

    public SingleDegree()
    {
        this( Integer.MAX_VALUE );
    }

    public SingleDegree( int maxDegree )
    {
        this.maxDegree = maxDegree;
    }

    @Override
    public boolean add( int type, int outgoing, int incoming, int loop )
    {
        this.total += outgoing + incoming + loop;
        return this.total < maxDegree;
    }

    @Override
    public boolean isSplit()
    {
        return false;
    }

    public int getTotal()
    {
        return total;
    }
}
