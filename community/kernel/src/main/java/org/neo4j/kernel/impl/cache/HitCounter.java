/*
 * Copyright (c) 2002-2015 "Neo Technology,"
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
package org.neo4j.kernel.impl.cache;

import org.neo4j.helpers.Counter;

final class HitCounter
{
    private final Counter hits, miss;

    public HitCounter( )
    {
        this.hits = new Counter();
        this.miss = new Counter();
    }

    public <T> T count( T item )
    {
        ( ( item == null ) ? miss : hits ).inc();
        return item;
    }

    public long getHitsCount()
    {
        return hits.count();
    }

    public long getMissCount()
    {
        return miss.count();
    }
}
