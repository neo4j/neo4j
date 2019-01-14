/*
 * Copyright (c) 2002-2019 "Neo4j,"
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
package org.neo4j.unsafe.impl.batchimport;

import java.util.concurrent.atomic.LongAdder;

import org.neo4j.unsafe.impl.batchimport.stats.DetailLevel;
import org.neo4j.unsafe.impl.batchimport.stats.Key;
import org.neo4j.unsafe.impl.batchimport.stats.Keys;
import org.neo4j.unsafe.impl.batchimport.stats.Stat;
import org.neo4j.unsafe.impl.batchimport.stats.StatsProvider;

import static org.neo4j.helpers.ArrayUtil.array;

/**
 * Able to provide {@link Keys#progress}. Mutable and thread-safe, use {@link #add(long)} to move progress forwards.
 */
public class RelationshipLinkingProgress implements StatsProvider, Stat
{
    private final Key[] keys = array( Keys.progress );
    private final LongAdder progress = new LongAdder();

    @Override
    public Stat stat( Key key )
    {
        return this;
    }

    @Override
    public Key[] keys()
    {
        return keys;
    }

    @Override
    public DetailLevel detailLevel()
    {
        return DetailLevel.BASIC;
    }

    @Override
    public long asLong()
    {
        return progress.longValue();
    }

    public void add( long amount )
    {
        progress.add( amount );
    }
}
