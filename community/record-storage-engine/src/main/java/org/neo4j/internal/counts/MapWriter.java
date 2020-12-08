/*
 * Copyright (c) "Neo4j"
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
package org.neo4j.internal.counts;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Function;
import java.util.function.ToLongFunction;

import org.neo4j.util.concurrent.OutOfOrderSequence;

import static org.apache.commons.lang3.ArrayUtils.EMPTY_LONG_ARRAY;

/**
 * Used during recovery and normal operations mode where changes gets applied to a {@link ConcurrentHashMap} and counts that haven't been seen before
 * are looked up from stored counts and placed into the map too.
 */
public class MapWriter implements CountUpdater.CountWriter
{
    private final CountsChanges changes;
    private final OutOfOrderSequence idSequence;
    private final long txId;
    private final Function<CountsKey,AtomicLong> defaultToStoredCount;

    MapWriter( ToLongFunction<CountsKey> storeLookup, CountsChanges changes, OutOfOrderSequence idSequence, long txId )
    {
        this.changes = changes;
        this.idSequence = idSequence;
        this.txId = txId;
        defaultToStoredCount = k -> new AtomicLong( storeLookup.applyAsLong( k ) );
    }

    @Override
    public void write( CountsKey key, long delta )
    {
        changes.add( key, delta, defaultToStoredCount );
    }

    @Override
    public void close()
    {
        idSequence.offer( txId, EMPTY_LONG_ARRAY );
    }
}
