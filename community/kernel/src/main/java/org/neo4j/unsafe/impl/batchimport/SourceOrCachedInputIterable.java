/*
 * Copyright (c) 2002-2017 "Neo Technology,"
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
package org.neo4j.unsafe.impl.batchimport;

import org.neo4j.unsafe.impl.batchimport.input.InputEntity;

/**
 * Convenience for where there's an {@link InputIterable} which doesn't
 * {@link InputIterable#supportsMultiplePasses() passes multiple support}, in which case a cached
 * {@link InputIterator} will be returned instead.
 *
 * @param <T> type of {@link InputEntity} of this iterator.
 */
public class SourceOrCachedInputIterable<T extends InputEntity> implements InputIterable<T>
{
    private final InputIterable<T> source;
    private final InputIterable<T> cached;

    public SourceOrCachedInputIterable( InputIterable<T> source, InputIterable<T> cached )
    {
        this.source = source;
        this.cached = cached;
    }

    @Override
    public InputIterator<T> iterator()
    {
        return source.supportsMultiplePasses() ? source.iterator() : cached.iterator();
    }

    @Override
    public boolean supportsMultiplePasses()
    {
        return true;
    }

    public static <T extends InputEntity> InputIterable<T> cachedForSure(
            InputIterable<T> source, InputIterable<T> cached )
    {
        return new SourceOrCachedInputIterable<>( source, cached );
    }
}
