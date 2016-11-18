/*
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
package org.neo4j.index;

import java.io.Closeable;
import java.io.IOException;
import org.neo4j.cursor.RawCursor;

/**
 * An index which can have data {@link #modifier(org.neo4j.index.Modifier.Options) added/removed} and
 * {@link #seek(Object, Object) looked up} using key range.
 *
 * @param <KEY> type of keys in the index
 * @param <VALUE> type values in the index
 */
public interface Index<KEY,VALUE> extends Closeable
{
    /**
     * Seeks hits in this index, given a key range. Hits are iterated over using the returned {@link RawCursor}.
     * There's no guarantee that neither the {@link Hit} nor key/value instances are stable and so
     * implementors can choose to reuse a single instance and update the values in them directly.
     * This to reduce garbage strain. If caller wants to cache the results it's safest to copy
     * the instances into its own result cache.
     *
     * @param fromInclusive lower bound of the range to seek (inclusive).
     * @param toExclusive higher bound of the range to seek (exclusive).
     * @return a {@link RawCursor} used to iterate over the hits within the specified key range.
     * @throws IOException on error reading from index.
     */
    RawCursor<Hit<KEY,VALUE>,IOException> seek( KEY fromInclusive, KEY toExclusive ) throws IOException;

    /**
     * Returns a {@link Modifier} able to modify the index, i.e. insert and remove keys/values.
     * After usage the returned modifier must be closed, typically by using try-with-resource clause.
     *
     * @param options {@link Modifier.Options} which will apply to all modifications by the returned {@link Modifier}.
     * @return {@link Modifier} able to modify the index.
     * @throws IOException on error accessing the index.
     */
    Modifier<KEY,VALUE> modifier( Modifier.Options options ) throws IOException;

    /**
     * Flushes any pending changes to storage.
     *
     * @throws IOException on error flushing to storage.
     */
    void flush() throws IOException;
}
