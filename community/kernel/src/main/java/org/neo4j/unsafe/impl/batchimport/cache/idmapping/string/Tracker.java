/*
 * Copyright (c) 2002-2018 "Neo Technology,"
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
package org.neo4j.unsafe.impl.batchimport.cache.idmapping.string;

import org.neo4j.helpers.progress.ProgressListener;
import org.neo4j.unsafe.impl.batchimport.InputIterable;
import org.neo4j.unsafe.impl.batchimport.cache.MemoryStatsVisitor;
import org.neo4j.unsafe.impl.batchimport.cache.idmapping.IdMapper;
import org.neo4j.unsafe.impl.batchimport.input.Collector;
import org.neo4j.unsafe.impl.batchimport.input.Group;

/**
 * {@link EncodingIdMapper} is an index where arbitrary ids, be it {@link String} or {@code long} or whatever
 * can be added and mapped to an internal (node) {@code long} id. The order in which ids are added can be
 * any order and so in the end when all ids have been added the index goes through a
 * {@link IdMapper#prepare(InputIterable, Collector, ProgressListener) prepare phase} where these ids are sorted
 * so that {@link IdMapper#get(Object, Group)} can execute efficiently later on.
 * <p>
 * In that sorting the ids aren't moved, but instead a {@link Tracker} created where these moves are recorded
 * and the initial data (in order of insertion) is kept intact to be able to track {@link Group} belonging among
 * other things. Since a tracker is instantiated after all ids have been added there's an opportunity to create
 * a smaller data structure for smaller datasets, for example those that fit inside {@code int} range.
 * That's why this abstraction exists so that the best suited implementation can be picked for every import.
 */
public interface Tracker extends MemoryStatsVisitor.Visitable
{
    /**
     * @param index data index to get the value for.
     * @return value previously {@link #set(long, long)}.
     */
    long get( long index );

    /**
     * Swaps values from {@code fromIndex} to {@code toIndex}, as many items as {@code count} specifies.
     *
     * @param fromIndex index to swap from.
     * @param toIndex index to swap to.
     * @param count number of items to swap.
     */
    void swap( long fromIndex, long toIndex, int count );

    /**
     * Sets {@code value} at the specified {@code index}.
     *
     * @param index data index to set value at.
     * @param value value to set at that index.
     */
    void set( long index, long value );
}
