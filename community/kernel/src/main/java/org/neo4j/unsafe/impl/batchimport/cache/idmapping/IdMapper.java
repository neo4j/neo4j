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
package org.neo4j.unsafe.impl.batchimport.cache.idmapping;

import org.neo4j.helpers.progress.ProgressListener;
import org.neo4j.unsafe.impl.batchimport.InputIterable;
import org.neo4j.unsafe.impl.batchimport.cache.MemoryStatsVisitor;
import org.neo4j.unsafe.impl.batchimport.input.Collector;
import org.neo4j.unsafe.impl.batchimport.input.Group;
import org.neo4j.unsafe.impl.batchimport.input.InputNode;
import org.neo4j.unsafe.impl.batchimport.input.InputRelationship;

/**
 * Maps node ids as specified by {@link InputNode#id()}, {@link InputRelationship#startNode()} and
 * {@link InputRelationship#endNode()} from an id of some unknown sort, coming directly from input, to actual node ids.
 */
public interface IdMapper extends MemoryStatsVisitor.Visitable
{
    /**
     * Maps an {@code inputId} to an actual node id.
     * @param inputId an id of an unknown type, coming from input.
     * @param actualId the actual node id that the inputId will represent.
     * @param group {@link Group} this input id will be added to. Used for handling input ids collisions
     * where multiple equal input ids might be added, as long as all input ids within a single group is unique.
     * Group ids are also passed into {@link #get(Object, Group)}.
     * It is required that all input ids belonging to a specific group are put in sequence before putting any
     * input ids for another group.
     */
    void put( Object inputId, long actualId, Group group );

    /**
     * @return whether or not a call to {@link #prepare(InputIterable, Collector, ProgressListener)} needs to commence after all calls to
     * {@link #put(Object, long, Group)} and before any call to {@link #get(Object, Group)}. I.e. whether or not all ids
     * needs to be put before making any call to {@link #get(Object, Group)}.
     */
    boolean needsPreparation();

    /**
     * After all mappings have been {@link #put(Object, long, Group)} call this method to prepare for
     * {@link #get(Object, Group)}.
     *
     * @param allIds put earlier, in the event of difficult collisions so that more information have to be read
     * from the input data again, data that normally isn't necessary and hence discarded.
     * @param collector {@link Collector} for bad entries, such as duplicate node ids.
     * @param progress reports preparation progress.
     */
    void prepare( InputIterable<Object> allIds, Collector collector, ProgressListener progress );

    /**
     * Returns an actual node id representing {@code inputId}. For this call to work {@link #prepare(InputIterable, Collector, ProgressListener)} must have
     * been called after all calls to {@link #put(Object, long, Group)} have been made,
     * iff {@link #needsPreparation()} returns {@code true}. Otherwise ids can be retrieved right after
     * @link #put(Object, long) being put}
     *
     * @param inputId the input id to get the actual node id for.
     * @param group {@link Group} the given {@code inputId} must exist in, i.e. have been put with.
     * @return the actual node id previously specified by {@link #put(Object, long, Group)}, or {@code -1} if not found.
     */
    long get( Object inputId, Group group );
}
