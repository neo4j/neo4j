/**
 * Copyright (c) 2002-2013 "Neo Technology,"
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
package org.neo4j.graphdb;

/**
 * Result of performing a merge
 *
 * Since merging can return more than one element, you must only ever exactly once either
 * iterate over this result by calling {@link MergeResult#hasNext()} and {@link MergeResult#next()}, or
 * retrieve a single entity result by calling {@link MergeResult#single()}.
 *
 * A {@link org.neo4j.graphdb.MergeResult} is a {@link org.neo4j.graphdb.ResourceIterator} that must be properly
 * closed.  This can be achieved by exhausting the iterator, by calling {@link MergeResult#close()} directly,
 * or by using try-with-resources.
 *
 * @see Merger#merge()
 * @see GraphDatabaseService#getOrCreateNode(Label...)
 *
 * @param <T> the type of entities contained in this result
 */
public interface MergeResult<T> extends ResourceIterator<T>
{
    /**
     * Attempts to return the single entity produces by merge and closes this merge result.
     *
     * @see MergeResult#containsNewlyCreated()
     *
     * @return the single merged entity found or created by merge
     *
     * @throws java.util.NoSuchElementException if more than one node was returned by merge
     * @throws java.lang.IllegalStateException if this merge result has already been closed
     */
    T single();

    /**
     * @return the next node found by merge
     * @throws java.util.NoSuchElementException if all nodes found by merge have already been returned
     * @throws java.lang.IllegalStateException if this merge result has already been closed
     */
    @Override
    T next();

    /**
     * Indicates if this iterator returns newly created entities (vs. returning already existing elements)
     *
     * @return true, if this iterator contains newly created entities
     */
    boolean containsNewlyCreated();
}
