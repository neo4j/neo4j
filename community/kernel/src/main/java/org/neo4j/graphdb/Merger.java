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
 * A utility interface for getting or creating entities (with regard to given indexes or constraints).
 *
 * @see org.neo4j.graphdb.GraphDatabaseService#getOrCreateNode(Label, Label...)
 * @see org.neo4j.graphdb.index.UniqueFactory#asMerger()
 *
 * @param <ENTITY> the type of entity created by this {@link Merger}.
 */
public interface Merger<ENTITY> extends ResourceIterable<ENTITY>
{
    Merger<ENTITY> withProperty( String key, Object value );

    /**
     * This method is an alias for merge() that exists for convenient iteration.
     *
     * @return The result of merging
     */
    @Override
    MergeResult<ENTITY> iterator();

    /**
     * @return The result of merging
     */
    MergeResult<ENTITY> merge();
}
