/*
 * Copyright (c) 2002-2017 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
 *
 * This file is part of Neo4j.
 *
 * Neo4j is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program. If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.kernel.api.impl.fulltext;

import org.neo4j.collection.primitive.PrimitiveLongIterator;

public interface ReadOnlyFulltext extends AutoCloseable
{
    /**
     * Searches the fulltext helper for any exact match of any of the given terms against any token in any of the indexed properties.
     *
     * @param terms The terms to query for.
     * @return An iterator over the matching entityIDs, ordered by lucene scoring of the match.
     */
    PrimitiveLongIterator query( String... terms );

    /**
     * Searches the fulltext helper for any fuzzy match of any of the given terms against any token in any of the indexed properties.
     *
     * @param terms The terms to query for.
     * @return An iterator over the matching entityIDs, ordered by lucene scoring of the match.
     */
    PrimitiveLongIterator fuzzyQuery( String... terms );
}
