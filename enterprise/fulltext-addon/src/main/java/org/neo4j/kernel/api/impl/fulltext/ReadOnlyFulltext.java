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

import java.io.IOException;
import java.util.Collection;

import org.neo4j.collection.primitive.PrimitiveLongIterator;

public interface ReadOnlyFulltext extends AutoCloseable
{
    /**
     * Searches the fulltext index for any exact match of any of the given terms against any token in any of the indexed properties.
     *
     *
     * @param terms The terms to query for.
     * @param matchAll If true, only resluts that match all the given terms will be returned
     * @return An iterator over the matching entityIDs, ordered by lucene scoring of the match.
     */
    PrimitiveLongIterator query( Collection<String> terms, boolean matchAll );

    /**
     * Searches the fulltext index for any fuzzy match of any of the given terms against any token in any of the indexed properties.
     *
     *
     * @param terms The terms to query for.
     * @param matchAll If true, only resluts that match all the given terms will be returned
     * @return An iterator over the matching entityIDs, ordered by lucene scoring of the match.
     */
    PrimitiveLongIterator fuzzyQuery( Collection<String> terms, boolean matchAll );

    @Override
    void close();

    FulltextIndexConfiguration getConfigurationDocument() throws IOException;
}
