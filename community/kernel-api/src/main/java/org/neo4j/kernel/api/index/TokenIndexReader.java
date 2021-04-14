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

package org.neo4j.kernel.api.index;

import org.neo4j.internal.kernel.api.IndexQueryConstraints;
import org.neo4j.internal.kernel.api.TokenPredicate;
import org.neo4j.io.pagecache.tracing.cursor.PageCursorTracer;

public interface TokenIndexReader extends IndexReader
{

    /**
     * Queries all the entities and coordinates with the supplied {@link IndexProgressor.EntityTokenClient} to return the results
     *
     * @param client       a handle for the token reader to propagate the queried results.
     * @param constraints  represents all constraints for this query like ordering, limit etc.
     * @param query        the predicate to identify the tokens being queried
     * @param cursorTracer underlying page cursor tracer
     */
    void query( IndexProgressor.EntityTokenClient client, IndexQueryConstraints constraints, TokenPredicate query, PageCursorTracer cursorTracer );

    /**
     * Queries a specific range of entities and coordinates with the supplied {@link IndexProgressor.EntityTokenClient} to return the results.
     *
     * @param client       a handle for the token reader to propagate the queried results.
     * @param constraints  represents all constraints for this query like ordering, limit etc.
     * @param query        the predicate to identify the tokens being queried
     * @param range        the range of entities that should be queried.
     * @param cursorTracer underlying page cursor tracer
     */
    void query( IndexProgressor.EntityTokenClient client,
                IndexQueryConstraints constraints, TokenPredicate query, EntityRange range, PageCursorTracer cursorTracer );

    TokenIndexReader EMPTY = new TokenIndexReader()
    {
        @Override
        public void query( IndexProgressor.EntityTokenClient client, IndexQueryConstraints constraints, TokenPredicate query, PageCursorTracer cursorTracer )
        {
        }

        @Override
        public void query( IndexProgressor.EntityTokenClient client,
                           IndexQueryConstraints constraints, TokenPredicate query, EntityRange range, PageCursorTracer cursorTracer )
        {

        }

        @Override
        public void close()
        {
        }
    };
}
