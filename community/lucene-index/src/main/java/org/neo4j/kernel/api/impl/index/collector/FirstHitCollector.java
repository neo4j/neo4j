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
package org.neo4j.kernel.api.impl.index.collector;

import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.search.CollectionTerminatedException;
import org.apache.lucene.search.SimpleCollector;

import java.io.IOException;

/**
 * A {@code Collector} that terminates the collection after the very first hit.
 * As a consequence, additional collectors in this search that require a complete run over the index,
 * such as {@code TotalHitCountCollector} or {@link DocValuesCollector}, won't work as expected.
 */
public final class FirstHitCollector extends SimpleCollector
{
    public static final int NO_MATCH = -1;

    private int result = NO_MATCH;
    private int readerDocBase = 0;

    /**
     * @return true when this collector got a match, otherwise false.
     */
    public boolean hasMatched()
    {
        return result != NO_MATCH;
    }

    /**
     * @return the docId if this collector got a match, otherwise {@link FirstHitCollector#NO_MATCH}.
     */
    public int getMatchedDoc()
    {
        return result;
    }

    @Override
    public void collect( int doc ) throws IOException
    {
        result = readerDocBase + doc;
        throw new CollectionTerminatedException();
    }

    @Override
    public boolean needsScores()
    {
        return false;
    }

    @Override
    protected void doSetNextReader( LeafReaderContext context ) throws IOException
    {
        super.doSetNextReader( context );
        readerDocBase = context.docBase;
    }
}
