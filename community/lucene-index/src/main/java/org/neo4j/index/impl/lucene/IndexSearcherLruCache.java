/**
 * Copyright (c) 2002-2014 "Neo Technology,"
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
package org.neo4j.index.impl.lucene;

import java.io.IOException;
import java.util.concurrent.atomic.AtomicBoolean;

import org.neo4j.helpers.Pair;
import org.neo4j.kernel.impl.cache.LruCache;

/**
 * An Lru Cache for Lucene Index searchers.
 *
 * @see LuceneDataSource
 */
public class IndexSearcherLruCache extends LruCache<IndexIdentifier, Pair<IndexReference, AtomicBoolean>>
{
    /**
     * Creates a LRU cache. If <CODE>maxSize < 1</CODE> an
     * IllegalArgumentException is thrown.
     *
     * @param maxSize maximum size of this cache
     */
    public IndexSearcherLruCache( int maxSize )
    {
        super( "IndexSearcherCache", maxSize );
    }

    @Override
    public void elementCleaned( Pair<IndexReference, AtomicBoolean> searcher )
    {
        try
        {
            searcher.first().dispose( true );
        }
        catch ( IOException e )
        {
            throw new RuntimeException( e );
        }
    }
}