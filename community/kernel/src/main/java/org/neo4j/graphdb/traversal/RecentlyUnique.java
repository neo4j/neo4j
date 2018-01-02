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
package org.neo4j.graphdb.traversal;

import org.neo4j.graphdb.Path;
import org.neo4j.kernel.impl.cache.LruCache;

class RecentlyUnique extends AbstractUniquenessFilter
{
    private static final Object PLACE_HOLDER = new Object();
    private static final int DEFAULT_RECENT_SIZE = 10000;

    private final LruCache<Long, Object> recentlyVisited;

    RecentlyUnique( PrimitiveTypeFetcher type, Object parameter )
    {
        super( type );
        parameter = parameter != null ? parameter : DEFAULT_RECENT_SIZE;
        recentlyVisited = new LruCache<Long, Object>( "Recently visited",
                ((Number) parameter).intValue() );
    }

    @Override
    public boolean check( TraversalBranch branch )
    {
        long id = type.getId( branch );
        boolean add = recentlyVisited.get( id ) == null;
        if ( add )
        {
            recentlyVisited.put( id, PLACE_HOLDER );
        }
        return add;
    }

    @Override
    public boolean checkFull( Path path )
    {
        // See GloballyUnique for comments.
        return true;
    }
}
