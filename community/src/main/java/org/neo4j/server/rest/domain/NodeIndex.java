/**
 * Copyright (c) 2002-2010 "Neo Technology,"
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

package org.neo4j.server.rest.domain;

import org.neo4j.graphdb.Node;
import org.neo4j.index.IndexHits;
import org.neo4j.index.IndexService;

public class NodeIndex implements Index<Node>
{
    private final IndexService indexService;

    public NodeIndex( IndexService indexService )
    {
        this.indexService = indexService;
    }

    public boolean add( Node node, String key, Object value )
    {
        // TODO Implement for real
        indexService.index( node, key, value );
        return true;
    }

    public IndexHits<Node> get( String key, Object value )
    {
        return indexService.getNodes( key, value );
    }

    public boolean remove( Node node, String key, Object value )
    {
        boolean existed = contains( node, key, value );
        indexService.removeIndex( node, key, value );
        return existed;
    }

    public boolean contains( Node node, String key, Object value )
    {
        // TODO When IndexService has a method like this, use it directly instead.
        IndexHits<Node> hits = indexService.getNodes( key, value );
        try
        {
            for ( Node hit : hits )
            {
                if ( hit.equals( node ) )
                {
                    return true;
                }
            }
            return false;
        }
        finally
        {
            hits.close();
        }
    }
}
