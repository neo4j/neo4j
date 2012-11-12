/**
 * Copyright (c) 2002-2012 "Neo Technology,"
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
package org.neo4j.server.rest.paging;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.neo4j.graphdb.Path;
import org.neo4j.graphdb.traversal.Traverser;

public class PagedTraverser implements Iterator<List<Path>>, Iterable<List<Path>>, Leasable
{

    private final int pageSize;
    private Iterator<Path> iterator;

    public PagedTraverser( Traverser traverser, int pageSize )
    {
        iterator = traverser.iterator();
        this.pageSize = pageSize;
    }

    public List<Path> next()
    {
        if ( !iterator.hasNext() )
        {
            return null;
        }

        ArrayList<Path> result = new ArrayList<Path>();

        for ( int i = 0; i < pageSize; i++ )
        {
            if ( !iterator.hasNext() )
            {
                break;
            }
            else
            {
                result.add( iterator.next() );
            }
        }

        return result;
    }

    @Override
    public boolean hasNext()
    {
        return iterator.hasNext();
    }

    @Override
    public void remove()
    {
        iterator.remove();
    }

    @Override
    public Iterator<List<Path>> iterator()
    {
        return this;
    }

}
