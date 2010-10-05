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

package org.neo4j.kernel.impl.traversal;

import java.util.ArrayList;
import java.util.Collection;

import org.neo4j.graphdb.Path;
import org.neo4j.helpers.Predicate;

class MultiFilter implements Predicate<Path>
{
    private final Collection<Predicate<Path>> filters = new ArrayList<Predicate<Path>>();
    
    MultiFilter( Predicate<Path>... filters )
    {
        for ( Predicate<Path> filter : filters )
        {
            this.filters.add( filter );
        }
    }

    MultiFilter( Collection<Predicate<Path>> filters )
    {
        this.filters.addAll( filters );
    }

    public boolean accept( Path path )
    {
        for ( Predicate<Path> filter : this.filters )
        {
            if ( !filter.accept( path ) )
            {
                return false;
            }
        }
        return true;
    }

    public MultiFilter add( Predicate<Path> filter )
    {
        Collection<Predicate<Path>> newFilters = new ArrayList<Predicate<Path>>( this.filters );
        newFilters.add( filter );
        return new MultiFilter( newFilters );
    }
}
