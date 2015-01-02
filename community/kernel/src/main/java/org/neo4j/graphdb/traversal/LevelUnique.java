/**
 * Copyright (c) 2002-2015 "Neo Technology,"
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

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.neo4j.graphdb.Path;

class LevelUnique extends AbstractUniquenessFilter
{
    private final Map<Integer, Set<Long>> idsPerLevel = new HashMap<Integer, Set<Long>>();
    
    LevelUnique( PrimitiveTypeFetcher type )
    {
        super( type );
    }
    
    @Override
    public boolean check( TraversalBranch branch )
    {
        Integer level = branch.length();
        Set<Long> levelIds = idsPerLevel.get( level );
        if ( levelIds == null )
        {
            levelIds = new HashSet<Long>();
            idsPerLevel.put( level, levelIds );
        }
        return levelIds.add( type.getId( branch ) );
    }
    
    @Override
    public boolean checkFull( Path path )
    {
        throw new UnsupportedOperationException( "Not implemented yet" );
    }
}
