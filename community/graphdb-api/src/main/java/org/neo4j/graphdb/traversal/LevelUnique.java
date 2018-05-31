/*
 * Copyright (c) 2002-2018 "Neo4j,"
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
package org.neo4j.graphdb.traversal;

import org.eclipse.collections.api.map.primitive.MutableIntObjectMap;
import org.eclipse.collections.api.set.primitive.MutableLongSet;
import org.eclipse.collections.impl.map.mutable.primitive.IntObjectHashMap;
import org.eclipse.collections.impl.set.mutable.primitive.LongHashSet;

import org.neo4j.graphdb.Path;

class LevelUnique extends AbstractUniquenessFilter
{
    private final MutableIntObjectMap<MutableLongSet> idsPerLevel = new IntObjectHashMap<>();

    LevelUnique( PrimitiveTypeFetcher type )
    {
        super( type );
    }

    @Override
    public boolean check( TraversalBranch branch )
    {
        int level = branch.length();
        MutableLongSet levelIds = idsPerLevel.get( level );
        if ( levelIds == null )
        {
            levelIds = new LongHashSet();
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
