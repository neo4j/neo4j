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

import org.neo4j.collection.primitive.Primitive;
import org.neo4j.collection.primitive.PrimitiveLongSet;
import org.neo4j.graphdb.Path;

class GloballyUnique extends AbstractUniquenessFilter
{
    private final PrimitiveLongSet visited = Primitive.longSet( 1 << 12 );

    GloballyUnique( PrimitiveTypeFetcher type )
    {
        super( type );
    }

    @Override
    public boolean check( TraversalBranch branch )
    {
        return visited.add( type.getId( branch ) );
    }

    @Override
    public boolean checkFull( Path path )
    {
        // Since this is for bidirectional uniqueness checks and
        // uniqueness is enforced through the shared "visited" set
        // this uniqueness contract is fulfilled automatically.
        return true;
    }
}
