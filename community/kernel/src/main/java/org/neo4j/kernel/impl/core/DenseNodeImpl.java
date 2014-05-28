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
package org.neo4j.kernel.impl.core;

import java.util.Iterator;

import org.neo4j.graphdb.Direction;
import org.neo4j.kernel.impl.nioneo.xa.RelationshipChainLoader;
import org.neo4j.kernel.impl.util.RelIdArray;

import static org.neo4j.helpers.collection.IteratorUtil.iterator;

public class DenseNodeImpl extends NodeImpl
{
    DenseNodeImpl( long id )
    {
        super( id );
    }

    @Override
    public int getDegree( NodeManager nm, int type, RelationshipChainLoader loader )
    {
        return getDegree( nm, type, Direction.BOTH, loader );
    }

    @Override
    public int getDegree( NodeManager nm, Direction direction, RelationshipChainLoader loader )
    {
        return loader.getRelationshipCount( getId(), -1, RelIdArray.wrap( direction ) );
    }

    @Override
    public int getDegree( NodeManager nm, int type, Direction direction, RelationshipChainLoader loader )
    {
        return loader.getRelationshipCount( getId(), type, RelIdArray.wrap( direction ) );
    }

    @Override
    public Iterator<Integer> getRelationshipTypes( NodeManager nm, RelationshipChainLoader loader )
    {
        return hasMoreRelationshipsToLoad() ? iterator( loader.getRelationshipTypes( getId() ) ) :
            super.getRelationshipTypes( nm, loader );
    }
}
