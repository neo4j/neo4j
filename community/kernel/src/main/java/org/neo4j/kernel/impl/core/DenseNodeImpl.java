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
import org.neo4j.kernel.impl.api.DegreeVisitor;
import org.neo4j.kernel.impl.api.store.CacheUpdateListener;
import org.neo4j.kernel.impl.util.RelIdArray;

import static org.neo4j.helpers.collection.IteratorUtil.iterator;

public class DenseNodeImpl extends NodeImpl
{
    public DenseNodeImpl( long id )
    {
        super( id );
    }

    @Override
    public int getDegree( RelationshipLoader relationshipLoader, int type,
            CacheUpdateListener cacheUpdateListener )
    {
        return getDegree( relationshipLoader, type, Direction.BOTH, cacheUpdateListener );
    }

    @Override
    public int getDegree( RelationshipLoader relationshipLoader, Direction direction,
            CacheUpdateListener cacheUpdateListener )
    {
        return relationshipLoader.getRelationshipCount( getId(), -1, RelIdArray.wrap( direction ) );
    }

    @Override
    public int getDegree( RelationshipLoader relationshipLoader, int type, Direction direction,
            CacheUpdateListener cacheUpdateListener )
    {
        return relationshipLoader.getRelationshipCount( getId(), type, RelIdArray.wrap( direction ) );
    }

    @Override
    public Iterator<Integer> getRelationshipTypes( RelationshipLoader relationshipLoader,
            CacheUpdateListener cacheUpdateListener )
    {
        return hasMoreRelationshipsToLoad() ? iterator( relationshipLoader.getRelationshipTypes( getId() ) ) :
            super.getRelationshipTypes( relationshipLoader, cacheUpdateListener );
    }

    @Override
    public void visitDegrees( RelationshipLoader relationshipLoader, DegreeVisitor visitor,
                              CacheUpdateListener cacheUpdateListener )
    {
        relationshipLoader.visitRelationshipCounts( getId(), visitor );
    }
}
