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

import org.neo4j.graphdb.Direction;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.kernel.impl.util.RelIdArray;
import org.neo4j.kernel.impl.util.RelIdArray.DirectionWrapper;

public class DenseNodeImpl extends NodeImpl
{
    DenseNodeImpl( long id )
    {
        super( id );
    }

    @Override
    public int getDegree( NodeManager nm, RelationshipType type )
    {
        return getDegree( nm, type, Direction.BOTH );
    }
    
    @Override
    public int getDegree( NodeManager nm, Direction direction )
    {
        int count = 0;
        TransactionState state = nm.getTransactionState();
        DirectionWrapper dir = RelIdArray.wrap( direction );
        boolean hasStateChanges = state.hasChanges();
        if ( !hasStateChanges || !state.getCreatedNodes().contains( getId() ) )
        {
            count = nm.getRelationshipCount( this, null, dir );
        }
        if ( hasStateChanges )
        {
            count += degreeFromTxState( nm, dir );
        }
        return count;
    }
    
    @Override
    public int getDegree( NodeManager nm, RelationshipType type, Direction direction )
    {
        int count = 0;
        TransactionState state = nm.getTransactionState();
        DirectionWrapper dir = RelIdArray.wrap( direction );
        boolean hasStateChanges = state.hasChanges();
        if ( !hasStateChanges || !state.getCreatedNodes().contains( getId() ) )
        {
            count = nm.getRelationshipCount( this, type, dir );
        }
        if ( hasStateChanges )
        {
            count += degreeFromTxState( nm, dir, nm.getRelationshipTypeIdFor( type ) );
        }
        return count;
    }

    @Override
    public Iterable<RelationshipType> getRelationshipTypes( NodeManager nm )
    {
        return hasMoreRelationshipsToLoad() ? nm.getRelationshipTypes( this ) : super.getRelationshipTypes( nm );
    }
}
