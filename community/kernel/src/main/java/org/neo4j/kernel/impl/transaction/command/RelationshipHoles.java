/*
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
package org.neo4j.kernel.impl.transaction.command;

import org.neo4j.collection.primitive.PrimitiveLongObjectMap;
import org.neo4j.collection.primitive.PrimitiveLongSet;
import org.neo4j.collection.primitive.PrimitiveLongVisitor;
import org.neo4j.function.primitive.FunctionFromPrimitiveLongLongToPrimitiveLong;
import org.neo4j.function.primitive.PrimitiveLongPredicate;
import org.neo4j.kernel.impl.core.CacheAccessBackDoor;
import org.neo4j.kernel.impl.store.record.RelationshipRecord;

import static org.neo4j.collection.primitive.Primitive.longObjectMap;
import static org.neo4j.collection.primitive.Primitive.longSet;
import static org.neo4j.kernel.impl.transaction.state.RelationshipChainLoader.followRelationshipChain;

/**
 * Keeps track of deleted relationships in a transaction and can detect and navigate past holes of deleted
 * relationships via their next pointers, such that a relationship that is in use after a hole can be retrieved.
 */
public class RelationshipHoles implements
        PrimitiveLongPredicate, FunctionFromPrimitiveLongLongToPrimitiveLong<RuntimeException>
{
    private final PrimitiveLongSet nodes = longSet( 8 );
    private final PrimitiveLongObjectMap<RelationshipRecord> relationships = longObjectMap( 32 );

    /**
     * Keeps track of the given relationship and makes use of it later in {@link #accept(long)}
     * and {@link #apply(long, long)}
     *
     * @param deletedRelationship relationship that has been deleted in this transaction.
     */
    public void deleted( RelationshipRecord deletedRelationship )
    {
        nodes.add( deletedRelationship.getFirstNode() );
        nodes.add( deletedRelationship.getSecondNode() );
        relationships.put( deletedRelationship.getId(), deletedRelationship );
    }

    /**
     * @return whether or not relationship by id {@code relationshipId} has been deleted in this transaction.
     */
    @Override
    public boolean accept( long relationshipId )
    {
        return relationships.containsKey( relationshipId );
    }

    /**
     * @return the next-most {@link RelationshipRecord#inUse() inUse} relationship given the deleted relationship
     * with id {@code deletedRelationshipId}.
     */
    @Override
    public long apply( long nodeId, long deletedRelationshipId ) throws RuntimeException
    {
        long relationshipId = deletedRelationshipId;
        while ( true )
        {
            RelationshipRecord relationship = relationships.get( relationshipId );
            if ( relationship == null )
            {
                return relationshipId;
            }
            relationshipId = followRelationshipChain( nodeId, relationship );
        }
    }

    public void apply( final CacheAccessBackDoor cacheAccess )
    {
        nodes.visitKeys( new PrimitiveLongVisitor<RuntimeException>()
        {
            @Override
            public boolean visited( long nodeId )
            {
                cacheAccess.patchDeletedRelationshipNodes( nodeId, RelationshipHoles.this );
                return false;
            }
        } );
    }
}
