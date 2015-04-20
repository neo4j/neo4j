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
package org.neo4j.kernel.impl.core;

import java.util.EnumMap;
import java.util.Map;
import java.util.Map.Entry;

import org.neo4j.collection.primitive.Primitive;
import org.neo4j.collection.primitive.PrimitiveIntCollections;
import org.neo4j.collection.primitive.PrimitiveIntObjectMap;
import org.neo4j.collection.primitive.PrimitiveIntObjectVisitor;
import org.neo4j.function.primitive.FunctionFromPrimitiveLongLongToPrimitiveLong;
import org.neo4j.kernel.impl.store.record.Record;
import org.neo4j.kernel.impl.store.record.RelationshipGroupRecord;
import org.neo4j.kernel.impl.util.RelIdArray.DirectionWrapper;

import static java.lang.String.format;

public class DenseNodeChainPosition implements RelationshipLoadingPosition
{
    private final PrimitiveIntObjectMap<RelationshipLoadingPosition> positions = Primitive.intObjectMap();
    private final int[] types;
    private RelationshipLoadingPosition currentPosition;

    public DenseNodeChainPosition( Map<Integer, RelationshipGroupRecord> groups )
    {
        this.types = PrimitiveIntCollections.asArray( groups.keySet() );

        // Instantiate all positions eagerly so that we get a fixed point in time where
        // all positions where initialized. This is required for relationship addition filtering
        // during committing relationship changes to a NodeImpl.
        for ( Entry<Integer,RelationshipGroupRecord> entry : groups.entrySet() )
        {
            this.positions.put( entry.getKey(), new TypePosition( entry.getValue() ) );
        }
    }

    private DenseNodeChainPosition( final DenseNodeChainPosition copyFrom )
    {
        // Deep-copy of positions
        copyFrom.positions.visitEntries( new PrimitiveIntObjectVisitor<RelationshipLoadingPosition,RuntimeException>()
        {
            @Override
            public boolean visited( int type, RelationshipLoadingPosition originalPosition )
            {
                RelationshipLoadingPosition position = originalPosition.clone();
                positions.put( type, position );
                if ( originalPosition == copyFrom.currentPosition )
                {
                    currentPosition = position;
                }
                return false;
            }
        } );

        this.types = copyFrom.types;
    }

    @Override
    public long position( DirectionWrapper direction, int[] types )
    {
        if ( types.length == 0 )
        {
            types = this.types;
        }
        for ( int type : types )
        {
            RelationshipLoadingPosition position = getTypePosition( type );
            if ( position.hasMore( direction, types ) )
            {
                currentPosition = position;
                return position.position( direction, types );
            }
        }
        return Record.NO_NEXT_RELATIONSHIP.intValue();
    }

    private RelationshipLoadingPosition getTypePosition( int type )
    {
        RelationshipLoadingPosition position = positions.get( type );
        if ( position == null )
        {
            return RelationshipLoadingPosition.EMPTY;
        }
        return position;
    }

    @Override
    public long nextPosition( long nextPosition, DirectionWrapper direction, int[] types )
    {
        currentPosition.nextPosition( nextPosition, direction, types );
        if ( nextPosition != Record.NO_NEXT_RELATIONSHIP.intValue() )
        {
            return nextPosition;
        }
        return position( direction, types );
    }

    @Override
    public boolean hasMore( DirectionWrapper direction, int[] types )
    {
        if ( types.length == 0 )
        {
            types = this.types;
        }
        for ( int type : types )
        {
            RelationshipLoadingPosition position = positions.get( type );
            if ( position != null && position.hasMore( direction, types ) )
            {
                return true;
            }
        }
        return false;
    }

    @Override
    public boolean atPosition( DirectionWrapper direction, int type, long position )
    {
        RelationshipLoadingPosition typePosition = positions.get( type );
        return typePosition != null ? typePosition.atPosition( direction, type, position ) : false;
    }

    @Override
    public void patchPosition( final long nodeId, final FunctionFromPrimitiveLongLongToPrimitiveLong<RuntimeException> next )
    {
        positions.visitEntries( new PrimitiveIntObjectVisitor<RelationshipLoadingPosition,RuntimeException>()
        {
            @Override
            public boolean visited( int key, RelationshipLoadingPosition value )
            {
                value.patchPosition( nodeId, next );
                return false;
            }
        } );
    }

    @Override
    public RelationshipLoadingPosition clone()
    {
        return new DenseNodeChainPosition( this );
    }

    @Override
    public String toString()
    {
        StringBuilder builder = new StringBuilder( getClass().getSimpleName() + ":" );
        builder.append( format( "%n  positions: %s", positions ) );
        return builder.toString();
    }

    private static class TypePosition implements RelationshipLoadingPosition
    {
        private final EnumMap<DirectionWrapper, RelationshipLoadingPosition> directions =
                new EnumMap<>( DirectionWrapper.class );
        private RelationshipLoadingPosition currentPosition;

        TypePosition( RelationshipGroupRecord record )
        {
            for ( DirectionWrapper direction : DirectionWrapper.values() )
            {
                long firstRel = direction.getNextRel( record );
                directions.put( direction, firstRel != Record.NO_NEXT_RELATIONSHIP.intValue()
                        ? new SingleChainPosition( firstRel ) : RelationshipLoadingPosition.EMPTY );
            }
        }

        private TypePosition( TypePosition copyFrom )
        {
            for ( Entry<DirectionWrapper,RelationshipLoadingPosition> entry : copyFrom.directions.entrySet() )
            {
                RelationshipLoadingPosition position = entry.getValue().clone();
                this.directions.put( entry.getKey(), position );
                if ( entry.getValue() == copyFrom.currentPosition )
                {
                    this.currentPosition = position;
                }
            }
        }

        @Override
        public boolean atPosition( DirectionWrapper direction, int type, long position )
        {
            return directions.get( direction ).atPosition( direction, type, position );
        }

        @Override
        public long position( DirectionWrapper direction, int[] types )
        {
            if ( direction == DirectionWrapper.BOTH )
            {
                for ( RelationshipLoadingPosition position : directions.values() )
                {
                    if ( position.hasMore( direction, types ) )
                    {
                        currentPosition = position;
                        return position.position( direction, types );
                    }
                }
            }
            else
            {
                for ( DirectionWrapper dir : new DirectionWrapper[] {direction, DirectionWrapper.BOTH} )
                {
                    RelationshipLoadingPosition position = directions.get( dir );
                    if ( position.hasMore( dir, types ) )
                    {
                        currentPosition = position;
                        return position.position( dir, types );
                    }
                }
            }
            return Record.NO_NEXT_RELATIONSHIP.intValue();
        }

        @Override
        public long nextPosition( long position, DirectionWrapper direction, int[] types )
        {
            currentPosition.nextPosition( position, direction, types );
            if ( position != Record.NO_NEXT_RELATIONSHIP.intValue() )
            {
                return position;
            }
            return position( direction, types );
        }

        @Override
        public boolean hasMore( DirectionWrapper direction, int[] types )
        {
            if ( direction == DirectionWrapper.BOTH )
            {
                return directions.get( DirectionWrapper.OUTGOING ).hasMore( direction, types ) ||
                        directions.get( DirectionWrapper.INCOMING ).hasMore( direction, types ) ||
                        directions.get( DirectionWrapper.BOTH ).hasMore( direction, types );
            }
            else
            {
                return directions.get( direction ).hasMore( direction, types ) ||
                        directions.get( DirectionWrapper.BOTH ).hasMore( direction, types );
            }
        }

        @Override
        public void patchPosition( long nodeId, FunctionFromPrimitiveLongLongToPrimitiveLong<RuntimeException> next )
        {
            for ( RelationshipLoadingPosition position : directions.values() )
            {
                position.patchPosition( nodeId, next );
            }
        }

        @Override
        public RelationshipLoadingPosition clone()
        {
            return new TypePosition( this );
        }

        @Override
        public String toString()
        {
            return directions.toString();
        }
    }
}
