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

import java.util.EnumMap;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;

import org.neo4j.collection.primitive.PrimitiveIntCollections;
import org.neo4j.kernel.impl.store.record.Record;
import org.neo4j.kernel.impl.store.record.RelationshipGroupRecord;
import org.neo4j.kernel.impl.util.RelIdArray.DirectionWrapper;

import static java.lang.String.format;

public class DenseNodeChainPosition implements RelationshipLoadingPosition
{
    private final Map<Integer, RelationshipLoadingPosition> positions = new HashMap<>();
    private final Map<Integer, RelationshipGroupRecord> groups;
    private final int[] types;
    private RelationshipLoadingPosition currentPosition;

    public DenseNodeChainPosition( Map<Integer, RelationshipGroupRecord> groups )
    {
        this.types = PrimitiveIntCollections.asArray( groups.keySet().iterator() );
        this.groups = groups;
    }

    private DenseNodeChainPosition( DenseNodeChainPosition copyFrom )
    {
        // Deep-copy of positions
        for ( Entry<Integer, RelationshipLoadingPosition> entry : copyFrom.positions.entrySet() )
        {
            RelationshipLoadingPosition position = entry.getValue().clone();
            this.positions.put( entry.getKey(), position );
            if ( entry.getValue() == copyFrom.currentPosition )
            {
                this.currentPosition = position;
            }
        }

        this.groups = new HashMap<>( copyFrom.groups );
        this.types = copyFrom.types.clone();
    }

    @Override
    public void updateFirst( long first )
    {
        // TODO here we need relationship groups for any new
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
            RelationshipGroupRecord record = groups.get( type );
            position = record != null ? new TypePosition( record ) : RelationshipLoadingPosition.EMPTY;
            positions.put( type, position );
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
            if ( position == null || position.hasMore( direction, types ) )
            {
                return true;
            }
        }
        return false;
    }

    @Override
    public void compareAndAdvance( long relIdDeleted, long nextRelId )
    {
        for ( RelationshipLoadingPosition typePosition : positions.values() )
        {
            typePosition.compareAndAdvance( relIdDeleted, nextRelId );
        }
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
        builder.append( format( "%n  groups: %s", groups ) );
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
            for ( DirectionWrapper dir : DirectionWrapper.values() )
            {
                directions.put( dir, new SingleChainPosition( dir.getNextRel( record ) ) );
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
        public void updateFirst( long first )
        {
            throw new UnsupportedOperationException();
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
        public void compareAndAdvance( long relIdDeleted, long nextRelId )
        {
            for ( RelationshipLoadingPosition position : directions.values() )
            {
                position.compareAndAdvance( relIdDeleted, nextRelId );
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
