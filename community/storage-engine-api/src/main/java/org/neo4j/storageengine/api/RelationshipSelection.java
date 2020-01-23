/*
 * Copyright (c) 2002-2020 "Neo4j,"
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
package org.neo4j.storageengine.api;

import org.apache.commons.lang3.ArrayUtils;
import org.eclipse.collections.api.iterator.LongIterator;

import org.neo4j.collection.PrimitiveLongCollections;
import org.neo4j.graphdb.Direction;
import org.neo4j.storageengine.api.txstate.NodeState;

public abstract class RelationshipSelection
{
    public abstract boolean test( int type );

    public abstract boolean test( int type, RelationshipDirection direction );

    public boolean isInitialized()
    {
        return true;
    }

    public abstract LongIterator addedRelationship( NodeState transactionState );

    public static RelationshipSelection lazyCapture()
    {
        return new RelationshipSelection()
        {
            private int type = -1;
            private RelationshipDirection direction;

            @Override
            public boolean test( int type, RelationshipDirection direction )
            {
                if ( this.direction == null )
                {
                    // Capture filtering from the first relationship we encounter. It's an old storage format thing
                    this.type = type;
                    this.direction = direction;
                    return true;
                }

                return this.type == type && this.direction.equals( direction );
            }

            @Override
            public boolean test( int type )
            {
                if ( this.type == -1 )
                {
                    this.type = type;
                    return true;
                }

                return this.type == type;
            }

            @Override
            public boolean isInitialized()
            {
                return direction != null && type != -1;
            }

            @Override
            public LongIterator addedRelationship( NodeState transactionState )
            {
                return transactionState.getAddedRelationships( asSelectionDirection( direction ), type );
            }
        };
    }

    public static RelationshipSelection selection( int[] types, Direction direction )
    {
        if ( types == null || types.length == 0 )
        {
            return direction == Direction.BOTH ? ALL_RELATIONSHIPS : new DirectionalAllTypes( direction );
        }
        else if ( types.length == 1 )
        {
            return new DirectionalSingleType( types[0], direction );
        }
        return new DirectionalMultipleTypes( types, direction );
    }

    public static RelationshipSelection selection( int type, Direction direction )
    {
        return new DirectionalSingleType( type, direction );
    }

    public static RelationshipSelection selection( Direction direction )
    {
        return direction == Direction.BOTH ? ALL_RELATIONSHIPS : new DirectionalAllTypes( direction );
    }

    private static class DirectionalSingleType extends RelationshipSelection
    {
        private final int type;
        private final Direction direction;

        DirectionalSingleType( int type, Direction direction )
        {
            this.type = type;
            this.direction = direction;
        }

        @Override
        public boolean test( int type )
        {
            return this.type == type;
        }

        @Override
        public boolean test( int type, RelationshipDirection direction )
        {
            return this.type == type && matchesDirection( direction, this.direction );
        }

        @Override
        public LongIterator addedRelationship( NodeState transactionState )
        {
            return transactionState.getAddedRelationships( direction, type );
        }
    }

    private static class DirectionalMultipleTypes extends RelationshipSelection
    {
        private final int[] types;
        private final Direction direction;

        DirectionalMultipleTypes( int[] types, Direction direction )
        {
            this.types = types;
            this.direction = direction;
        }

        @Override
        public boolean test( int type )
        {
            return ArrayUtils.contains( types, type );
        }

        @Override
        public boolean test( int type, RelationshipDirection direction )
        {
            return test( type ) && matchesDirection( direction, this.direction );
        }

        @Override
        public LongIterator addedRelationship( NodeState transactionState )
        {
            LongIterator[] all = new LongIterator[types.length];
            for ( int i = 0; i < types.length; i++ )
            {
                all[i] = transactionState.getAddedRelationships( direction, types[i] );
            }
            return PrimitiveLongCollections.concat( all );
        }
    }

    private static class DirectionalAllTypes extends RelationshipSelection
    {
        private final Direction direction;

        DirectionalAllTypes( Direction direction )
        {
            this.direction = direction;
        }

        @Override
        public boolean test( int type )
        {
            return true;
        }

        @Override
        public boolean test( int type, RelationshipDirection direction )
        {
            return matchesDirection( direction, this.direction );
        }

        @Override
        public LongIterator addedRelationship( NodeState transactionState )
        {
            return transactionState.getAddedRelationships( direction );
        }
    }

    public static RelationshipSelection ALL_RELATIONSHIPS = new RelationshipSelection()
    {
        @Override
        public boolean test( int type )
        {
            return true;
        }

        @Override
        public boolean test( int type, RelationshipDirection direction )
        {
            return true;
        }

        @Override
        public LongIterator addedRelationship( NodeState transactionState )
        {
            return transactionState.getAddedRelationships();
        }
    };

    private static boolean matchesDirection( RelationshipDirection relationshipDirection, Direction selectionDirection )
    {
        switch ( selectionDirection )
        {
        case OUTGOING:
            return relationshipDirection == RelationshipDirection.OUTGOING;
        case INCOMING:
            return relationshipDirection == RelationshipDirection.INCOMING;
        case BOTH:
            return true;
        default:
            throw new UnsupportedOperationException( selectionDirection.name() );
        }
    }
}
