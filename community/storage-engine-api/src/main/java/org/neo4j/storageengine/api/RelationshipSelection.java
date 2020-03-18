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
import org.eclipse.collections.impl.iterator.ImmutableEmptyLongIterator;

import java.util.Arrays;

import org.neo4j.collection.PrimitiveLongCollections;
import org.neo4j.graphdb.Direction;
import org.neo4j.storageengine.api.txstate.NodeState;

/**
 * Used to specify a selection of relationships to get from a node.
 */
public abstract class RelationshipSelection
{
    /**
     * Tests whether a relationship of a certain type should be part of this selection.
     * @param type the relationship type id of the relationship to test.
     * @return whether or not this relationship type is part of this selection.
     */
    public abstract boolean test( int type );

    /**
     * Tests whether a relationship of a certain type and direction should be part of this selection.
     * @param type the relationship type id of the relationship to test.
     * @param direction {@link RelationshipDirection} of the relationship to test.
     * @return whether or not this relationship type is part of this selection.
     */
    public abstract boolean test( int type, RelationshipDirection direction );

    /**
     * Selects the correct set of added relationships from transaction state, based on the selection criteria.
     * @param transactionState the {@link NodeState} to select added relationships from.
     * @return a {@link LongIterator} of added relationships matching the selection criteria from transaction state.
     */
    public abstract LongIterator addedRelationship( NodeState transactionState );

    public static RelationshipSelection selection( int[] types, Direction direction )
    {
        if ( types == null )
        {
            return selection( direction );
        }
        else if ( types.length == 0 )
        {
            return NO_RELATIONSHIPS;
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

        @Override
        public String toString()
        {
            return "RelationshipSelection[" + "type=" + type + ", " + direction + "]";
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
                // We have to avoid duplication here, so check backwards if this type exists earlier in the array
                if ( !existsEarlier( types, i ) )
                {
                    all[i] = transactionState.getAddedRelationships( direction, types[i] );
                }
            }
            return PrimitiveLongCollections.concat( all );
        }

        @Override
        public String toString()
        {
            return "RelationshipSelection[" + "types=" + Arrays.toString( types ) + ", " + direction + "]";
        }

        private boolean existsEarlier( int[] types, int i )
        {
            int candidateType = types[i];
            for ( int j = i - 1; j >= 0 ; j-- )
            {
                if ( candidateType == types[j] )
                {
                    return true;
                }
            }
            return false;
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

        @Override
        public String toString()
        {
            return "RelationshipSelection[" + direction + "]";
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

        @Override
        public String toString()
        {
            return "RelationshipSelection[*]";
        }
    };

    public static RelationshipSelection NO_RELATIONSHIPS = new RelationshipSelection()
    {
        @Override
        public boolean test( int type )
        {
            return false;
        }

        @Override
        public boolean test( int type, RelationshipDirection direction )
        {
            return false;
        }

        @Override
        public LongIterator addedRelationship( NodeState transactionState )
        {
            return ImmutableEmptyLongIterator.INSTANCE;
        }

        @Override
        public String toString()
        {
            return "RelationshipSelection[NOTHING]";
        }
    };

    private static boolean matchesDirection( RelationshipDirection relationshipDirection, Direction selectionDirection )
    {
        switch ( selectionDirection )
        {
        case OUTGOING:
            return relationshipDirection == RelationshipDirection.OUTGOING || relationshipDirection == RelationshipDirection.LOOP;
        case INCOMING:
            return relationshipDirection == RelationshipDirection.INCOMING || relationshipDirection == RelationshipDirection.LOOP;
        case BOTH:
            return true;
        default:
            throw new UnsupportedOperationException( selectionDirection.name() );
        }
    }
}
