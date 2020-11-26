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
import org.neo4j.util.Preconditions;

import static org.neo4j.token.api.TokenConstants.ANY_RELATIONSHIP_TYPE;

/**
 * Used to specify a selection of relationships to get from a node.
 */
public abstract class RelationshipSelection
{
    /**
     * Tests whether a relationship of a certain type should be part of this selection.
     *
     * @param type the relationship type id of the relationship to test.
     * @return whether or not this relationship type is part of this selection.
     */
    public abstract boolean test( int type );

    /**
     * Tests whether a relationship of a certain type between certain nodes should be part of this selection.
     *
     * @param type the relationship type id of the relationship to test.
     * @param sourceReference reference to start node.
     * @param targetReference reference to end node.
     * @return whether or not this relationship type is part of this selection.
     */
    public boolean test( int type, long sourceReference, long targetReference )
    {
        return test( type );
    }

    /**
     * Tests whether a relationship of a certain direction should be part of this selection.
     * @param direction {@link RelationshipDirection} of the relationship to test.
     * @return whether or not this relationship is part of this selection.
     */
    public abstract boolean test( RelationshipDirection direction );

    /**
     * @return the {@link Direction} of relationships in this selection.
     */
    public abstract Direction direction();

    /**
     * Tests whether a relationship of a certain type and direction should be part of this selection.
     *
     * @param type      the relationship type id of the relationship to test.
     * @param direction {@link RelationshipDirection} of the relationship to test.
     * @return whether or not this relationship type is part of this selection.
     */
    public abstract boolean test( int type, RelationshipDirection direction );

    /**
     * @return the number of criteria in this selection. One {@link Criterion} is a type and {@link Direction}.
     */
    public abstract int numberOfCriteria();

    /**
     * @param index which {@link Criterion} to access.
     * @return the {@link Criterion} for the given index, must be between 0 and {@link #numberOfCriteria()}.
     */
    public abstract Criterion criterion( int index );

    /**
     * @return {@code true} if this selection is limited on type, i.e. if number of criteria matches number of selected types,
     * otherwise {@code false}.
     */
    public abstract boolean isTypeLimited();

    /**
     * @return {@code true} if this selection is limited in any way, otherwise {@code false} where all relationships should be selected.
     */
    public boolean isLimited()
    {
        return true;
    }

    /**
     * Selects the correct set of added relationships from transaction state, based on the selection criteria.
     *
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

    private abstract static class Directional extends RelationshipSelection
    {
        protected final Direction direction;

        Directional( Direction direction )
        {
            this.direction = direction;
        }

        @Override
        public boolean test( RelationshipDirection direction )
        {
            return RelationshipSelection.matchesDirection( direction, this.direction );
        }

        @Override
        public Direction direction()
        {
            return direction;
        }
    }

    private abstract static class DirectionalSingleCriterion extends Directional implements Criterion
    {
        protected final int type;

        DirectionalSingleCriterion( int type, Direction direction )
        {
            super( direction );
            this.type = type;
        }

        @Override
        public boolean test( RelationshipDirection direction )
        {
            return RelationshipSelection.matchesDirection( direction, this.direction );
        }

        @Override
        public int numberOfCriteria()
        {
            return 1;
        }

        @Override
        public Criterion criterion( int index )
        {
            Preconditions.checkArgument( index == 0, "Unknown criterion index %d", index );
            return this;
        }

        @Override
        public int type()
        {
            return type;
        }

        @Override
        public Direction direction()
        {
            return direction;
        }
    }

    private static class DirectionalSingleType extends DirectionalSingleCriterion
    {
        DirectionalSingleType( int type, Direction direction )
        {
            super( type, direction );
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
        public boolean isTypeLimited()
        {
            return true;
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

    private static class DirectionalMultipleTypes extends Directional
    {
        private final int[] types;

        DirectionalMultipleTypes( int[] types, Direction direction )
        {
            super( direction );
            this.types = types;
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
        public int numberOfCriteria()
        {
            return types.length;
        }

        @Override
        public boolean isTypeLimited()
        {
            return true;
        }

        @Override
        public Criterion criterion( int index )
        {
            Preconditions.checkArgument( index < types.length, "Unknown criterion index %d", index );
            return new CriterionImpl( types[index], direction );
        }

        @Override
        public LongIterator addedRelationship( NodeState transactionState )
        {
            LongIterator[] all = new LongIterator[types.length];
            int index = 0;
            for ( int i = 0; i < types.length; i++ )
            {
                // We have to avoid duplication here, so check backwards if this type exists earlier in the array
                if ( !existsEarlier( types, i ) )
                {
                    all[index++] = transactionState.getAddedRelationships( direction, types[i] );
                }
            }
            if ( index != types.length )
            {
                all = Arrays.copyOf( all, index );
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
            for ( int j = i - 1; j >= 0; j-- )
            {
                if ( candidateType == types[j] )
                {
                    return true;
                }
            }
            return false;
        }
    }

    private static class DirectionalAllTypes extends DirectionalSingleCriterion
    {
        DirectionalAllTypes( Direction direction )
        {
            super( ANY_RELATIONSHIP_TYPE, direction );
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
        public boolean isTypeLimited()
        {
            return false;
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

    public interface Criterion
    {
        int type();

        Direction direction();
    }

    public static class CriterionImpl implements Criterion
    {
        private final int type;
        private final Direction direction;

        CriterionImpl( int type, Direction direction )
        {
            this.type = type;
            this.direction = direction;
        }

        @Override
        public int type()
        {
            return type;
        }

        @Override
        public Direction direction()
        {
            return direction;
        }
    }

    public static final RelationshipSelection ALL_RELATIONSHIPS = new RelationshipSelection()
    {
        private final Criterion ALL_CRITERIA = new CriterionImpl( ANY_RELATIONSHIP_TYPE, Direction.BOTH );

        @Override
        public boolean test( int type )
        {
            return true;
        }

        @Override
        public boolean test( RelationshipDirection direction )
        {
            return true;
        }

        @Override
        public Direction direction()
        {
            return Direction.BOTH;
        }

        @Override
        public boolean test( int type, RelationshipDirection direction )
        {
            return true;
        }

        @Override
        public int numberOfCriteria()
        {
            return 1;
        }

        @Override
        public Criterion criterion( int index )
        {
            Preconditions.checkArgument( index == 0, "Unknown criterion index %d", index );
            return ALL_CRITERIA;
        }

        @Override
        public boolean isTypeLimited()
        {
            return false;
        }

        @Override
        public boolean isLimited()
        {
            return false;
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

    public static final RelationshipSelection NO_RELATIONSHIPS = new RelationshipSelection()
    {
        @Override
        public boolean test( int type )
        {
            return false;
        }

        @Override
        public boolean test( RelationshipDirection direction )
        {
            return false;
        }

        @Override
        public Direction direction()
        {
            return Direction.BOTH;
        }

        @Override
        public boolean test( int type, RelationshipDirection direction )
        {
            return false;
        }

        @Override
        public int numberOfCriteria()
        {
            return 0;
        }

        @Override
        public Criterion criterion( int index )
        {
            throw new IllegalArgumentException( "Unknown criterion index " + index );
        }

        @Override
        public boolean isTypeLimited()
        {
            return true;
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
