/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [https://neo4j.com]
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
 * along with this program.  If not, see <https://www.gnu.org/licenses/>.
 */
package org.neo4j.storageengine.api;

import static java.lang.String.format;
import static org.neo4j.token.api.TokenConstants.ANY_RELATIONSHIP_TYPE;

import java.util.Arrays;
import org.apache.commons.lang3.ArrayUtils;
import org.eclipse.collections.api.iterator.LongIterator;
import org.eclipse.collections.impl.iterator.ImmutableEmptyLongIterator;
import org.neo4j.collection.PrimitiveLongCollections;
import org.neo4j.graphdb.Direction;
import org.neo4j.storageengine.api.txstate.NodeState;

/**
 * Used to specify a selection of relationships to get from a node.
 */
public abstract class RelationshipSelection {
    /**
     * Tests whether a relationship of a certain type should be part of this selection.
     *
     * @param type the relationship type id of the relationship to test.
     * @return whether or not this relationship type is part of this selection.
     */
    public abstract boolean test(int type);

    /**
     * Tests whether a relationship of a certain direction should be part of this selection.
     *
     * @param direction {@link RelationshipDirection} of the relationship to test.
     * @return whether or not this relationship is part of this selection.
     */
    public abstract boolean test(RelationshipDirection direction);

    /**
     * @param type relationship type to get selected {@link Direction} for.
     * @return the {@link Direction} of relationships for the given {@code type}.
     */
    public abstract Direction direction(int type);

    /**
     * Tests whether a relationship of a certain type and direction should be part of this selection.
     *
     * @param type      the relationship type id of the relationship to test.
     * @param direction {@link RelationshipDirection} of the relationship to test.
     * @return whether or not this relationship type is part of this selection.
     */
    public abstract boolean test(int type, RelationshipDirection direction);

    /**
     * @return the number of criteria in this selection. One criterion is a type and {@link Direction}.
     */
    public abstract int numberOfCriteria();

    /**
     * @param index which criterion to access.
     * @return the type for the given index, must be between 0 and {@link #numberOfCriteria()}.
     */
    public abstract int criterionType(int index);

    /**
     * @param index which criterion to access.
     * @return the {@link Direction} for the given index, must be between 0 and {@link #numberOfCriteria()}.
     */
    public abstract Direction criterionDirection(int index);

    /**
     * @return {@code true} if this selection is limited on type, i.e. if number of criteria matches number of selected types,
     * otherwise {@code false}.
     */
    public abstract boolean isTypeLimited();

    /**
     * @return {@code true} if this selection is limited in any way, otherwise {@code false} where all relationships should be selected.
     */
    public boolean isLimited() {
        return true;
    }

    /**
     * @return the highest possible type in the selection.
     */
    public abstract int highestType();

    /**
     * Selects the correct set of added relationships from transaction state, based on the selection criteria.
     *
     * @param transactionState the {@link NodeState} to select added relationships from.
     * @return a {@link LongIterator} of added relationships matching the selection criteria from transaction state.
     */
    public abstract LongIterator addedRelationships(NodeState transactionState);

    public abstract RelationshipSelection reverse();

    public static RelationshipSelection selection(int[] types, Direction direction) {
        if (types == null) {
            return selection(direction);
        } else if (types.length == 0) {
            return NO_RELATIONSHIPS;
        } else if (types.length == 1) {
            return new DirectionalSingleType(types[0], direction);
        }
        return new DirectionalMultipleTypes(types, direction);
    }

    public static RelationshipSelection selection(int type, Direction direction) {
        return new DirectionalSingleType(type, direction);
    }

    public static RelationshipSelection selection(Direction direction) {
        return direction == Direction.BOTH ? ALL_RELATIONSHIPS : new DirectionalAllTypes(direction);
    }

    /**
     * Allows specification of types for the three different directions. Assumes that the
     * three arrays are all pairwise disjoint. A null array signifies that we allow all types in the corresponding
     * direction.
     * <p>
     * If one of the directed arrays are null, then the other directed array must be empty per the disjoint
     * assumption. If the bothTypes array is null, then both other arrays need to be empty.
     */
    public static RelationshipSelection selection(DirectedTypes directedTypes) {
        if (directedTypes.allowsAll()) {
            return ALL_RELATIONSHIPS;
        }

        if (!directedTypes.hasTypesInBothDirections()) {
            if (!directedTypes.hasSomeOutgoing()) {
                return selection(directedTypes.typesWithoutDirections(), Direction.INCOMING);
            } else if (!directedTypes.hasSomeIncoming()) {
                return selection(directedTypes.typesWithoutDirections(), Direction.OUTGOING);
            }
        } else if (!directedTypes.hasSomeOutgoing() && !directedTypes.hasSomeIncoming()) {
            return selection(directedTypes.typesWithoutDirections(), Direction.BOTH);
        }

        return new MultiDirectionalMultiType(directedTypes);
    }

    private abstract static class Directional extends RelationshipSelection {
        protected final Direction direction;

        Directional(Direction direction) {
            this.direction = direction;
        }

        @Override
        public boolean test(RelationshipDirection direction) {
            return direction.matches(this.direction);
        }

        @Override
        public Direction direction(int type) {
            return direction;
        }
    }

    private abstract static class DirectionalSingleCriterion extends Directional {
        protected final int type;

        DirectionalSingleCriterion(int type, Direction direction) {
            super(direction);
            this.type = type;
        }

        @Override
        public int numberOfCriteria() {
            return 1;
        }

        @Override
        public Direction criterionDirection(int index) {
            assert index == 0;
            return direction;
        }

        @Override
        public int criterionType(int index) {
            assert index == 0;
            return type;
        }

        @Override
        public int highestType() {
            return type;
        }
    }

    private static class DirectionalSingleType extends DirectionalSingleCriterion {
        DirectionalSingleType(int type, Direction direction) {
            super(type, direction);
        }

        @Override
        public boolean test(int type) {
            return this.type == type;
        }

        @Override
        public boolean test(int type, RelationshipDirection direction) {
            return this.type == type && direction.matches(this.direction);
        }

        @Override
        public boolean isTypeLimited() {
            return true;
        }

        @Override
        public LongIterator addedRelationships(NodeState transactionState) {
            return transactionState.getAddedRelationships(direction, type);
        }

        @Override
        public RelationshipSelection reverse() {
            return Direction.BOTH.equals(direction) ? this : new DirectionalSingleType(type, direction.reverse());
        }

        @Override
        public String toString() {
            return "RelationshipSelection[" + "type=" + type + ", " + direction + "]";
        }
    }

    private static class DirectionalMultipleTypes extends Directional {
        private final int[] types;
        private final int highestType;

        DirectionalMultipleTypes(int[] types, Direction direction) {
            super(direction);
            this.types = types.clone();
            this.highestType = findHighestType(this.types);
        }

        private static int findHighestType(int[] types) {
            int highest = -1;
            for (int type : types) {
                highest = Math.max(highest, type);
            }
            return highest;
        }

        @Override
        public boolean test(int type) {
            return ArrayUtils.contains(types, type);
        }

        @Override
        public boolean test(int type, RelationshipDirection direction) {
            return test(type) && direction.matches(this.direction);
        }

        @Override
        public int numberOfCriteria() {
            return types.length;
        }

        @Override
        public boolean isTypeLimited() {
            return true;
        }

        @Override
        public Direction criterionDirection(int index) {
            assert index < types.length;
            return direction;
        }

        @Override
        public int criterionType(int index) {
            assert index < types.length;
            return types[index];
        }

        @Override
        public int highestType() {
            return highestType;
        }

        @Override
        public LongIterator addedRelationships(NodeState transactionState) {
            LongIterator[] all = new LongIterator[types.length];
            int index = 0;
            for (int i = 0; i < types.length; i++) {
                // We have to avoid duplication here, so check backwards if this type exists earlier in the array
                if (!existsEarlier(types, i)) {
                    all[index++] = transactionState.getAddedRelationships(direction, types[i]);
                }
            }
            if (index != types.length) {
                all = Arrays.copyOf(all, index);
            }
            return PrimitiveLongCollections.concat(all);
        }

        @Override
        public RelationshipSelection reverse() {
            return Direction.BOTH.equals(direction) ? this : new DirectionalMultipleTypes(types, direction.reverse());
        }

        @Override
        public String toString() {
            return "RelationshipSelection[" + "types=" + Arrays.toString(types) + ", " + direction + "]";
        }

        private static boolean existsEarlier(int[] types, int i) {
            int candidateType = types[i];
            for (int j = i - 1; j >= 0; j--) {
                if (candidateType == types[j]) {
                    return true;
                }
            }
            return false;
        }
    }

    private static class DirectionalAllTypes extends DirectionalSingleCriterion {
        DirectionalAllTypes(Direction direction) {
            super(ANY_RELATIONSHIP_TYPE, direction);
        }

        @Override
        public boolean test(int type) {
            return true;
        }

        @Override
        public boolean test(int type, RelationshipDirection direction) {
            return direction.matches(this.direction);
        }

        @Override
        public boolean isTypeLimited() {
            return false;
        }

        @Override
        public int highestType() {
            return Integer.MAX_VALUE;
        }

        @Override
        public LongIterator addedRelationships(NodeState transactionState) {
            return transactionState.getAddedRelationships(direction);
        }

        @Override
        public RelationshipSelection reverse() {
            return Direction.BOTH.equals(direction) ? this : new DirectionalAllTypes(direction.reverse());
        }

        @Override
        public String toString() {
            return "RelationshipSelection[" + direction + "]";
        }
    }

    private static class MultiDirectionalMultiType extends Directional {
        private final DirectedTypes directedTypes;

        /**
         * Allows specification of types for the three different directions. Assumes that the
         * three arrays are all pairwise disjoint. A null array signifies that we allow all types in the corresponding
         * direction.
         * <p>
         * If one of the directed arrays are null, then the other directed array must be empty per the disjoint
         * assumption. If the bothTypes array is null, then both other arrays need to be empty.
         */
        MultiDirectionalMultiType(DirectedTypes directedTypes) {
            super(directedTypes.computeDirection());
            this.directedTypes = directedTypes;
        }

        @Override
        public boolean test(RelationshipDirection direction) {
            return super.test(direction);
        }

        @Override
        public boolean test(int type) {
            return directedTypes.hasEither(type);
        }

        @Override
        public boolean test(int type, RelationshipDirection direction) {
            if (direction == RelationshipDirection.LOOP) {
                return test(type);
            } else if (direction == RelationshipDirection.OUTGOING) {
                return directedTypes.hasOutgoing(type);
            } else {
                return directedTypes.hasIncoming(type);
            }
        }

        @Override
        public int numberOfCriteria() {
            return directedTypes.numberOfCriteria();
        }

        @Override
        public boolean isTypeLimited() {
            return directedTypes.isTypeLimited();
        }

        @Override
        public Direction criterionDirection(int index) {
            return directedTypes.criterionDirection(index);
        }

        @Override
        public int criterionType(int index) {
            return directedTypes.criterionType(index);
        }

        @Override
        public int highestType() {
            return directedTypes.isTypeLimited()
                    ? directedTypes.criterionType(directedTypes.numberOfCriteria() - 1)
                    : Integer.MAX_VALUE;
        }

        @Override
        public LongIterator addedRelationships(NodeState transactionState) {
            return directedTypes.addedRelationships(transactionState);
        }

        @Override
        public RelationshipSelection reverse() {
            return new MultiDirectionalMultiType(directedTypes.reverse());
        }

        @Override
        public Direction direction(int type) {
            return directedTypes.direction(type);
        }

        @Override
        public String toString() {
            return format("RelationshipSelection[%s]", directedTypes);
        }
    }

    public static final RelationshipSelection ALL_RELATIONSHIPS = new RelationshipSelection() {
        @Override
        public boolean test(int type) {
            return true;
        }

        @Override
        public boolean test(RelationshipDirection direction) {
            return true;
        }

        @Override
        public Direction direction(int type) {
            return Direction.BOTH;
        }

        @Override
        public boolean test(int type, RelationshipDirection direction) {
            return true;
        }

        @Override
        public RelationshipSelection reverse() {
            return this;
        }

        @Override
        public int numberOfCriteria() {
            return 1;
        }

        @Override
        public Direction criterionDirection(int index) {
            assert index == 0;
            return Direction.BOTH;
        }

        @Override
        public int criterionType(int index) {
            assert index == 0;
            return ANY_RELATIONSHIP_TYPE;
        }

        @Override
        public boolean isTypeLimited() {
            return false;
        }

        @Override
        public boolean isLimited() {
            return false;
        }

        @Override
        public int highestType() {
            return Integer.MAX_VALUE;
        }

        @Override
        public LongIterator addedRelationships(NodeState transactionState) {
            return transactionState.getAddedRelationships();
        }

        @Override
        public String toString() {
            return "RelationshipSelection[*]";
        }
    };

    public static final RelationshipSelection NO_RELATIONSHIPS = new RelationshipSelection() {
        @Override
        public boolean test(int type) {
            return false;
        }

        @Override
        public boolean test(RelationshipDirection direction) {
            return false;
        }

        @Override
        public Direction direction(int type) {
            return Direction.BOTH;
        }

        @Override
        public boolean test(int type, RelationshipDirection direction) {
            return false;
        }

        @Override
        public int numberOfCriteria() {
            return 0;
        }

        @Override
        public Direction criterionDirection(int index) {
            throw new IllegalArgumentException("Unknown criterion index " + index);
        }

        @Override
        public int criterionType(int index) {
            throw new IllegalArgumentException("Unknown criterion index " + index);
        }

        @Override
        public boolean isTypeLimited() {
            return true;
        }

        @Override
        public int highestType() {
            return -1;
        }

        @Override
        public LongIterator addedRelationships(NodeState transactionState) {
            return ImmutableEmptyLongIterator.INSTANCE;
        }

        @Override
        public RelationshipSelection reverse() {
            return this;
        }

        @Override
        public String toString() {
            return "RelationshipSelection[NOTHING]";
        }
    };
}
