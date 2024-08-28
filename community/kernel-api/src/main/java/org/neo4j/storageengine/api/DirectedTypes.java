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

import static org.neo4j.token.api.TokenConstants.ANY_RELATIONSHIP_TYPE;

import java.util.Arrays;
import org.eclipse.collections.api.iterator.LongIterator;
import org.neo4j.collection.PrimitiveLongCollections;
import org.neo4j.collection.trackable.HeapTrackingArrayList;
import org.neo4j.collection.trackable.HeapTrackingIntArrayList;
import org.neo4j.graphdb.Direction;
import org.neo4j.memory.MemoryTracker;
import org.neo4j.storageengine.api.txstate.NodeState;

public final class DirectedTypes {
    /**
     * Enum to represent whether either direction has an unspecified type
     */
    private enum DirectionCombination {
        NEITHER(null) {
            @Override
            public Direction combine(Direction direction) {
                return direction;
            }
        },
        OUTGOING(Direction.OUTGOING) {
            @Override
            public Direction combine(Direction direction) {
                return direction == Direction.OUTGOING ? Direction.OUTGOING : Direction.BOTH;
            }
        },
        INCOMING(Direction.INCOMING) {
            @Override
            public Direction combine(Direction direction) {
                return direction == Direction.INCOMING ? Direction.INCOMING : Direction.BOTH;
            }
        },
        BOTH(Direction.BOTH) {
            @Override
            public Direction combine(Direction direction) {
                return Direction.BOTH;
            }
        };

        private final Direction direction;

        DirectionCombination(Direction direction) {
            this.direction = direction;
        }

        public boolean matchesDirection(Direction direction) {
            return switch (this) {
                case NEITHER -> false;
                case OUTGOING -> direction == Direction.OUTGOING;
                case INCOMING -> direction == Direction.INCOMING;
                case BOTH -> true;
            };
        }

        public boolean matchesOutgoing() {
            return this == OUTGOING || this == BOTH;
        }

        public boolean matchesIncoming() {
            return this == INCOMING || this == BOTH;
        }

        public boolean matchesLoop() {
            return this != NEITHER;
        }

        private DirectionCombination fromDirection(Direction direction) {
            return switch (direction) {
                case OUTGOING -> OUTGOING;
                case INCOMING -> INCOMING;
                case BOTH -> BOTH;
            };
        }

        public int numberOfCriteria() {
            return switch (this) {
                case NEITHER -> 0;
                case INCOMING, OUTGOING, BOTH -> 1;
            };
        }

        public DirectionCombination addDirection(Direction direction) {
            return switch (this) {
                case NEITHER -> fromDirection(direction);
                case OUTGOING -> (direction == Direction.INCOMING) || (direction == Direction.BOTH) ? BOTH : this;
                case INCOMING -> (direction == Direction.OUTGOING) || (direction == Direction.BOTH) ? BOTH : this;
                case BOTH -> this;
            };
        }

        public DirectionCombination reverse() {
            return switch (this) {
                case NEITHER -> NEITHER;
                case OUTGOING -> INCOMING;
                case INCOMING -> OUTGOING;
                case BOTH -> BOTH;
            };
        }

        public abstract Direction combine(Direction direction);
    }

    private final HeapTrackingIntArrayList types;
    private final HeapTrackingArrayList<Direction> directions;

    private DirectionCombination untyped;
    private DirectionCombination existingDirections;

    private boolean isCompacted;

    private DirectedTypes(
            HeapTrackingIntArrayList types,
            HeapTrackingArrayList<Direction> directions,
            DirectionCombination untyped,
            DirectionCombination existingDirections) {
        this.types = types;
        this.directions = directions;
        this.untyped = untyped;
        this.existingDirections = existingDirections;
        this.isCompacted = true;
    }

    public DirectedTypes(MemoryTracker memoryTracker) {
        this(
                HeapTrackingIntArrayList.newIntArrayList(memoryTracker),
                HeapTrackingArrayList.newArrayList(memoryTracker),
                DirectionCombination.NEITHER,
                DirectionCombination.NEITHER);
    }

    private boolean hasTypeInDirection(int type, RelationshipDirection direction) {
        int i = types.indexOf(type);
        return i != -1 && direction.matches(directions.get(i));
    }

    public boolean hasOutgoing(int type) {
        return untyped.matchesOutgoing() || hasTypeInDirection(type, RelationshipDirection.OUTGOING);
    }

    public boolean hasIncoming(int type) {
        return untyped.matchesIncoming() || hasTypeInDirection(type, RelationshipDirection.INCOMING);
    }

    public boolean hasEither(int type) {
        return untyped.matchesLoop()
                || hasTypeInDirection(
                        type, RelationshipDirection.LOOP); // We don't need to care about directions when matching loops
    }

    public Direction computeDirection() {
        return switch (this.existingDirections) {
            case OUTGOING -> Direction.OUTGOING;
            case INCOMING -> Direction.INCOMING;
            case BOTH -> Direction.BOTH;
            case NEITHER -> throw new IllegalStateException("This should not happen");
        };
    }

    public Direction direction(int type) {
        int i = types.indexOf(type);
        return i != -1 ? untyped.combine(directions.get(i)) : untyped.direction;
    }

    public boolean hasSomeOutgoing() {
        return this.existingDirections.matchesOutgoing();
    }

    public boolean hasSomeIncoming() {
        return this.existingDirections.matchesIncoming();
    }

    public boolean hasTypesInBothDirections() {
        return this.existingDirections == DirectionCombination.BOTH;
    }

    public boolean isTypeLimited() {
        return this.untyped == DirectionCombination.NEITHER;
    }

    public int numberOfCriteria() {
        compact();
        return this.types.size() + untyped.numberOfCriteria();
    }

    public Direction criterionDirection(int index) {
        compact();
        if (index < types.size()) {
            return directions.get(index);
        }

        if (untyped.numberOfCriteria() == 1) {

            assert index == types.size()
                    : "Index out of bounds that we don't pay for checking when assertions are turned off";

            return switch (untyped) {
                case OUTGOING -> Direction.OUTGOING;
                case INCOMING -> Direction.INCOMING;
                case BOTH -> Direction.BOTH;
                case NEITHER -> throw new IllegalStateException(
                        "The numberOfCriteria returned from Neither is 0 so this should never happen");
            };
        }

        throw new IndexOutOfBoundsException(index);
    }

    public int criterionType(int index) {
        compact();

        if (index < types.size()) {
            return types.get(index);
        } else if (untyped != DirectionCombination.NEITHER) {
            assert index == types.size()
                    : "Index out of bounds that we don't pay for checking when assertions are turned off";
            return ANY_RELATIONSHIP_TYPE;
        }
        throw new IndexOutOfBoundsException(index);
    }

    public boolean allowsAllIncoming() {
        return untyped.matchesIncoming();
    }

    public boolean allowsAllOutgoing() {
        return untyped.matchesOutgoing();
    }

    public boolean allowsAll() {
        return untyped == DirectionCombination.BOTH;
    }

    public void addUntyped(Direction direction) {
        if (!untyped.matchesDirection(direction)) {
            this.isCompacted = false;
            this.untyped = this.untyped.addDirection(direction);
            this.existingDirections = this.existingDirections.addDirection(direction);
        }
    }

    public void addTypes(int[] newTypes, Direction direction) {
        if (newTypes == null) {
            this.addUntyped(direction);
        } else {
            if (this.untyped.matchesDirection(direction)) {
                return;
            }

            for (int newType : newTypes) {
                addType(newType, direction);
            }
        }
    }

    private void addType(int newType, Direction direction) {
        int insertionIndex = -1;
        for (int i = 0; i < this.types.size(); i++) {
            int type = this.types.get(i);

            if (type == newType) {
                var existingDirection = directions.get(i);
                if (existingDirection != Direction.BOTH && existingDirection != direction) {
                    // If the new direction isn't already covered, it must result in BOTH after update
                    this.directions.set(i, Direction.BOTH);
                    this.existingDirections = DirectionCombination.BOTH;
                }

                return;
            }
            if (newType < type) {
                insertionIndex = i;
                break;
            }
        }

        if (insertionIndex != -1) {
            this.types.add(insertionIndex, newType);
            this.directions.add(insertionIndex, direction);
        } else {
            this.types.add(newType);
            this.directions.add(direction);
        }

        this.existingDirections = this.existingDirections.addDirection(direction);
    }

    public DirectedTypes reverse() {
        if (untyped == DirectionCombination.BOTH) {
            DirectedTypes reverse = new DirectedTypes(types.clone(), directions.clone(), untyped, existingDirections);
            reverse.isCompacted = this.isCompacted;
            return reverse;
        }

        var reversedDirections = directions.clone();
        reversedDirections.replaceAll(Direction::reverse);

        DirectedTypes reverse =
                new DirectedTypes(types.clone(), reversedDirections, untyped.reverse(), existingDirections.reverse());
        reverse.isCompacted = this.isCompacted;
        return reverse;
    }

    public void clear() {
        untyped = DirectionCombination.NEITHER;
        existingDirections = DirectionCombination.NEITHER;
        types.clear();
        directions.clear();
        this.isCompacted = true;
    }

    public void compact() {
        if (this.isCompacted || untyped == DirectionCombination.NEITHER) {
            this.isCompacted = true;
            return;
        }

        if (untyped == DirectionCombination.BOTH) {
            types.truncate(0);
            directions.truncate(0);
            this.isCompacted = true;
            return;
        }

        int writeIndex = 0;
        int readIndex = 0;
        for (; readIndex < types.size(); readIndex++) {
            Direction direction = directions.get(readIndex);
            if (!untyped.matchesDirection(direction)) {
                if (writeIndex != readIndex) {
                    types.set(writeIndex, types.get(readIndex));
                    directions.set(writeIndex, directions.get(readIndex));
                }
                writeIndex++;
            }
        }

        if (writeIndex != readIndex) {
            types.truncate(writeIndex);
            directions.truncate(writeIndex);
        }
        this.isCompacted = true;
    }

    public int[] typesWithoutDirections() {
        if (untyped.numberOfCriteria() != 0) {
            return null; // null represents all types when they are given as an int[] array.
        }

        return this.types.toArray();
    }

    private LongIterator addedRelationshipsInner(NodeState transactionState, RelationshipDirection relDirection) {
        LongIterator[] all = new LongIterator[types.size()];
        int index = 0;
        for (int i = 0; i < types.size(); i++) {
            var direction = directions.get(i);
            if (relDirection.matches(direction)) {
                // We assume that all types are unique here, so we don't need to check backwards if this type exists
                // earlier in the array
                all[index++] = transactionState.getAddedRelationships(relDirection, types.get(i));
            }
        }
        if (index != types.size()) {
            all = Arrays.copyOf(all, index);
        }
        return PrimitiveLongCollections.concat(all);
    }

    private LongIterator addedRelationshipsInner(NodeState transactionState) {
        LongIterator[] all = new LongIterator[types.size()];
        int index = 0;
        for (int i = 0; i < types.size(); i++) {
            // We assume that all types are unique here, so we don't need to check backwards if this type exists
            // earlier in the array
            all[index++] = transactionState.getAddedRelationships(directions.get(i), types.get(i));
        }
        if (index != types.size()) {
            all = Arrays.copyOf(all, index);
        }
        return PrimitiveLongCollections.concat(all);
    }

    public LongIterator addedRelationships(NodeState transactionState) {
        return switch (untyped) {
            case OUTGOING -> PrimitiveLongCollections.concat(
                    transactionState.getAddedRelationships(Direction.OUTGOING),
                    addedRelationshipsInner(transactionState, RelationshipDirection.INCOMING));

            case INCOMING -> PrimitiveLongCollections.concat(
                    transactionState.getAddedRelationships(Direction.INCOMING),
                    addedRelationshipsInner(transactionState, RelationshipDirection.OUTGOING));

            case BOTH -> transactionState.getAddedRelationships();

            case NEITHER -> addedRelationshipsInner(transactionState);
        };
    }

    @Override
    public String toString() {
        var builder = new StringBuilder();
        for (int i = 0; i < types.size(); i++) {
            if (i > 0) {
                builder.append(", ");
            }
            builder.append(types.get(i)).append(":").append(directions.get(i));
        }
        if (untyped != DirectionCombination.NEITHER) {
            if (!types.isEmpty()) {
                builder.append(", ");
            }
            builder.append("*:").append(untyped.direction);
        }
        return builder.toString();
    }
}
