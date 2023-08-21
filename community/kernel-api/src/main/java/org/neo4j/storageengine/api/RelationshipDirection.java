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

import org.neo4j.graphdb.Direction;

/**
 * Low level representation of relationship direction, used to keep traversal state and query tx-state
 *
 * Given the graph
 *      a  -[r1]-> b
 *      a <-[r2]-  b
 *      a  -[r3]-> a
 *
 * Then
 *  a.getRelationships( OUTGOING ) => r1
 *  a.getRelationships( INCOMING ) => r2
 *  a.getRelationships( LOOP ) => r3
 *
 * Note that this contrasts with /** @see #org.neo4j.graphdb.Direction(long), where
 *  a.getRelationships( org.neo4j.graphdb.Direction.OUTGOING ) => r1, r3
 *  a.getRelationships( org.neo4j.graphdb.Direction.INCOMING ) => r2, r3
 *  a.getRelationships( org.neo4j.graphdb.Direction.BOTH ) => r1, r2, r3
 */
public enum RelationshipDirection {
    // These IDs mustn't change, they are used for serializing/deserializing directions
    OUTGOING(0) {
        @Override
        public boolean matches(Direction direction) {
            return direction != Direction.INCOMING;
        }
    },
    INCOMING(1) {
        @Override
        public boolean matches(Direction direction) {
            return direction != Direction.OUTGOING;
        }
    },
    LOOP(2) {
        @Override
        public boolean matches(Direction direction) {
            return true;
        }
    };

    public static final RelationshipDirection MIN_VALUE = OUTGOING;
    public static final RelationshipDirection MAX_VALUE = LOOP;

    private static final RelationshipDirection[] DIRECTIONS_BY_ID =
            new RelationshipDirection[] {OUTGOING, INCOMING, LOOP};

    public static RelationshipDirection ofId(int id) {
        return DIRECTIONS_BY_ID[id];
    }

    public static RelationshipDirection directionOfStrict(
            long nodeReference, long sourceNodeReference, long targetNodeReference) {
        if (sourceNodeReference == nodeReference) {
            return targetNodeReference == nodeReference ? RelationshipDirection.LOOP : RelationshipDirection.OUTGOING;
        }
        if (targetNodeReference == nodeReference) {
            return RelationshipDirection.INCOMING;
        }
        throw new IllegalStateException("Traversed relationship that wasn't part of the origin node:" + nodeReference
                + ". The encountered relationship has source:" + sourceNodeReference + " and target:"
                + targetNodeReference);
    }

    private final int id;

    RelationshipDirection(int id) {
        this.id = id;
    }

    public int id() {
        return id;
    }

    public abstract boolean matches(Direction direction);
}
