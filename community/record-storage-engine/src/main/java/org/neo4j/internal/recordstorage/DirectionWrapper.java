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
package org.neo4j.internal.recordstorage;

import org.neo4j.graphdb.Direction;
import org.neo4j.kernel.impl.store.record.NodeRecord;
import org.neo4j.kernel.impl.store.record.RelationshipGroupRecord;
import org.neo4j.kernel.impl.store.record.RelationshipRecord;
import org.neo4j.storageengine.api.RelationshipDirection;

public enum DirectionWrapper {
    OUTGOING(RelationshipDirection.OUTGOING) {
        @Override
        public long getNextRel(RelationshipGroupRecord group) {
            return group.getFirstOut();
        }

        @Override
        public void setNextRel(RelationshipGroupRecord group, long firstNextRel) {
            group.setFirstOut(firstNextRel);
        }

        @Override
        public boolean hasExternalDegrees(RelationshipGroupRecord group) {
            return group.hasExternalDegreesOut();
        }

        @Override
        public void setHasExternalDegrees(RelationshipGroupRecord group) {
            group.setHasExternalDegreesOut(true);
        }
    },
    INCOMING(RelationshipDirection.INCOMING) {
        @Override
        public long getNextRel(RelationshipGroupRecord group) {
            return group.getFirstIn();
        }

        @Override
        public void setNextRel(RelationshipGroupRecord group, long firstNextRel) {
            group.setFirstIn(firstNextRel);
        }

        @Override
        public boolean hasExternalDegrees(RelationshipGroupRecord group) {
            return group.hasExternalDegreesIn();
        }

        @Override
        public void setHasExternalDegrees(RelationshipGroupRecord group) {
            group.setHasExternalDegreesIn(true);
        }
    },
    LOOP(RelationshipDirection.LOOP) {
        @Override
        public long getNextRel(RelationshipGroupRecord group) {
            return group.getFirstLoop();
        }

        @Override
        public void setNextRel(RelationshipGroupRecord group, long firstNextRel) {
            group.setFirstLoop(firstNextRel);
        }

        @Override
        public boolean hasExternalDegrees(RelationshipGroupRecord group) {
            return group.hasExternalDegreesLoop();
        }

        @Override
        public void setHasExternalDegrees(RelationshipGroupRecord group) {
            group.setHasExternalDegreesLoop(true);
        }
    };

    DirectionWrapper(RelationshipDirection direction) {
        this.direction = direction;
    }

    private final RelationshipDirection direction;

    public abstract long getNextRel(RelationshipGroupRecord group);

    public abstract void setNextRel(RelationshipGroupRecord group, long firstNextRel);

    public abstract boolean hasExternalDegrees(RelationshipGroupRecord group);

    public abstract void setHasExternalDegrees(RelationshipGroupRecord group);

    public RelationshipDirection direction() {
        return direction;
    }

    public static DirectionWrapper wrapDirection(RelationshipRecord rel, NodeRecord startNode) {
        boolean isOut = rel.getFirstNode() == startNode.getId();
        boolean isIn = rel.getSecondNode() == startNode.getId();
        assert isOut || isIn;
        if (isOut && isIn) {
            return LOOP;
        }
        return isOut ? OUTGOING : INCOMING;
    }

    public static DirectionWrapper from(Direction direction) {
        return direction == Direction.INCOMING ? INCOMING : direction == Direction.OUTGOING ? OUTGOING : LOOP;
    }
}
