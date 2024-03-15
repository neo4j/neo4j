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
package org.neo4j.kernel.impl.store.record;

import static org.neo4j.kernel.impl.store.record.Record.NO_NEXT_PROPERTY;
import static org.neo4j.kernel.impl.store.record.Record.NO_NEXT_RELATIONSHIP;
import static org.neo4j.memory.HeapEstimator.shallowSizeOfInstance;

import java.util.Objects;
import org.neo4j.string.Mask;

public class RelationshipRecord extends PrimitiveRecord {
    public static final long SHALLOW_SIZE = shallowSizeOfInstance(RelationshipRecord.class);
    private long firstNode;
    private long secondNode;
    private int type;
    private long firstPrevRel;
    private long firstNextRel;
    private long secondPrevRel;
    private long secondNextRel;
    private boolean firstInFirstChain;
    private boolean firstInSecondChain;

    public RelationshipRecord(long id) {
        super(id);
    }

    public RelationshipRecord(RelationshipRecord other) {
        super(other);
        this.firstNode = other.firstNode;
        this.secondNode = other.secondNode;
        this.type = other.type;
        this.firstPrevRel = other.firstPrevRel;
        this.firstNextRel = other.firstNextRel;
        this.secondPrevRel = other.secondPrevRel;
        this.secondNextRel = other.secondNextRel;
        this.firstInFirstChain = other.firstInFirstChain;
        this.firstInSecondChain = other.firstInSecondChain;
    }

    public RelationshipRecord initialize(
            boolean inUse,
            long nextProp,
            long firstNode,
            long secondNode,
            int type,
            long firstPrevRel,
            long firstNextRel,
            long secondPrevRel,
            long secondNextRel,
            boolean firstInFirstChain,
            boolean firstInSecondChain) {
        super.initialize(inUse, nextProp);
        this.firstNode = firstNode;
        this.secondNode = secondNode;
        this.type = type;
        this.firstPrevRel = firstPrevRel;
        this.firstNextRel = firstNextRel;
        this.secondPrevRel = secondPrevRel;
        this.secondNextRel = secondNextRel;
        this.firstInFirstChain = firstInFirstChain;
        this.firstInSecondChain = firstInSecondChain;
        return this;
    }

    @Override
    public void clear() {
        initialize(
                false,
                NO_NEXT_PROPERTY.intValue(),
                -1,
                -1,
                -1,
                1,
                NO_NEXT_RELATIONSHIP.intValue(),
                1,
                NO_NEXT_RELATIONSHIP.intValue(),
                true,
                true);
    }

    public void setLinks(long firstNode, long secondNode, int type) {
        this.firstNode = firstNode;
        this.secondNode = secondNode;
        this.type = type;
    }

    public long getFirstNode() {
        return firstNode;
    }

    public void setFirstNode(long firstNode) {
        this.firstNode = firstNode;
    }

    public long getSecondNode() {
        return secondNode;
    }

    public void setSecondNode(long secondNode) {
        this.secondNode = secondNode;
    }

    public int getType() {
        return type;
    }

    public void setType(int type) {
        this.type = type;
    }

    public long getFirstPrevRel() {
        return firstPrevRel;
    }

    public long getPrevRel(long nodeId) {
        assertEitherFirstOrSecondNode(nodeId);
        return nodeId == firstNode ? firstPrevRel : secondPrevRel;
    }

    public long getNextRel(long nodeId) {
        assertEitherFirstOrSecondNode(nodeId);
        return nodeId == firstNode ? firstNextRel : secondNextRel;
    }

    public void setFirstPrevRel(long firstPrevRel) {
        this.firstPrevRel = firstPrevRel;
    }

    public void setPrevRel(long prevRel, long nodeId) {
        assertEitherFirstOrSecondNode(nodeId);
        if (nodeId == firstNode) {
            this.firstPrevRel = prevRel;
        }
        if (nodeId == secondNode) {
            this.secondPrevRel = prevRel;
        }
    }

    private void assertEitherFirstOrSecondNode(long nodeId) {
        var firstOrSecond = nodeId == firstNode || nodeId == secondNode;
        if (!firstOrSecond) {
            throw new IllegalArgumentException(nodeId + " is neither first nor second node of " + this);
        }
    }

    public void setNextRel(long nextRel, long nodeId) {
        assertEitherFirstOrSecondNode(nodeId);
        if (nodeId == firstNode) {
            this.firstNextRel = nextRel;
        }
        if (nodeId == secondNode) {
            this.secondNextRel = nextRel;
        }
    }

    public long getFirstNextRel() {
        return firstNextRel;
    }

    public void setFirstNextRel(long firstNextRel) {
        this.firstNextRel = firstNextRel;
    }

    public long getSecondPrevRel() {
        return secondPrevRel;
    }

    public void setSecondPrevRel(long secondPrevRel) {
        this.secondPrevRel = secondPrevRel;
    }

    public long getSecondNextRel() {
        return secondNextRel;
    }

    public void setSecondNextRel(long secondNextRel) {
        this.secondNextRel = secondNextRel;
    }

    public boolean isFirstInFirstChain() {
        return firstInFirstChain;
    }

    public void setFirstInFirstChain(boolean firstInFirstChain) {
        this.firstInFirstChain = firstInFirstChain;
    }

    public void setFirstInChain(boolean first, long nodeId) {
        assertEitherFirstOrSecondNode(nodeId);
        if (nodeId == firstNode) {
            firstInFirstChain = first;
        }
        if (nodeId == secondNode) {
            firstInSecondChain = first;
        }
    }

    public boolean isFirstInChain(long nodeId) {
        assertEitherFirstOrSecondNode(nodeId);
        return nodeId == firstNode ? firstInFirstChain : firstInSecondChain;
    }

    public boolean isFirstInSecondChain() {
        return firstInSecondChain;
    }

    public void setFirstInSecondChain(boolean firstInSecondChain) {
        this.firstInSecondChain = firstInSecondChain;
    }

    @Override
    public String toString(Mask mask) {
        return "Relationship[" + getId() + ",used="
                + inUse() + ",source="
                + firstNode + ",target="
                + secondNode + ",type="
                + type + (firstInFirstChain ? ",sCount=" : ",sPrev=")
                + firstPrevRel + ",sNext="
                + firstNextRel + (firstInSecondChain ? ",tCount=" : ",tPrev=")
                + secondPrevRel + ",tNext="
                + secondNextRel + ",prop="
                + getNextProp() + secondaryUnitToString()
                + (firstInFirstChain ? ", sFirst" : ",!sFirst")
                + (firstInSecondChain ? ", tFirst" : ",!tFirst")
                + "]";
    }

    @Override
    public void setIdTo(PropertyRecord property) {
        property.setRelId(getId());
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        if (!super.equals(o)) {
            return false;
        }
        RelationshipRecord that = (RelationshipRecord) o;
        return firstNode == that.firstNode
                && secondNode == that.secondNode
                && type == that.type
                && firstPrevRel == that.firstPrevRel
                && firstNextRel == that.firstNextRel
                && secondPrevRel == that.secondPrevRel
                && secondNextRel == that.secondNextRel
                && firstInFirstChain == that.firstInFirstChain
                && firstInSecondChain == that.firstInSecondChain;
    }

    @Override
    public int hashCode() {
        return Objects.hash(
                super.hashCode(),
                firstNode,
                secondNode,
                type,
                firstPrevRel,
                firstNextRel,
                secondPrevRel,
                secondNextRel,
                firstInFirstChain,
                firstInSecondChain);
    }
}
