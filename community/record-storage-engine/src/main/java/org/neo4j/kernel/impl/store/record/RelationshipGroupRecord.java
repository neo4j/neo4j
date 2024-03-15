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

import static org.neo4j.kernel.impl.store.record.Record.NULL_REFERENCE;
import static org.neo4j.memory.HeapEstimator.shallowSizeOfInstance;

import java.util.Objects;
import org.neo4j.string.Mask;

public class RelationshipGroupRecord extends AbstractBaseRecord {
    public static final long SHALLOW_SIZE = shallowSizeOfInstance(RelationshipGroupRecord.class);
    private int type;
    private long next;
    private long firstOut;
    private long firstIn;
    private long firstLoop;
    private long owningNode;
    private boolean externalDegreesOut;
    private boolean externalDegreesIn;
    private boolean externalDegreesLoop;

    // Not stored, just kept in memory temporarily when loading the group chain
    private long prev;

    public RelationshipGroupRecord(long id) {
        super(id);
    }

    public RelationshipGroupRecord(RelationshipGroupRecord other) {
        super(other);
        this.type = other.type;
        this.next = other.next;
        this.firstOut = other.firstOut;
        this.firstIn = other.firstIn;
        this.firstLoop = other.firstLoop;
        this.owningNode = other.owningNode;
        this.prev = other.prev;
        this.externalDegreesOut = other.externalDegreesOut;
        this.externalDegreesIn = other.externalDegreesIn;
        this.externalDegreesLoop = other.externalDegreesLoop;
    }

    public RelationshipGroupRecord initialize(
            boolean inUse, int type, long firstOut, long firstIn, long firstLoop, long owningNode, long next) {
        super.initialize(inUse);
        this.type = type;
        this.firstOut = firstOut;
        this.firstIn = firstIn;
        this.firstLoop = firstLoop;
        this.owningNode = owningNode;
        this.next = next;
        this.prev = NULL_REFERENCE.intValue();
        this.externalDegreesOut = false;
        this.externalDegreesIn = false;
        this.externalDegreesLoop = false;
        return this;
    }

    @Override
    public void clear() {
        initialize(
                false,
                NULL_REFERENCE.intValue(),
                NULL_REFERENCE.intValue(),
                NULL_REFERENCE.intValue(),
                NULL_REFERENCE.intValue(),
                NULL_REFERENCE.intValue(),
                NULL_REFERENCE.intValue());
        prev = NULL_REFERENCE.intValue();
    }

    public int getType() {
        return type;
    }

    public void setType(int type) {
        this.type = type;
    }

    public long getFirstOut() {
        return firstOut;
    }

    public void setFirstOut(long firstOut) {
        this.firstOut = firstOut;
    }

    public long getFirstIn() {
        return firstIn;
    }

    public void setFirstIn(long firstIn) {
        this.firstIn = firstIn;
    }

    public long getFirstLoop() {
        return firstLoop;
    }

    public void setFirstLoop(long firstLoop) {
        this.firstLoop = firstLoop;
    }

    public long getNext() {
        return next;
    }

    public void setNext(long next) {
        this.next = next;
    }

    /**
     * The previous pointer, i.e. previous group in this chain of groups isn't
     * persisted in the store, but only set during reading of the group
     * chain.
     * @param prev the id of the previous group in this chain.
     */
    public void setPrev(long prev) {
        this.prev = prev;
    }

    /**
     * The previous pointer, i.e. previous group in this chain of groups isn't
     * persisted in the store, but only set during reading of the group
     * chain.
     * @return the id of the previous group in this chain.
     */
    public long getPrev() {
        return prev;
    }

    public long getOwningNode() {
        return owningNode;
    }

    public void setOwningNode(long owningNode) {
        this.owningNode = owningNode;
    }

    public boolean hasExternalDegreesOut() {
        return externalDegreesOut;
    }

    public void setHasExternalDegreesOut(boolean externalDegrees) {
        this.externalDegreesOut = externalDegrees;
    }

    public boolean hasExternalDegreesIn() {
        return externalDegreesIn;
    }

    public void setHasExternalDegreesIn(boolean externalDegrees) {
        this.externalDegreesIn = externalDegrees;
    }

    public boolean hasExternalDegreesLoop() {
        return externalDegreesLoop;
    }

    public void setHasExternalDegreesLoop(boolean externalDegrees) {
        this.externalDegreesLoop = externalDegrees;
    }

    @Override
    public String toString(Mask mask) {
        return "RelationshipGroup[" + getId() + ",type="
                + type + ",out="
                + firstOut + ",in="
                + firstIn + ",loop="
                + firstLoop + ",prev="
                + prev + ",next="
                + next + ",used="
                + inUse() + ",owner="
                + getOwningNode() + ",externalDegrees=[out:"
                + externalDegreesOut + ",in:" + externalDegreesIn + ",loop:" + externalDegreesLoop + "]"
                + secondaryUnitToString()
                + "]";
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
        RelationshipGroupRecord that = (RelationshipGroupRecord) o;
        return type == that.type
                && next == that.next
                && firstOut == that.firstOut
                && firstIn == that.firstIn
                && firstLoop == that.firstLoop
                && owningNode == that.owningNode
                && externalDegreesOut == that.externalDegreesOut
                && externalDegreesIn == that.externalDegreesIn
                && externalDegreesLoop == that.externalDegreesLoop;
        // don't compare prev since it's not persisted
    }

    @Override
    public int hashCode() {
        return Objects.hash(
                super.hashCode(),
                type,
                next,
                firstOut,
                firstIn,
                firstLoop,
                owningNode,
                prev,
                externalDegreesOut,
                externalDegreesIn,
                externalDegreesLoop);
    }
}
