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
package org.neo4j.internal.kernel.api.helpers.traversal.productgraph;

import java.util.Objects;
import org.neo4j.util.Preconditions;

public final class NodeJuxtaposition implements Transition {
    private State targetState;

    public NodeJuxtaposition(State targetState) {
        this.targetState = targetState;
    }

    public boolean testNode(long node) {
        return targetState.test(node);
    }

    @Override
    public State targetState() {
        return targetState;
    }

    @Override
    public void setTargetState(State state) {
        Preconditions.checkState(
                targetState == null,
                "Shouldn't set target state more than once. The targetState field is only mutable to support delayed initialization which is require when there are cycles in the NFA");
        this.targetState = state;
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == this) return true;
        if (obj == null || obj.getClass() != this.getClass()) return false;
        var that = (NodeJuxtaposition) obj;
        return Objects.equals(this.targetState, that.targetState);
    }

    @Override
    public int hashCode() {
        return Objects.hash(targetState);
    }

    @Override
    public String toString() {
        return "NodeJuxtaposition[targetState=" + targetState + ']';
    }
}
