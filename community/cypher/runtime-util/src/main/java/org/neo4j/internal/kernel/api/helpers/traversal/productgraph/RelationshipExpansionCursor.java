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

import org.neo4j.internal.kernel.api.helpers.traversal.ppbfs.TraversalDirection;

final class RelationshipExpansionCursor implements SourceCursor<State, RelationshipExpansion> {
    private int index = -1;
    private State state;

    private TraversalDirection direction = TraversalDirection.Forward;

    public void setDirection(TraversalDirection direction) {
        this.direction = direction;
    }

    @Override
    public void setSource(State state) {
        this.index = -1;
        this.state = state;
    }

    @Override
    public boolean next() {
        return ++this.index < this.state.getRelationshipExpansions(direction).length;
    }

    @Override
    public RelationshipExpansion current() {
        return this.state.getRelationshipExpansions(direction)[this.index];
    }

    @Override
    public void reset() {
        this.index = -1;
    }

    @Override
    public void close() {
        this.state = null;
    }
}
