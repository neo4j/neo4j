/*
 * Copyright (c) "Neo4j"
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
package org.neo4j.internal.kernel.api.helpers.traversal.productgraph;

final class RelationshipExpansionCursor implements SourceCursor<State, RelationshipExpansion> {
    private int index = -1;
    private State state;

    public void setSource(State state) {
        this.index = -1;
        this.state = state;
    }

    public boolean next() {
        return ++this.index < this.state.getRelationshipExpansions().length;
    }

    public RelationshipExpansion current() {
        return this.state.getRelationshipExpansions()[this.index];
    }

    public void reset() {
        this.index = -1;
    }

    public void close() {
        this.state = null;
    }
}
