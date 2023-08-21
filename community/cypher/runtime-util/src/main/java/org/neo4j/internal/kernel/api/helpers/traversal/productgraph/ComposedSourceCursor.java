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

/**
 * Composes two source cursors to provide a SourceCursor from the outer source to the inner output
 */
final class ComposedSourceCursor<Outer, Intermediate, Inner> implements SourceCursor<Outer, Inner> {

    private final SourceCursor<Outer, Intermediate> outerCursor;
    private final SourceCursor<Intermediate, Inner> innerCursor;
    private boolean innerSourceSet = false;

    public ComposedSourceCursor(
            SourceCursor<Outer, Intermediate> outerCursor, SourceCursor<Intermediate, Inner> innerCursor) {
        this.outerCursor = outerCursor;
        this.innerCursor = innerCursor;
    }

    @Override
    public void setSource(Outer outer) {
        this.outerCursor.setSource(outer);
        this.innerSourceSet = false;
    }

    @Override
    public boolean next() {
        while (true) {
            if (!this.innerSourceSet) {
                // it is possible for the outer cursor to be empty, in which case we can't set the inner cursor source
                // at all
                if (!this.outerCursor.next()) {
                    return false;
                }

                this.innerCursor.setSource(this.outerCursor.current());
                this.innerSourceSet = true;
            }

            if (this.innerCursor.next()) {
                return true;
            }

            // if we reach this point, the inner cursor is empty so we need to
            // progress the outer cursor and reset the inner cursor source

            if (!this.outerCursor.next()) {
                this.innerSourceSet = false;
                return false;
            }

            this.innerCursor.setSource(this.outerCursor.current());
        }
    }

    @Override
    public Inner current() {
        return this.innerSourceSet ? this.innerCursor.current() : null;
    }

    @Override
    public void reset() {
        this.outerCursor.reset();
        this.innerCursor.reset();
        this.innerSourceSet = false;
    }

    @Override
    public void close() throws Exception {
        this.outerCursor.close();
        this.innerCursor.close();
    }
}
