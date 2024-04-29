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
package org.neo4j.kernel.impl.newapi;

import org.neo4j.kernel.api.index.IndexProgressor;

abstract class IndexCursor<T extends IndexProgressor, CURSOR> extends TraceableCursorImpl<CURSOR> {
    protected T progressor;

    protected IndexCursor(CursorPool<CURSOR> pool) {
        super(pool);
    }

    final void initialize(T progressor) {
        if (this.progressor != null) {
            this.progressor.close();
        }
        this.progressor = progressor;
    }

    protected final boolean indexNext() {
        return progressor != null && progressor.next();
    }

    void closeProgressor() {
        if (progressor != null) {
            progressor.close();
        }
        progressor = null;
    }

    boolean isProgressorClosed() {
        return progressor == null;
    }
}
