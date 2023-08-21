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
package org.neo4j.kernel.impl.coreapi.internal;

import org.neo4j.graphdb.Entity;
import org.neo4j.internal.helpers.collection.PrefetchingResourceIterator;
import org.neo4j.internal.kernel.api.Cursor;

abstract class PrefetchingEntityResourceIterator<CURSOR extends Cursor, T extends Entity>
        extends PrefetchingResourceIterator<T> {
    private final CURSOR cursor;
    private final CursorEntityFactory<CURSOR, T> entityFactory;
    private boolean closed;

    protected static final long NO_ID = -1L;

    PrefetchingEntityResourceIterator(CURSOR cursor, CursorEntityFactory<CURSOR, T> entityFactory) {
        this.cursor = cursor;
        this.entityFactory = entityFactory;
    }

    @Override
    protected T fetchNextOrNull() {
        var id = fetchNext();
        if (id != NO_ID) {
            return entityFactory.make(cursor);
        }
        close();
        return null;
    }

    @Override
    public void close() {
        if (!closed) {
            closeResources();
            closed = true;
        }
    }

    abstract long fetchNext();

    abstract void closeResources();
}
