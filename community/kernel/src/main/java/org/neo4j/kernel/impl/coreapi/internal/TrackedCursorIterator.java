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

import java.util.function.ToLongFunction;
import org.neo4j.graphdb.Entity;
import org.neo4j.internal.kernel.api.Cursor;
import org.neo4j.kernel.api.ResourceMonitor;

public class TrackedCursorIterator<CURSOR extends Cursor, E extends Entity> extends CursorIterator<CURSOR, E> {
    private final ResourceMonitor resourceMonitor;

    public TrackedCursorIterator(
            CURSOR cursor,
            ToLongFunction<CURSOR> toReferenceFunction,
            CursorEntityFactory<CURSOR, E> entityFactory,
            ResourceMonitor resourceMonitor) {
        super(cursor, toReferenceFunction, entityFactory);
        this.resourceMonitor = resourceMonitor;
        resourceMonitor.registerCloseableResource(this);
    }

    @Override
    void closeResources() {
        resourceMonitor.unregisterCloseableResource(this);
        super.closeResources();
    }
}
