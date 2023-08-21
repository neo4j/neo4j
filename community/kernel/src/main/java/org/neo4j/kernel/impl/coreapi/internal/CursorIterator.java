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

import static org.neo4j.io.IOUtils.closeAllSilently;

import java.util.function.ToLongFunction;
import org.neo4j.graphdb.Entity;
import org.neo4j.internal.kernel.api.Cursor;

public class CursorIterator<CURSOR extends Cursor, E extends Entity>
        extends PrefetchingEntityResourceIterator<CURSOR, E> {
    private final CURSOR cursor;
    private final ToLongFunction<CURSOR> toReferenceFunction;

    public CursorIterator(
            CURSOR cursor, ToLongFunction<CURSOR> toReferenceFunction, CursorEntityFactory<CURSOR, E> entityFactory) {
        super(cursor, entityFactory);
        this.cursor = cursor;
        this.toReferenceFunction = toReferenceFunction;
    }

    @Override
    long fetchNext() {
        if (cursor.next()) {
            return toReferenceFunction.applyAsLong(cursor);
        }
        return NO_ID;
    }

    @Override
    void closeResources() {
        closeAllSilently(cursor);
    }
}
