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

package org.neo4j.kernel.impl.coreapi.internal;

import java.util.function.ToLongFunction;

import org.neo4j.graphdb.Entity;
import org.neo4j.internal.kernel.api.Cursor;
import org.neo4j.kernel.api.ResourceTracker;

import static org.neo4j.io.IOUtils.closeAllSilently;

public class CursorIterator<CURSOR extends Cursor, E extends Entity> extends PrefetchingEntityResourceIterator<CURSOR,E>
{
    private final CURSOR cursor;
    private final ToLongFunction<CURSOR> toReferenceFunction;
    private final ResourceTracker resourceTracker;

    public CursorIterator( CURSOR cursor, ToLongFunction<CURSOR> toReferenceFunction, CursorEntityFactory<CURSOR,E> entityFactory,
            ResourceTracker resourceTracker )
    {
        super( cursor, entityFactory );
        this.cursor = cursor;
        this.toReferenceFunction = toReferenceFunction;
        this.resourceTracker = resourceTracker;
        resourceTracker.registerCloseableResource( this );
    }

    @Override
    long fetchNext()
    {
        if ( cursor.next() )
        {
            return toReferenceFunction.applyAsLong( cursor );
        }
        return NO_ID;
    }

    @Override
    void closeResources()
    {
        resourceTracker.unregisterCloseableResource( this );
        closeAllSilently( cursor );
    }
}
