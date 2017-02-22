/*
 * Copyright (c) 2002-2017 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
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
package org.neo4j.kernel.impl.api.store;

import org.neo4j.cursor.Cursor;
import org.neo4j.kernel.impl.locking.Lock;
import org.neo4j.kernel.impl.store.RecordCursors;
import org.neo4j.kernel.impl.util.InstanceCache;
import org.neo4j.storageengine.api.PropertyItem;

class NodeExploringCursors
{
    private final InstanceCache<StoreSinglePropertyCursor> singlePropertyCursorCache;
    private final InstanceCache<StorePropertyCursor> propertyCursorCache;

    NodeExploringCursors( final RecordCursors cursors )
    {
        singlePropertyCursorCache = new InstanceCache<StoreSinglePropertyCursor>()
        {
            @Override
            protected StoreSinglePropertyCursor create()
            {
                return new StoreSinglePropertyCursor( cursors, singlePropertyCursorCache );
            }
        };
        propertyCursorCache = new InstanceCache<StorePropertyCursor>()
        {
            @Override
            protected StorePropertyCursor create()
            {
                return new StorePropertyCursor( cursors, propertyCursorCache );
            }
        };
    }

    public Cursor<PropertyItem> properties( long nextProp, Lock lock )
    {
        return propertyCursorCache.get().init( nextProp, lock );
    }

    public Cursor<PropertyItem> property( long nextProp, int propertyKeyId, Lock lock )
    {
        return singlePropertyCursorCache.get().init( nextProp, propertyKeyId, lock );
    }
}
