/*
 * Copyright (c) 2002-2019 "Neo4j,"
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
package org.neo4j.kernel.impl.newapi;

import org.neo4j.internal.kernel.api.NodeCursor;
import org.neo4j.internal.kernel.api.PropertyCursor;
import org.neo4j.internal.kernel.api.Read;
import org.neo4j.internal.kernel.api.exceptions.EntityNotFoundException;
import org.neo4j.io.IOUtils;
import org.neo4j.kernel.api.index.PropertyAccessor;
import org.neo4j.storageengine.api.EntityType;
import org.neo4j.values.storable.Value;
import org.neo4j.values.storable.Values;

/**
 * Generic single-threaded {@link PropertyAccessor} given a {@link NodeCursor} and {@link PropertyCursor}.
 */
class CursorPropertyAccessor implements PropertyAccessor, AutoCloseable
{
    private final NodeCursor nodeCursor;
    private final PropertyCursor propertyCursor;
    private final Read read;

    CursorPropertyAccessor( NodeCursor nodeCursor, PropertyCursor propertyCursor, Read read )
    {
        this.nodeCursor = nodeCursor;
        this.propertyCursor = propertyCursor;
        this.read = read;
    }

    @Override
    public void close()
    {
        IOUtils.closeAllSilently( propertyCursor, nodeCursor );
    }

    @Override
    public Value getPropertyValue( long nodeId, int propertyKeyId ) throws EntityNotFoundException
    {
        read.singleNode( nodeId, nodeCursor );
        if ( !nodeCursor.next() )
        {
            throw new EntityNotFoundException( EntityType.NODE, nodeId );
        }

        nodeCursor.properties( propertyCursor );
        while ( propertyCursor.next() )
        {
            if ( propertyCursor.propertyKey() == propertyKeyId )
            {
                return propertyCursor.propertyValue();
            }
        }
        return Values.NO_VALUE;
    }
}
