/*
 * Copyright (c) 2002-2018 "Neo4j,"
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
package org.neo4j.kernel.impl.transaction.state.storeview;

import org.neo4j.internal.kernel.api.exceptions.EntityNotFoundException;
import org.neo4j.io.IOUtils;
import org.neo4j.kernel.api.index.NodePropertyAccessor;
import org.neo4j.storageengine.api.StorageNodeCursor;
import org.neo4j.storageengine.api.StoragePropertyCursor;
import org.neo4j.storageengine.api.StorageReader;
import org.neo4j.values.storable.Value;

import static org.neo4j.values.storable.Values.NO_VALUE;

public class DefaultNodePropertyAccessor implements NodePropertyAccessor
{
    private final StorageReader reader;
    private final StorageNodeCursor nodeCursor;
    private final StoragePropertyCursor propertyCursor;

    public DefaultNodePropertyAccessor( StorageReader reader )
    {
        this.reader = reader;
        nodeCursor = reader.allocateNodeCursor();
        propertyCursor = reader.allocatePropertyCursor();
    }

    @Override
    public Value getNodePropertyValue( long nodeId, int propertyKeyId ) throws EntityNotFoundException
    {
        nodeCursor.single( nodeId );
        if ( nodeCursor.next() && nodeCursor.hasProperties() )
        {
            nodeCursor.properties( propertyCursor );
            while ( propertyCursor.next() )
            {
                if ( propertyCursor.propertyKey() == propertyKeyId )
                {
                    return propertyCursor.propertyValue();
                }
            }
        }
        return NO_VALUE;
    }

    @Override
    public void close()
    {
        IOUtils.closeAllUnchecked( nodeCursor, propertyCursor, reader );
    }
}
