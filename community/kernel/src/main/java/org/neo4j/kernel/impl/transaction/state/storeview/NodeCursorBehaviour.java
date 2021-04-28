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
package org.neo4j.kernel.impl.transaction.state.storeview;

import org.neo4j.io.pagecache.tracing.cursor.CursorContext;
import org.neo4j.storageengine.api.StorageNodeCursor;
import org.neo4j.storageengine.api.StorageReader;

public class NodeCursorBehaviour implements EntityScanCursorBehaviour<StorageNodeCursor>
{
    private final StorageReader storageReader;

    NodeCursorBehaviour( StorageReader storageReader )
    {
        this.storageReader = storageReader;
    }

    @Override
    public StorageNodeCursor allocateEntityScanCursor( CursorContext cursorContext )
    {
        return storageReader.allocateNodeCursor( cursorContext );
    }

    @Override
    public long[] readTokens( StorageNodeCursor cursor )
    {
        return cursor.labels();
    }
}
