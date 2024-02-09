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

import org.neo4j.storageengine.api.StorageNodeCursor;

/**
 * Several security related code paths need to stream nodes from the store without underlying security
 * checks because they are used in cases where the security checks are handled above this cursor in the
 * form of specific filters.
 */
class FullAccessNodeCursor extends DefaultNodeCursor {
    FullAccessNodeCursor(CursorPool<DefaultNodeCursor> pool, StorageNodeCursor storeCursor) {
        super(pool, storeCursor, null, false);
    }

    @Override
    protected final boolean allowsTraverse() {
        return true;
    }

    @Override
    protected final boolean allowsTraverseAll() {
        return true;
    }
}
