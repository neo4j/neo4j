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
package org.neo4j.storageengine.api.cursor;

import org.neo4j.io.pagecache.PageCursor;
import org.neo4j.io.pagecache.context.CursorContext;

public class StoreCursorsAdapter implements StoreCursors
{
    @Override
    public void reset( CursorContext cursorContext )
    {

    }

    @Override
    public PageCursor labelTokenStoreCursor()
    {
        return null;
    }

    @Override
    public PageCursor dynamicLabelTokeStoreCursor()
    {
        return null;
    }

    @Override
    public PageCursor propertyKeyTokenCursor()
    {
        return null;
    }

    @Override
    public PageCursor dynamicPropertyKeyTokenCursor()
    {
        return null;
    }

    @Override
    public PageCursor relationshipTypeTokenCursor()
    {
        return null;
    }

    @Override
    public PageCursor dynamicRelationshipTypeTokenCursor()
    {
        return null;
    }

    @Override
    public PageCursor dynamicLabelStoreCursor()
    {
        return null;
    }

    @Override
    public PageCursor dynamicStringStoreCursor()
    {
        return null;
    }

    @Override
    public PageCursor dynamicArrayStoreCursor()
    {
        return null;
    }

    @Override
    public PageCursor nodeCursor()
    {
        return null;
    }

    @Override
    public PageCursor propertyCursor()
    {
        return null;
    }

    @Override
    public PageCursor relationshipCursor()
    {
        return null;
    }

    @Override
    public PageCursor groupCursor()
    {
        return null;
    }

    @Override
    public PageCursor schemaCursor()
    {
        return null;
    }

    @Override
    public void close()
    {

    }
}
