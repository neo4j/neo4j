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
package org.neo4j.kernel.impl.store.cursor;

import org.neo4j.io.pagecache.PageCursor;
import org.neo4j.io.pagecache.context.CursorContext;
import org.neo4j.kernel.impl.store.NeoStores;
import org.neo4j.storageengine.api.cursor.StoreCursors;

import static org.neo4j.storageengine.api.cursor.CursorTypes.DYNAMIC_ARRAY_STORE_CURSOR;
import static org.neo4j.storageengine.api.cursor.CursorTypes.DYNAMIC_LABEL_STORE_CURSOR;
import static org.neo4j.storageengine.api.cursor.CursorTypes.DYNAMIC_LABEL_TOKEN_CURSOR;
import static org.neo4j.storageengine.api.cursor.CursorTypes.DYNAMIC_PROPERTY_KEY_TOKEN_CURSOR;
import static org.neo4j.storageengine.api.cursor.CursorTypes.DYNAMIC_REL_TYPE_TOKEN_CURSOR;
import static org.neo4j.storageengine.api.cursor.CursorTypes.DYNAMIC_STRING_STORE_CURSOR;
import static org.neo4j.storageengine.api.cursor.CursorTypes.GROUP_CURSOR;
import static org.neo4j.storageengine.api.cursor.CursorTypes.LABEL_TOKEN_CURSOR;
import static org.neo4j.storageengine.api.cursor.CursorTypes.MAX_TYPE;
import static org.neo4j.storageengine.api.cursor.CursorTypes.NODE_CURSOR;
import static org.neo4j.storageengine.api.cursor.CursorTypes.PROPERTY_CURSOR;
import static org.neo4j.storageengine.api.cursor.CursorTypes.PROPERTY_KEY_TOKEN_CURSOR;
import static org.neo4j.storageengine.api.cursor.CursorTypes.RELATIONSHIP_CURSOR;
import static org.neo4j.storageengine.api.cursor.CursorTypes.REL_TYPE_TOKEN_CURSOR;
import static org.neo4j.storageengine.api.cursor.CursorTypes.SCHEMA_CURSOR;

public class CachedStoreCursors implements StoreCursors
{
    private final NeoStores neoStores;
    private CursorContext cursorContext;

    private PageCursor[] cursorsByType;

    public CachedStoreCursors( NeoStores neoStores, CursorContext cursorContext )
    {
        this.neoStores = neoStores;
        this.cursorContext = cursorContext;
        this.cursorsByType = createEmptyCursorArray();
    }

    public void reset( CursorContext cursorContext )
    {
        this.cursorContext = cursorContext;
        resetCursors();
    }

    private void resetCursors()
    {
        for ( PageCursor pageCursor : cursorsByType )
        {
            if ( pageCursor != null )
            {
                pageCursor.close();
            }
        }
        cursorsByType = createEmptyCursorArray();
    }

    @Override
    public PageCursor readCursor( short type )
    {
        var cursor = cursorsByType[type];
        if ( cursor == null )
        {
            cursor = createReadCursor( type );
            cursorsByType[type] = cursor;
        }
        return cursor;
    }

    @Override
    public PageCursor writeCursor( short type )
    {
        switch ( type )
        {
        case NODE_CURSOR:
            return neoStores.getNodeStore().openPageCursorForWriting( 0, cursorContext );
        case GROUP_CURSOR:
            return neoStores.getRelationshipGroupStore().openPageCursorForWriting( 0, cursorContext );
        case SCHEMA_CURSOR:
            return neoStores.getSchemaStore().openPageCursorForWriting( 0, cursorContext );
        case RELATIONSHIP_CURSOR:
            return neoStores.getRelationshipStore().openPageCursorForWriting( 0, cursorContext );
        case PROPERTY_CURSOR:
            return neoStores.getPropertyStore().openPageCursorForWriting( 0, cursorContext );
        case DYNAMIC_ARRAY_STORE_CURSOR:
            return neoStores.getPropertyStore().getArrayStore().openPageCursorForWriting( 0, cursorContext );
        case DYNAMIC_STRING_STORE_CURSOR:
            return neoStores.getPropertyStore().getStringStore().openPageCursorForWriting( 0, cursorContext );
        case DYNAMIC_LABEL_STORE_CURSOR:
            return neoStores.getNodeStore().getDynamicLabelStore().openPageCursorForWriting( 0, cursorContext );
        case DYNAMIC_REL_TYPE_TOKEN_CURSOR:
            return neoStores.getRelationshipTypeTokenStore().getNameStore().openPageCursorForWriting( 0, cursorContext );
        case REL_TYPE_TOKEN_CURSOR:
            return neoStores.getRelationshipTypeTokenStore().openPageCursorForWriting( 0, cursorContext );
        case DYNAMIC_PROPERTY_KEY_TOKEN_CURSOR:
            return neoStores.getPropertyKeyTokenStore().getNameStore().openPageCursorForWriting( 0, cursorContext );
        case PROPERTY_KEY_TOKEN_CURSOR:
            return neoStores.getPropertyKeyTokenStore().openPageCursorForWriting( 0, cursorContext );
        case DYNAMIC_LABEL_TOKEN_CURSOR:
            return neoStores.getLabelTokenStore().getNameStore().openPageCursorForWriting( 0, cursorContext );
        case LABEL_TOKEN_CURSOR:
            return neoStores.getLabelTokenStore().openPageCursorForWriting( 0, cursorContext );
        default:
            throw new UnsupportedOperationException( "Unsupported type " + type + " of cursor was requested." );
        }
    }

    private PageCursor createReadCursor( short type )
    {
        switch ( type )
        {
        case NODE_CURSOR:
            return neoStores.getNodeStore().openPageCursorForReading( 0, cursorContext );
        case GROUP_CURSOR:
            return neoStores.getRelationshipGroupStore().openPageCursorForReading( 0, cursorContext );
        case SCHEMA_CURSOR:
            return neoStores.getSchemaStore().openPageCursorForReading( 0, cursorContext );
        case RELATIONSHIP_CURSOR:
            return neoStores.getRelationshipStore().openPageCursorForReading( 0, cursorContext );
        case PROPERTY_CURSOR:
            return neoStores.getPropertyStore().openPageCursorForReading( 0, cursorContext );
        case DYNAMIC_ARRAY_STORE_CURSOR:
            return neoStores.getPropertyStore().getArrayStore().openPageCursorForReading( 0, cursorContext );
        case DYNAMIC_STRING_STORE_CURSOR:
            return neoStores.getPropertyStore().getStringStore().openPageCursorForReading( 0, cursorContext );
        case DYNAMIC_LABEL_STORE_CURSOR:
            return neoStores.getNodeStore().getDynamicLabelStore().openPageCursorForReading( 0, cursorContext );
        case DYNAMIC_REL_TYPE_TOKEN_CURSOR:
            return neoStores.getRelationshipTypeTokenStore().getNameStore().openPageCursorForReading( 0, cursorContext );
        case REL_TYPE_TOKEN_CURSOR:
            return neoStores.getRelationshipTypeTokenStore().openPageCursorForReading( 0, cursorContext );
        case DYNAMIC_PROPERTY_KEY_TOKEN_CURSOR:
            return neoStores.getPropertyKeyTokenStore().getNameStore().openPageCursorForReading( 0, cursorContext );
        case PROPERTY_KEY_TOKEN_CURSOR:
            return neoStores.getPropertyKeyTokenStore().openPageCursorForReading( 0, cursorContext );
        case DYNAMIC_LABEL_TOKEN_CURSOR:
            return neoStores.getLabelTokenStore().getNameStore().openPageCursorForReading( 0, cursorContext );
        case LABEL_TOKEN_CURSOR:
            return neoStores.getLabelTokenStore().openPageCursorForReading( 0, cursorContext );
        default:
            throw new UnsupportedOperationException( "Unsupported type " + type + "of cursor was requested." );
        }
    }

    @Override
    public void close()
    {
        resetCursors();
    }

    private static PageCursor[] createEmptyCursorArray()
    {
        return new PageCursor[MAX_TYPE + 1];
    }
}
