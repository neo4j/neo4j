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

public class CachedStoreCursors implements StoreCursors
{
    private final NeoStores neoStores;
    private CursorContext cursorContext;

    private PageCursor schemaCursor;
    private PageCursor propertyCursor;
    private PageCursor dynamicStringCursor;
    private PageCursor dynamicArrayCursor;
    private PageCursor groupCursor;
    private PageCursor relCursor;
    private PageCursor nodeCursor;
    private PageCursor dynamicLabelCursor;
    private PageCursor labelTokenCursor;
    private PageCursor propertyKeyTokenCursor;
    private PageCursor relTypeTokenCursor;
    private PageCursor dynamicRelTypeCursor;
    private PageCursor dynamicLabelTokenCursor;
    private PageCursor dynamicPropKeyTokenCursor;

    public CachedStoreCursors( NeoStores neoStores, CursorContext cursorContext )
    {
        this.neoStores = neoStores;
        this.cursorContext = cursorContext;
    }

    public void reset( CursorContext cursorContext )
    {
        this.cursorContext = cursorContext;
        resetCursors();
    }

    //close all old cursors
    private void resetCursors()
    {
        if ( dynamicArrayCursor != null )
        {
            dynamicArrayCursor.close();
            dynamicArrayCursor = null;
        }
        if ( dynamicStringCursor != null )
        {
            dynamicStringCursor.close();
            dynamicStringCursor = null;
        }
        if ( schemaCursor != null )
        {
            schemaCursor.close();
            schemaCursor = null;
        }
        if ( propertyCursor != null )
        {
            propertyCursor.close();
            propertyCursor = null;
        }
        if ( groupCursor != null )
        {
            groupCursor.close();
            groupCursor = null;
        }
        if ( relCursor != null )
        {
            relCursor.close();
            relCursor = null;
        }
        if ( nodeCursor != null )
        {
            nodeCursor.close();
            nodeCursor = null;
        }
        if ( dynamicLabelCursor != null )
        {
            dynamicLabelCursor.close();
            dynamicLabelCursor = null;
        }
        if ( labelTokenCursor != null )
        {
            labelTokenCursor.close();
            labelTokenCursor = null;
        }
        if ( propertyKeyTokenCursor != null )
        {
            propertyKeyTokenCursor.close();
            propertyKeyTokenCursor = null;
        }
        if ( relTypeTokenCursor != null )
        {
            relTypeTokenCursor.close();
            relTypeTokenCursor = null;
        }
        if ( dynamicLabelTokenCursor != null )
        {
            dynamicLabelTokenCursor.close();
            dynamicLabelTokenCursor = null;
        }
        if ( dynamicPropKeyTokenCursor != null )
        {
            dynamicPropKeyTokenCursor.close();
            dynamicPropKeyTokenCursor = null;
        }
        if ( dynamicRelTypeCursor != null )
        {
            dynamicRelTypeCursor.close();
            dynamicRelTypeCursor = null;
        }
    }

    @Override
    public PageCursor labelTokenStoreCursor()
    {
        if ( labelTokenCursor == null )
        {
            labelTokenCursor = neoStores.getLabelTokenStore().openPageCursorForReading( 0, cursorContext );
        }
        return labelTokenCursor;
    }

    @Override
    public PageCursor dynamicLabelTokeStoreCursor()
    {
        if ( dynamicLabelTokenCursor == null )
        {
            dynamicLabelTokenCursor = neoStores.getLabelTokenStore().getNameStore().openPageCursorForReading( 0, cursorContext );
        }
        return dynamicLabelTokenCursor;
    }

    @Override
    public PageCursor dynamicPropertyKeyTokenCursor()
    {
        if ( dynamicPropKeyTokenCursor == null )
        {
            dynamicPropKeyTokenCursor = neoStores.getPropertyKeyTokenStore().getNameStore().openPageCursorForReading( 0, cursorContext );
        }
        return dynamicPropKeyTokenCursor;
    }

    @Override
    public PageCursor dynamicRelationshipTypeTokenCursor()
    {
        if ( dynamicRelTypeCursor == null )
        {
            dynamicRelTypeCursor = neoStores.getRelationshipTypeTokenStore().getNameStore().openPageCursorForReading( 0, cursorContext );
        }
        return dynamicRelTypeCursor;
    }

    @Override
    public PageCursor propertyKeyTokenCursor()
    {
        if ( propertyKeyTokenCursor == null )
        {
            propertyKeyTokenCursor = neoStores.getPropertyKeyTokenStore().openPageCursorForReading( 0, cursorContext );
        }
        return propertyKeyTokenCursor;
    }

    @Override
    public PageCursor relationshipTypeTokenCursor()
    {
        if ( relTypeTokenCursor == null )
        {
            relTypeTokenCursor = neoStores.getRelationshipTypeTokenStore().openPageCursorForReading( 0, cursorContext );
        }
        return relTypeTokenCursor;
    }

    @Override
    public PageCursor dynamicLabelStoreCursor()
    {
        if ( dynamicLabelCursor == null )
        {
            dynamicLabelCursor = neoStores.getNodeStore().getDynamicLabelStore().openPageCursorForWriting( 0, cursorContext );
        }
        return dynamicLabelCursor;
    }

    @Override
    public PageCursor dynamicStringStoreCursor()
    {
        if ( dynamicStringCursor == null )
        {
            dynamicStringCursor = neoStores.getPropertyStore().getStringStore().openPageCursorForReading( 0, cursorContext );
        }
        return dynamicStringCursor;
    }

    @Override
    public PageCursor dynamicArrayStoreCursor()
    {
        if ( dynamicArrayCursor == null )
        {
            dynamicArrayCursor = neoStores.getPropertyStore().getArrayStore().openPageCursorForReading( 0, cursorContext );
        }
        return dynamicArrayCursor;
    }

    @Override
    public PageCursor nodeCursor()
    {
        if ( nodeCursor == null )
        {
            nodeCursor = neoStores.getNodeStore().openPageCursorForReading( 0, cursorContext );
        }
        return nodeCursor;
    }

    @Override
    public PageCursor propertyCursor()
    {
        if ( propertyCursor == null )
        {
            propertyCursor = neoStores.getPropertyStore().openPageCursorForReading( 0, cursorContext );
        }
        return propertyCursor;
    }

    @Override
    public PageCursor relationshipCursor()
    {
        if ( relCursor == null )
        {
            relCursor = neoStores.getRelationshipStore().openPageCursorForReading( 0, cursorContext );
        }
        return relCursor;
    }

    @Override
    public PageCursor groupCursor()
    {
        if ( groupCursor == null )
        {
            groupCursor = neoStores.getRelationshipGroupStore().openPageCursorForReading( 0, cursorContext );
        }
        return groupCursor;
    }

    @Override
    public PageCursor schemaCursor()
    {
        if ( schemaCursor == null )
        {
            schemaCursor = neoStores.getSchemaStore().openPageCursorForReading( 0, cursorContext );
        }
        return schemaCursor;
    }

    @Override
    public void close()
    {
        resetCursors();
    }
}
