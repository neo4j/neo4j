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
package org.neo4j.kernel.impl.storemigration.legacy;

import org.neo4j.io.pagecache.PageCursor;
import org.neo4j.io.pagecache.context.CursorContext;
import org.neo4j.storageengine.api.cursor.StoreCursorsAdapter;

import static org.neo4j.storageengine.api.cursor.CursorTypes.SCHEMA_CURSOR;

public class SchemaStore35StoreCursors extends StoreCursorsAdapter
{
    private final SchemaStore35 schemaStore35;
    private final CursorContext cursorContext;

    private PageCursor schemaCursor;

    public SchemaStore35StoreCursors( SchemaStore35 schemaStore35, CursorContext cursorContext )
    {
        this.schemaStore35 = schemaStore35;
        this.cursorContext = cursorContext;
    }

    @Override
    public PageCursor readCursor( short type )
    {
        if ( SCHEMA_CURSOR == type )
        {
            if ( schemaCursor == null )
            {
                schemaCursor = schemaStore35.openPageCursorForReading( 0, cursorContext );
            }
            return schemaCursor;
        }
        else
        {
            return super.readCursor( type );
        }
    }

    @Override
    public PageCursor writeCursor( short type )
    {
        if ( SCHEMA_CURSOR == type )
        {
            return schemaStore35.openPageCursorForWriting( 0, cursorContext );
        }
        else
        {
            return super.writeCursor( type );
        }
    }

    @Override
    public void close()
    {
        if ( schemaCursor != null )
        {
            schemaCursor.close();
            schemaCursor = null;
        }
        super.close();
    }
}
