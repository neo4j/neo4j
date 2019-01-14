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
package org.neo4j.kernel.impl.index.schema;

import org.neo4j.index.internal.gbptree.Layout;
import org.neo4j.io.pagecache.PageCursor;

/**
 * {@link Layout} for local times.
 */
class LocalTimeLayout extends SchemaLayout<LocalTimeSchemaKey>
{
    LocalTimeLayout()
    {
        super( "Tlt", 0, 1 );
    }

    @Override
    public LocalTimeSchemaKey newKey()
    {
        return new LocalTimeSchemaKey();
    }

    @Override
    public LocalTimeSchemaKey copyKey( LocalTimeSchemaKey key, LocalTimeSchemaKey into )
    {
        into.nanoOfDay = key.nanoOfDay;
        into.setEntityId( key.getEntityId() );
        into.setCompareId( key.getCompareId() );
        return into;
    }

    @Override
    public int keySize( LocalTimeSchemaKey key )
    {
        return LocalTimeSchemaKey.SIZE;
    }

    @Override
    public void writeKey( PageCursor cursor, LocalTimeSchemaKey key )
    {
        cursor.putLong( key.nanoOfDay );
        cursor.putLong( key.getEntityId() );
    }

    @Override
    public void readKey( PageCursor cursor, LocalTimeSchemaKey into, int keySize )
    {
        into.nanoOfDay = cursor.getLong();
        into.setEntityId( cursor.getLong() );
    }
}
