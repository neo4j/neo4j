/*
 * Copyright (c) 2002-2018 "Neo Technology,"
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
package org.neo4j.kernel.impl.index.schema;

import org.neo4j.index.internal.gbptree.Layout;
import org.neo4j.io.pagecache.PageCursor;
import org.neo4j.kernel.api.schema.index.IndexDescriptor;

class DateLayout extends SchemaLayout<DateSchemaKey>
{
    public static Layout<DateSchemaKey,NativeSchemaValue> of( IndexDescriptor descriptor )
    {
        return descriptor.type() == IndexDescriptor.Type.UNIQUE ? DateLayout.UNIQUE : DateLayout.NON_UNIQUE;
    }

    private static final long UNIQUE_LAYOUT_IDENTIFIER = Layout.namedIdentifier( "UTda", NativeSchemaValue.SIZE );
    public static DateLayout UNIQUE = new DateLayout( UNIQUE_LAYOUT_IDENTIFIER, 0, 1 );

    private static final long NON_UNIQUE_LAYOUT_IDENTIFIER = Layout.namedIdentifier( "NTda", NativeSchemaValue.SIZE );
    public static DateLayout NON_UNIQUE = new DateLayout( NON_UNIQUE_LAYOUT_IDENTIFIER, 0, 1 );

    DateLayout( long identifier, int majorVersion, int minorVersion )
    {
        super( identifier, majorVersion, minorVersion );
    }

    @Override
    public DateSchemaKey newKey()
    {
        return new DateSchemaKey();
    }

    @Override
    public DateSchemaKey copyKey( DateSchemaKey key, DateSchemaKey into )
    {
        into.epochDay = key.epochDay;
        into.setEntityId( key.getEntityId() );
        into.setCompareId( key.getCompareId() );
        return into;
    }

    @Override
    public int keySize( DateSchemaKey key )
    {
        return DateSchemaKey.SIZE;
    }

    @Override
    public void writeKey( PageCursor cursor, DateSchemaKey key )
    {
        cursor.putLong( key.epochDay );
        cursor.putLong( key.getEntityId() );
    }

    @Override
    public void readKey( PageCursor cursor, DateSchemaKey into, int keySize )
    {
        into.epochDay = cursor.getLong();
        into.setEntityId( cursor.getLong() );
    }

    @Override
    int compareValue( DateSchemaKey o1, DateSchemaKey o2 )
    {
        return o1.compareValueTo( o2 );
    }
}
