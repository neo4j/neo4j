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

/**
 * {@link Layout} for local times.
 */
abstract class LocalTimeLayout extends Layout.Adapter<LocalTimeSchemaKey,NativeSchemaValue>
{
    public static Layout<LocalTimeSchemaKey,NativeSchemaValue> of( IndexDescriptor descriptor )
    {
        return descriptor.type() == IndexDescriptor.Type.UNIQUE ? LocalTimeLayout.UNIQUE : LocalTimeLayout.NON_UNIQUE;
    }

    private static final long UNIQUE_LAYOUT_IDENTIFIER = Layout.namedIdentifier( "UTlt", NativeSchemaValue.SIZE );
    public static LocalTimeLayout UNIQUE = new LocalTimeLayout()
    {
        @Override
        public long identifier()
        {
            return UNIQUE_LAYOUT_IDENTIFIER;
        }

        @Override
        public int majorVersion()
        {
            return 0;
        }

        @Override
        public int minorVersion()
        {
            return 1;
        }

        @Override
        public int compare( LocalTimeSchemaKey o1, LocalTimeSchemaKey o2 )
        {
            int comparison = o1.compareValueTo( o2 );
            if ( comparison == 0 )
            {
                // This is a special case where we need also compare entityId to support inclusive/exclusive
                if ( o1.getCompareId() || o2.getCompareId() )
                {
                    return Long.compare( o1.getEntityId(), o2.getEntityId() );
                }
            }
            return comparison;
        }
    };

    private static final long NON_UNIQUE_LAYOUT_IDENTIFIER = Layout.namedIdentifier( "NTlt", NativeSchemaValue.SIZE );
    public static LocalTimeLayout NON_UNIQUE = new LocalTimeLayout()
    {
        @Override
        public long identifier()
        {
            return NON_UNIQUE_LAYOUT_IDENTIFIER;
        }

        @Override
        public int majorVersion()
        {
            return 0;
        }

        @Override
        public int minorVersion()
        {
            return 1;
        }

        @Override
        public int compare( LocalTimeSchemaKey o1, LocalTimeSchemaKey o2 )
        {
            int comparison = o1.compareValueTo( o2 );
            return comparison != 0 ? comparison : Long.compare( o1.getEntityId(), o2.getEntityId() );
        }
    };

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
    public NativeSchemaValue newValue()
    {
        return NativeSchemaValue.INSTANCE;
    }

    @Override
    public int keySize( LocalTimeSchemaKey key )
    {
        return LocalTimeSchemaKey.SIZE;
    }

    @Override
    public int valueSize( NativeSchemaValue value )
    {
        return NativeSchemaValue.SIZE;
    }

    @Override
    public void writeKey( PageCursor cursor, LocalTimeSchemaKey key )
    {
        cursor.putLong( key.nanoOfDay );
        cursor.putLong( key.getEntityId() );
    }

    @Override
    public void writeValue( PageCursor cursor, NativeSchemaValue value )
    {
    }

    @Override
    public void readKey( PageCursor cursor, LocalTimeSchemaKey into, int keySize )
    {
        into.nanoOfDay = cursor.getLong();
        into.setEntityId( cursor.getLong() );
    }

    @Override
    public void readValue( PageCursor cursor, NativeSchemaValue into, int valueSize )
    {
    }

    @Override
    public boolean fixedSize()
    {
        return true;
    }
}
