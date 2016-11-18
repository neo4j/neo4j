/*
 * Copyright (c) 2002-2016 "Neo Technology,"
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
package org.neo4j.index.bptree.path;

import java.nio.charset.Charset;

import org.neo4j.graphdb.Direction;
import org.neo4j.index.bptree.Layout;
import org.neo4j.io.pagecache.PageCursor;

public class PathIndexLayout implements Layout<TwoLongs,TwoLongs>
{
    private static final Charset UTF_8 = Charset.forName( "UTF-8" );
    private static final int SIZE_KEY = 2 * Long.BYTES;
    private static final int SIZE_VALUE = 2 * Long.BYTES;
    private SCIndexDescription description;

    public PathIndexLayout( SCIndexDescription description )
    {
        this.description = description;
    }

    @Override
    public int compare( TwoLongs o1, TwoLongs o2 )
    {
        int compareId = Long.compare( o1.first, o2.first );
        return compareId != 0 ? compareId : Long.compare( o1.other, o2.other );
    }

    @Override
    public TwoLongs newKey()
    {
        return new TwoLongs();
    }

    @Override
    public TwoLongs minKey( TwoLongs into )
    {
        into.first = Long.MIN_VALUE;
        into.other = Long.MIN_VALUE;
        return into;
    }

    @Override
    public TwoLongs maxKey( TwoLongs into )
    {
        into.first = Long.MAX_VALUE;
        into.other = Long.MAX_VALUE;
        return into;
    }

    @Override
    public TwoLongs newValue()
    {
        return new TwoLongs();
    }

    @Override
    public int keySize()
    {
        return SIZE_KEY;
    }

    @Override
    public void copyKey( TwoLongs key, TwoLongs into )
    {
        into.first = key.first;
        into.other = key.other;
    }

    @Override
    public int valueSize()
    {
        return SIZE_VALUE;
    }

    @Override
    public void writeKey( PageCursor cursor, TwoLongs key )
    {
        cursor.putLong( key.first );
        cursor.putLong( key.other );
    }

    @Override
    public void writeValue( PageCursor cursor, TwoLongs value )
    {
        cursor.putLong( value.first );
        cursor.putLong( value.other );
    }

    @Override
    public void readKey( PageCursor cursor, TwoLongs into )
    {
        into.first = cursor.getLong();
        into.other = cursor.getLong();
    }

    @Override
    public void readValue( PageCursor cursor, TwoLongs into )
    {
        into.first = cursor.getLong();
        into.other = cursor.getLong();
    }

    @Override
    public long identifier()
    {
        long namedIdentifier = Layout.namedIdentifier( "PIL", description.toString().hashCode() );
        return namedIdentifier;
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
    public void writeMetaData( PageCursor cursor )
    {
        writeString( cursor, description.firstLabel );
        writeString( cursor, description.secondLabel );
        writeString( cursor, description.relationshipType );
        cursor.putByte( (byte) description.direction.ordinal() );
        writeString( cursor, description.relationshipPropertyKey );
        writeString( cursor, description.nodePropertyKey );
    }

    private static void writeString( PageCursor cursor, String string )
    {
        if ( string == null )
        {
            cursor.putInt( -1 );
        }
        else
        {
            byte[] bytes = string.getBytes( UTF_8 );
            cursor.putInt( string.length() );
            cursor.putBytes( bytes );
        }
    }

    @Override
    public void readMetaData( PageCursor cursor )
    {
        SCIndexDescription description = new SCIndexDescription(
                readString( cursor ), // firstLabel
                readString( cursor ), // secondLabel
                readString( cursor ), // relationshipType
                Direction.values()[cursor.getByte()],
                readString( cursor ), // nodePropertyKey
                readString( cursor ) ); // relationshipPropertyKey
        if ( this.description == null )
        {
            this.description = description;
        }
        else
        {
            if ( !this.description.equals( description ) )
            {
                throw new IllegalArgumentException( "Index was created with different path description than " +
                        "the one used to load it. Created with " + description +
                        ", but loaded with " + this.description );
            }
        }
    }

    private static String readString( PageCursor cursor )
    {
        int length = cursor.getInt();
        if ( length == -1 )
        {
            return null;
        }

        byte[] bytes = new byte[length];
        cursor.getBytes( bytes );
        return new String( bytes, UTF_8 );
    }
}
